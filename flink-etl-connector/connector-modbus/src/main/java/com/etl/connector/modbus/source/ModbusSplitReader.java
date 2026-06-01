package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.serotonin.modbus4j.ModbusFactory;
import com.serotonin.modbus4j.ModbusMaster;
import com.serotonin.modbus4j.ip.IpParameters;
import com.serotonin.modbus4j.msg.ModbusResponse;
import com.serotonin.modbus4j.msg.ReadHoldingRegistersRequest;
import com.serotonin.modbus4j.msg.ReadHoldingRegistersResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Modbus Split 读取器
 * <p>
 * 通过 Modbus TCP 协议读取 Holding Registers，每个寄存器生成一行 Row(address, value)。
 * <ul>
 *   <li>批处理模式：读取一次后标记分片完成</li>
 *   <li>流处理模式：每次读取后 sleep intervalMs，持续循环</li>
 * </ul>
 * <p>
 * 配置信息从 Split 中获取，不通过构造函数传递
 */
@Slf4j
public class ModbusSplitReader extends AbstractSplitReader<Row, ModbusSplit> {

    /**
     * 单次请求最大读取寄存器数量，兼容存在读取数量限制的 Modbus 设备
     */
    private static final int MAX_READ_SIZE = 32;

    private final Set<String> finishedSplits = new HashSet<>();

    private ModbusSplit currentSplit;
    private ModbusSourceConfig currentConfig;
    private ModbusMaster master;
    private long readCount = 0;

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        if (currentSplit == null) {
            ModbusSplit split = pendingSplits.poll();
            if (split == null) {
                RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }

            currentSplit = split;
            currentConfig = split.getModbusConfig();
            initMaster();
            log.info("开始读取分片: {}", split.splitId());
        }

        return readRegisters();
    }

    /**
     * 初始化 Modbus TCP 连接
     */
    private void initMaster() throws IOException {
        IpParameters params = new IpParameters();
        params.setHost(currentConfig.getIp());
        params.setPort(currentConfig.getPort());

        ModbusFactory factory = new ModbusFactory();
        master = factory.createTcpMaster(params, false);
        try {
            master.init();
            log.info("Modbus TCP 连接成功: {}:{}", currentConfig.getIp(), currentConfig.getPort());
        } catch (Exception e) {
            throw new IOException("Modbus TCP 连接失败: " + currentConfig.getIp() + ":" + currentConfig.getPort(), e);
        }
    }

    /**
     * 处理寄存器数据并生成 Row 记录
     *
     * @param data       寄存器原始数据
     * @param startAddr  当前批次的起始地址
     * @param wordSize   每个数据值占用的寄存器数
     * @param builder    记录构建器
     */
    private void processRegisterData(short[] data, int startAddr, int wordSize, RecordsBySplits.Builder<Row> builder) {
        for (int i = 0; i < data.length; i += wordSize) {
            int registerAddress = startAddr + i;
            int value;
            if (wordSize == 1) {
                value = data[i];
            } else {
                // 2 个寄存器组合为 32bit：高位寄存器在前（大端）
                value = (data[i] << 16) | (data[i + 1] & 0xFFFF);
            }
            Row row = Row.withPositions(RowKind.INSERT, 2);
            row.setField(0, registerAddress);
            row.setField(1, value);

            builder.add(currentSplit.splitId(), row);
            readCount++;
            log.debug("读取寄存器: address={}, value={}", registerAddress, value);
        }
    }

    /**
     * 读取所有寄存器并生成 Row 数据
     * 使用单个 ReadHoldingRegistersRequest 一次性读取所有寄存器，仅一次 TCP 往返
     */
    private RecordsWithSplitIds<Row> readRegisters() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        try {
            int deviceId = currentConfig.getDeviceId();
            int address = currentConfig.getAddress();
            int count = currentConfig.getCount();
            int wordSize = currentConfig.getWordSize();

            ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(deviceId, address, count);
            ModbusResponse response = master.send(request);
            if (response.isException()) {
                throw new IOException("Modbus 异常响应: " + response.getExceptionMessage());
            }

            short[] data = ((ReadHoldingRegistersResponse) response).getShortData();
            if (data == null || data.length < count) {
                throw new IOException(String.format("Modbus 响应数据不足: 期望 %d 个寄存器, 实际 %s",
                        count, data == null ? "null" : data.length));
            }
            processRegisterData(data, address, wordSize, builder);
        } catch (Exception e) {
            throw new IOException("读取 Modbus 寄存器失败", e);
        }

        if (currentConfig.isBounded()) {
            finishedSplits.add(currentSplit.splitId());
            log.info("批处理模式数据读取完毕，共 {} 行", readCount);
            destroyMaster();
            currentSplit = null;
            currentConfig = null;
        } else {
            try {
                Thread.sleep(currentConfig.getIntervalMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return builder.build();
    }

    private void destroyMaster() {
        if (master != null) {
            master.destroy();
            master = null;
            log.info("Modbus TCP 连接已释放");
        }
    }

    @Override
    public void close() throws Exception {
        destroyMaster();
        log.info("ModbusSplitReader 关闭，共读取 {} 行数据", readCount);
    }
}
