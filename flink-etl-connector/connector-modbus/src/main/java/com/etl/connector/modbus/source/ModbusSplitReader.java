package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.core.source.BaseSplitReader;
import com.serotonin.modbus4j.ModbusFactory;
import com.serotonin.modbus4j.ModbusMaster;
import com.serotonin.modbus4j.ip.IpParameters;
import com.serotonin.modbus4j.msg.ModbusResponse;
import com.serotonin.modbus4j.msg.ReadHoldingRegistersRequest;
import com.serotonin.modbus4j.msg.ReadHoldingRegistersResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
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
public class ModbusSplitReader implements BaseSplitReader<Row, ModbusSplit> {

    /** Modbus 保持寄存器的手册地址起始偏移 */
    private static final int HOLDING_REGISTER_OFFSET = 40001;

    private final Queue<ModbusSplit> pendingSplits = new ArrayDeque<>();
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
     * 读取所有寄存器并生成 Row 数据
     * 使用单个 ReadHoldingRegistersRequest 一次性读取所有寄存器，仅一次 TCP 往返
     */
    private RecordsWithSplitIds<Row> readRegisters() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        try {
            int deviceId = currentConfig.getDeviceId();
            int address = currentConfig.getAddress();
            int count = currentConfig.getCount();

            ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(deviceId, address, count);
            ModbusResponse response = master.send(request);
            if (response.isException()) {
                throw new IOException("Modbus 异常响应: " + response.getExceptionMessage());
            }

            short[] data = ((ReadHoldingRegistersResponse) response).getShortData();
            for (int i = 0; i < count; i++) {
                int registerAddress = HOLDING_REGISTER_OFFSET + address + i;
                Row row = Row.withPositions(RowKind.INSERT, 2);
                row.setField(0, registerAddress);
                row.setField(1, (int) data[i]);

                builder.add(currentSplit.splitId(), row);
                readCount++;
                log.debug("读取寄存器: address={}, value={}", registerAddress, (int) data[i]);
            }
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
    public void handleSplitsChanges(SplitsChange<ModbusSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个新分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        destroyMaster();
        log.info("ModbusSplitReader 关闭，共读取 {} 行数据", readCount);
    }
}
