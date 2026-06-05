package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.core.source.AbstractSplitReader;
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
    private ModbusTcpClient client;
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
            initClient();
            log.info("开始读取分片: {}", split.splitId());
        }

        return readRegisters();
    }

    /**
     * 初始化 Modbus TCP 连接
     */
    private void initClient() throws IOException {
        client = new ModbusTcpClient(
                currentConfig.getIp(),
                currentConfig.getPort(),
                currentConfig.getDeviceId(),
                currentConfig.getTimeoutMs()
        );
        client.connect();
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
     * 分批读取寄存器并生成 Row 数据
     * 每次请求最多读取 MAX_READ_SIZE 个寄存器，循环读取直到所有寄存器读完
     */
    private RecordsWithSplitIds<Row> readRegisters() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        try {
            int address = currentConfig.getAddress();
            int count = currentConfig.getCount();
            int wordSize = currentConfig.getWordSize();

            int remaining = count;
            int currentAddress = address;
            while (remaining > 0) {
                int batchSize = Math.min(remaining, MAX_READ_SIZE);

                short[] data = client.readHoldingRegisters(currentAddress, batchSize);

                processRegisterData(data, currentAddress, wordSize, builder);

                currentAddress += batchSize;
                remaining -= batchSize;
            }
        } catch (Exception e) {
            throw new IOException("读取 Modbus 寄存器失败", e);
        }

        if (currentConfig.isBounded()) {
            finishedSplits.add(currentSplit.splitId());
            log.info("批处理模式数据读取完毕，共 {} 行", readCount);
            this.close();
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

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
        log.info("ModbusSplitReader 关闭，共读取 {} 行数据", readCount);
    }
}
