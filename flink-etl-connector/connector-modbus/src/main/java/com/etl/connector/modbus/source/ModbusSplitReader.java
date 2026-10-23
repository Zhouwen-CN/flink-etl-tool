package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.connector.modbus.source.config.WordOrder;
import com.etl.connector.modbus.source.exception.ModbusProtocolException;
import com.etl.core.source.AbstractSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
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
 * 支持两种读取模式：
 * <ul>
 *   <li>连续模式：{@code address} + {@code count} 指定一个连续寄存器区间</li>
 *   <li>间隔模式：{@code points} 指定一组离散的寄存器起始地址，每个点位读取
 *       {@code wordSize} 个寄存器</li>
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
    /**
     * 累计跳过的点位数量（设备返回协议层异常的点位）
     */
    private long skippedPointCount = 0;

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
     * @param config     来源配置
     * @param builder    记录构建器
     */
    private void processRegisterData(short[] data, int startAddr, ModbusSourceConfig config,
                                     RecordsBySplits.Builder<Row> builder) {
        int wordSize = config.getWordSize();
        WordOrder wordOrder = config.getWordOrder();
        boolean floatValue = config.isFloatValue();

        for (int i = 0; i < data.length; i += wordSize) {
            int registerAddress = startAddr + i;
            Object value;
            if (floatValue) {
                // 2 个寄存器按 wordOrder 组合为 32bit，解析为 IEEE 754 浮点数
                value = Float.intBitsToFloat(wordOrder.combine(data[i], data[i + 1]));
            } else if (wordSize == 1) {
                value = (int) data[i];
            } else {
                // 2 个寄存器按 wordOrder 组合为 32bit 有符号整数
                value = wordOrder.combine(data[i], data[i + 1]);
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
     * 读取寄存器并生成 Row 数据
     * <p>
     * 按配置自动选择连续模式或间隔模式
     */
    private RecordsWithSplitIds<Row> readRegisters() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        if (currentConfig.isPointsMode()) {
            readPoints(builder);
        } else {
            readRange(builder);
        }

        if (currentConfig.isBounded()) {
            finishedSplits.add(currentSplit.splitId());
            log.info("批处理模式数据读取完毕，共 {} 行，跳过点位 {} 个", readCount, skippedPointCount);
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

    /**
     * 连续模式：分批读取连续寄存器区间并生成 Row 数据
     * <p>
     * 每次请求最多读取 MAX_READ_SIZE 个寄存器，循环读取直到所有寄存器读完。
     * 读取失败时抛出异常，交由 Flink 从 checkpoint 恢复。
     */
    private void readRange(RecordsBySplits.Builder<Row> builder) throws IOException {
        try {
            int address = currentConfig.getAddress();
            int remaining = currentConfig.getCount();
            int currentAddress = address;
            while (remaining > 0) {
                int batchSize = Math.min(remaining, MAX_READ_SIZE);

                short[] data = client.readHoldingRegisters(currentAddress, batchSize);

                processRegisterData(data, currentAddress, currentConfig, builder);

                currentAddress += batchSize;
                remaining -= batchSize;
            }
        } catch (Exception e) {
            throw new IOException("读取 Modbus 寄存器失败", e);
        }
    }

    /**
     * 间隔模式：逐个读取离散点位
     * <p>
     * 每个点位发起一次独立的读请求，读取 {@code wordSize} 个寄存器。
     * <p>
     * 单个点位读取失败时：
     * <ul>
     *   <li>协议层异常（{@link ModbusProtocolException}）：设备通信正常但拒绝了该地址
     *       （如异常码 02 地址非法），跳过该点位并告警，继续读取其余点位</li>
     *   <li>连接层故障（超时、断连等）：数据流已不可信，抛出异常由 Flink 从 checkpoint 恢复</li>
     * </ul>
     */
    private void readPoints(RecordsBySplits.Builder<Row> builder) throws IOException {
        List<Integer> points = currentConfig.getPoints();
        int wordSize = currentConfig.getWordSize();

        for (int point : points) {
            try {
                short[] data = client.readHoldingRegisters(point, wordSize);
                processRegisterData(data, point, currentConfig, builder);
            } catch (ModbusProtocolException e) {
                skippedPointCount++;
                log.warn("点位读取失败，已跳过: address={}, wordSize={}, 原因: {}",
                        point, wordSize, e.getMessage());
            } catch (IOException e) {
                // 连接层故障：数据流已不可信，交给 Flink 从 checkpoint 恢复
                throw new IOException("读取 Modbus 点位失败: address=" + point, e);
            }
        }

        log.debug("间隔读取完成，本轮点位 {} 个，累计跳过 {} 个", points.size(), skippedPointCount);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
        log.info("ModbusSplitReader 关闭，共读取 {} 行数据，跳过点位 {} 个", readCount, skippedPointCount);
    }
}
