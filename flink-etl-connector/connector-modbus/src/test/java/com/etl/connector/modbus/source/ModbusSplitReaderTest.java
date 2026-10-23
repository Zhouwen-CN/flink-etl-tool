package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.connector.modbus.source.config.WordOrder;
import com.etl.connector.modbus.source.exception.ModbusProtocolException;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ModbusSplitReaderTest {

    private ModbusSplitReader reader;

    @BeforeEach
    void setUp() {
        reader = new ModbusSplitReader();
    }

    /**
     * 通过反射调用 processRegisterData，验证数据处理逻辑
     */
    private List<Row> invokeProcessRegisterData(short[] data, int startAddr, int wordSize,
                                                boolean floatValue, WordOrder wordOrder) throws Exception {
        // 构造一个 split 用于 splitId
        ModbusSourceConfig config = ModbusSourceConfig.builder()
                .ip("127.0.0.1").port(502).deviceId(1)
                .address(0).count(data.length).wordSize(wordSize)
                .floatValue(floatValue).wordOrder(wordOrder)
                .bounded(true).intervalMs(1000).timeoutMs(3000)
                .build();
        ModbusSplit split = new ModbusSplit(config);

        // 设置 currentSplit 字段
        java.lang.reflect.Field currentSplitField = ModbusSplitReader.class.getDeclaredField("currentSplit");
        currentSplitField.setAccessible(true);
        currentSplitField.set(reader, split);

        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        Method method = ModbusSplitReader.class.getDeclaredMethod(
                "processRegisterData", short[].class, int.class, ModbusSourceConfig.class,
                RecordsBySplits.Builder.class);
        method.setAccessible(true);
        method.invoke(reader, data, startAddr, config, builder);

        List<Row> rows = new ArrayList<>();
        RecordsWithSplitIds<Row> result = builder.build();
        while (result.nextSplit() != null) {
            Row row;
            while ((row = result.nextRecordFromSplit()) != null) {
                rows.add(row);
            }
        }
        return rows;
    }

    @Test
    void testProcessRegisterData_wordSize1() throws Exception {
        short[] data = {100, 200, 300};
        List<Row> rows = invokeProcessRegisterData(data, 0, 1, false, WordOrder.ABCD);

        assertEquals(3, rows.size());
        assertEquals(0, rows.get(0).getField(0));
        assertEquals(100, rows.get(0).getField(1));
        assertEquals(1, rows.get(1).getField(0));
        assertEquals(200, rows.get(1).getField(1));
        assertEquals(2, rows.get(2).getField(0));
        assertEquals(300, rows.get(2).getField(1));
    }

    @Test
    void testProcessRegisterData_wordSize2() throws Exception {
        // data[0]=1, data[1]=2 → (1 << 16) | 2 = 65538
        short[] data = {1, 2, 3, 4};
        List<Row> rows = invokeProcessRegisterData(data, 10, 2, false, WordOrder.ABCD);

        assertEquals(2, rows.size());
        assertEquals(10, rows.get(0).getField(0));
        assertEquals(65538, rows.get(0).getField(1));
        assertEquals(12, rows.get(1).getField(0));
        assertEquals(196612, rows.get(1).getField(1)); // (3 << 16) | 4
    }

    @Test
    void testProcessRegisterData_wordSize2CDAB() throws Exception {
        // CDAB 低字在前：寄存器 [0x0000, 0x4148] → (0x4148 << 16) | 0x0000 = 0x41480000
        short[] data = {0x0000, (short) 0x4148, 0x0002, 0x0001};
        List<Row> rows = invokeProcessRegisterData(data, 0, 2, false, WordOrder.CDAB);

        assertEquals(2, rows.size());
        assertEquals(0, rows.get(0).getField(0));
        assertEquals(0x41480000, rows.get(0).getField(1));
        assertEquals(2, rows.get(1).getField(0));
        assertEquals(0x00010002, rows.get(1).getField(1)); // (1 << 16) | 2
    }

    @Test
    void testProcessRegisterData_withOffset() throws Exception {
        short[] data = {42};
        List<Row> rows = invokeProcessRegisterData(data, 100, 1, false, WordOrder.ABCD);

        assertEquals(1, rows.size());
        assertEquals(100, rows.get(0).getField(0));
        assertEquals(42, rows.get(0).getField(1));
    }

    @Test
    void testProcessRegisterData_floatValue() throws Exception {
        // 1.0f 的 IEEE 754 位模式为 0x3F800000 → 寄存器 [0x3F80, 0x0000]（ABCD 高字在前）
        // 10.0f 的 IEEE 754 位模式为 0x41200000 → 寄存器 [0x4120, 0x0000]
        short[] data = {(short) 0x3F80, 0x0000, (short) 0x4120, 0x0000};
        List<Row> rows = invokeProcessRegisterData(data, 0, 2, true, WordOrder.ABCD);

        assertEquals(2, rows.size());
        assertEquals(0, rows.get(0).getField(0));
        assertEquals(1.0f, rows.get(0).getField(1));
        assertEquals(2, rows.get(1).getField(0));
        assertEquals(10.0f, rows.get(1).getField(1));
    }

    @Test
    void testProcessRegisterData_floatValueNegative() throws Exception {
        // -2.5f 的 IEEE 754 位模式为 0xC0200000 → 寄存器 [0xC020, 0x0000]
        short[] data = {(short) 0xC020, 0x0000};
        List<Row> rows = invokeProcessRegisterData(data, 5, 2, true, WordOrder.ABCD);

        assertEquals(1, rows.size());
        assertEquals(5, rows.get(0).getField(0));
        assertEquals(-2.5f, rows.get(0).getField(1));
    }

    @Test
    void testProcessRegisterData_floatValueCDAB() throws Exception {
        // CDAB 低字在前：1.0f（0x3F800000）→ 寄存器 [0x0000, 0x3F80]；10.0f → [0x0000, 0x4120]
        short[] data = {0x0000, (short) 0x3F80, 0x0000, (short) 0x4120};
        List<Row> rows = invokeProcessRegisterData(data, 0, 2, true, WordOrder.CDAB);

        assertEquals(2, rows.size());
        assertEquals(0, rows.get(0).getField(0));
        assertEquals(1.0f, rows.get(0).getField(1));
        assertEquals(2, rows.get(1).getField(0));
        assertEquals(10.0f, rows.get(1).getField(1));
    }

    // ==================== 间隔读取（points）模式 ====================

    /**
     * 构造 points 模式的配置
     */
    private ModbusSourceConfig pointsConfig(List<Integer> points, int wordSize) {
        return ModbusSourceConfig.builder()
                .ip("127.0.0.1").port(502).deviceId(1)
                .wordSize(wordSize)
                .floatValue(false).wordOrder(WordOrder.ABCD)
                .bounded(true).intervalMs(1000).timeoutMs(3000)
                .points(points)
                .build();
    }

    /**
     * 注入 split 与 mock 客户端，并执行 readPoints
     */
    private List<Row> invokeReadPoints(ModbusSourceConfig config, ModbusTcpClient mockClient)
            throws Exception {
        ModbusSplit split = new ModbusSplit(config);

        Field currentSplitField = ModbusSplitReader.class.getDeclaredField("currentSplit");
        currentSplitField.setAccessible(true);
        currentSplitField.set(reader, split);

        Field currentConfigField = ModbusSplitReader.class.getDeclaredField("currentConfig");
        currentConfigField.setAccessible(true);
        currentConfigField.set(reader, config);

        Field clientField = ModbusSplitReader.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(reader, mockClient);

        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
        Method method = ModbusSplitReader.class.getDeclaredMethod(
                "readPoints", RecordsBySplits.Builder.class);
        method.setAccessible(true);
        try {
            method.invoke(reader, builder);
        } catch (java.lang.reflect.InvocationTargetException e) {
            // 解包反射调用抛出的业务异常，便于断言具体异常类型
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }

        List<Row> rows = new ArrayList<>();
        RecordsWithSplitIds<Row> result = builder.build();
        while (result.nextSplit() != null) {
            Row row;
            while ((row = result.nextRecordFromSplit()) != null) {
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * 验证间隔读取：每个点位独立发起请求，产出对应地址的行
     */
    @Test
    void testReadPoints_readsEachPointIndependently() throws Exception {
        ModbusTcpClient mockClient = mock(ModbusTcpClient.class);
        // 点位 0 → 寄存器 [1, 2]；点位 8 → [3, 4]；点位 16 → [5, 6]
        when(mockClient.readHoldingRegisters(0, 2)).thenReturn(new short[]{1, 2});
        when(mockClient.readHoldingRegisters(8, 2)).thenReturn(new short[]{3, 4});
        when(mockClient.readHoldingRegisters(16, 2)).thenReturn(new short[]{5, 6});

        List<Row> rows = invokeReadPoints(
                pointsConfig(Arrays.asList(0, 8, 16), 2), mockClient);

        assertEquals(3, rows.size());
        // address 报告的是每个 32bit 组的第一个寄存器地址
        assertEquals(0, rows.get(0).getField(0));
        assertEquals(65538, rows.get(0).getField(1));   // (1 << 16) | 2
        assertEquals(8, rows.get(1).getField(0));
        assertEquals(196612, rows.get(1).getField(1)); // (3 << 16) | 4
        assertEquals(16, rows.get(2).getField(0));
        assertEquals(327686, rows.get(2).getField(1)); // (5 << 16) | 6
    }

    /**
     * 验证间隔读取：wordSize=1 时每个点位产出 1 行
     */
    @Test
    void testReadPoints_wordSize1() throws Exception {
        ModbusTcpClient mockClient = mock(ModbusTcpClient.class);
        when(mockClient.readHoldingRegisters(0, 1)).thenReturn(new short[]{100});
        when(mockClient.readHoldingRegisters(58, 1)).thenReturn(new short[]{200});

        List<Row> rows = invokeReadPoints(
                pointsConfig(Arrays.asList(0, 58), 1), mockClient);

        assertEquals(2, rows.size());
        assertEquals(0, rows.get(0).getField(0));
        assertEquals(100, rows.get(0).getField(1));
        assertEquals(58, rows.get(1).getField(0));
        assertEquals(200, rows.get(1).getField(1));
    }

    /**
     * 验证单个点位协议层异常时被跳过，其余点位正常产出
     */
    @Test
    void testReadPoints_skipsFailedPoint() throws Exception {
        ModbusTcpClient mockClient = mock(ModbusTcpClient.class);
        when(mockClient.readHoldingRegisters(0, 2)).thenReturn(new short[]{1, 2});
        // 点位 8 在设备上不存在，返回异常码 02
        when(mockClient.readHoldingRegisters(8, 2))
                .thenThrow(new ModbusProtocolException(2, "Modbus 异常响应: 异常码=2"));
        when(mockClient.readHoldingRegisters(16, 2)).thenReturn(new short[]{5, 6});

        List<Row> rows = invokeReadPoints(
                pointsConfig(Arrays.asList(0, 8, 16), 2), mockClient);

        assertEquals(2, rows.size());
        assertEquals(0, rows.get(0).getField(0));
        assertEquals(16, rows.get(1).getField(0));
    }

    /**
     * 验证连接层故障时抛出异常，交给 Flink 从 checkpoint 恢复
     */
    @Test
    void testReadPoints_connectionFailurePropagates() throws Exception {
        ModbusTcpClient mockClient = mock(ModbusTcpClient.class);
        when(mockClient.readHoldingRegisters(0, 2)).thenReturn(new short[]{1, 2});
        when(mockClient.readHoldingRegisters(8, 2)).thenThrow(new IOException("连接超时"));

        ModbusSourceConfig config = pointsConfig(Arrays.asList(0, 8), 2);
        IOException ex = assertThrows(IOException.class, () -> invokeReadPoints(config, mockClient));
        assertTrue(ex.getMessage().contains("address=8"));
    }

    /**
     * 验证全部点位失败时不抛异常，产出 0 行
     */
    @Test
    void testReadPoints_allPointsFailed() throws Exception {
        ModbusTcpClient mockClient = mock(ModbusTcpClient.class);
        when(mockClient.readHoldingRegisters(anyInt(), anyInt()))
                .thenThrow(new ModbusProtocolException(2, "Modbus 异常响应: 异常码=2"));

        List<Row> rows = invokeReadPoints(
                pointsConfig(Arrays.asList(0, 8), 2), mockClient);

        assertEquals(0, rows.size());
    }
}
