package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.connector.modbus.source.config.WordOrder;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
                .bounded(true).intervalMs(1000)
                .build();
        ModbusSplit split = new ModbusSplit(config);

        // 设置 currentSplit 字段
        java.lang.reflect.Field currentSplitField = ModbusSplitReader.class.getDeclaredField("currentSplit");
        currentSplitField.setAccessible(true);
        currentSplitField.set(reader, split);

        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        Method method = ModbusSplitReader.class.getDeclaredMethod(
                "processRegisterData", short[].class, int.class, int.class, ModbusSourceConfig.class,
                RecordsBySplits.Builder.class);
        method.setAccessible(true);
        method.invoke(reader, data, startAddr, wordSize, config, builder);

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
}
