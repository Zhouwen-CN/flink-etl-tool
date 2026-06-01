package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ModbusSplitReaderTest {

    private ModbusSplitReader reader;

    @BeforeEach
    void setUp() {
        reader = new ModbusSplitReader();
    }

    /**
     * 通过反射调用 processRegisterData，验证数据处理逻辑
     */
    private List<Row> invokeProcessRegisterData(short[] data, int startAddr, int wordSize) throws Exception {
        // 构造一个 split 用于 splitId
        ModbusSourceConfig config = ModbusSourceConfig.builder()
                .ip("127.0.0.1").port(502).deviceId(1)
                .address(0).count(data.length).wordSize(wordSize)
                .bounded(true).intervalMs(1000)
                .build();
        ModbusSplit split = new ModbusSplit(config);

        // 设置 currentSplit 字段
        java.lang.reflect.Field currentSplitField = ModbusSplitReader.class.getDeclaredField("currentSplit");
        currentSplitField.setAccessible(true);
        currentSplitField.set(reader, split);

        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        Method method = ModbusSplitReader.class.getDeclaredMethod(
                "processRegisterData", short[].class, int.class, int.class, RecordsBySplits.Builder.class);
        method.setAccessible(true);
        method.invoke(reader, data, startAddr, wordSize, builder);

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
        List<Row> rows = invokeProcessRegisterData(data, 0, 1);

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
        List<Row> rows = invokeProcessRegisterData(data, 10, 2);

        assertEquals(2, rows.size());
        assertEquals(10, rows.get(0).getField(0));
        assertEquals(65538, rows.get(0).getField(1));
        assertEquals(12, rows.get(1).getField(0));
        assertEquals(196612, rows.get(1).getField(1)); // (3 << 16) | 4
    }

    @Test
    void testProcessRegisterData_withOffset() throws Exception {
        short[] data = {42};
        List<Row> rows = invokeProcessRegisterData(data, 100, 1);

        assertEquals(1, rows.size());
        assertEquals(100, rows.get(0).getField(0));
        assertEquals(42, rows.get(0).getField(1));
    }
}
