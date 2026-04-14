package com.etl.source.mock.generator;

import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.source.mock.config.MockSourceConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataRowGeneratorTest {

    @Test
    void testParseRowKind() {
        assertEquals(RowKind.INSERT, DataRowGenerator.parseRowKind("INSERT"));
        assertEquals(RowKind.UPDATE_BEFORE, DataRowGenerator.parseRowKind("UPDATE_BEFORE"));
        assertEquals(RowKind.UPDATE_AFTER, DataRowGenerator.parseRowKind("UPDATE_AFTER"));
        assertEquals(RowKind.DELETE, DataRowGenerator.parseRowKind("DELETE"));

        // 大小写不敏感
        assertEquals(RowKind.INSERT, DataRowGenerator.parseRowKind("insert"));
        assertEquals(RowKind.INSERT, DataRowGenerator.parseRowKind("Insert"));
    }

    @Test
    void testParseInvalidRowKind() {
        assertThrows(SchemaConfigException.class, () -> {
            DataRowGenerator.parseRowKind("INVALID");
        });
    }

    @Test
    void testGenerateRowsFromConfig() {
        // 创建 schema
        String[] fieldNames = {"id", "name", "age"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING, Types.INT};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        // 创建 rows 配置
        List<MockSourceConfig.RowData> rowsData = Arrays.asList(
            createRowData("INSERT", Map.of("id", 1L, "name", "Alice", "age", 25)),
            createRowData("UPDATE_AFTER", Map.of("id", 2L, "name", "Bob", "age", 30)),
            createRowData("DELETE", Map.of("id", 3L, "name", "Charlie", "age", 28))
        );

        // 生成 rows
        List<Row> rows = DataRowGenerator.generateRows(rowsData, schema);

        // 验证结果
        assertEquals(3, rows.size());

        Row row1 = rows.get(0);
        assertEquals(RowKind.INSERT, row1.getKind());
        assertEquals(1L, row1.getField(0));
        assertEquals("Alice", row1.getField(1));
        assertEquals(25, row1.getField(2));

        Row row2 = rows.get(1);
        assertEquals(RowKind.UPDATE_AFTER, row2.getKind());
        assertEquals(2L, row2.getField(0));

        Row row3 = rows.get(2);
        assertEquals(RowKind.DELETE, row3.getKind());
        assertEquals(3L, row3.getField(0));
    }

    @Test
    void testGenerateRowsMissingField() {
        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<MockSourceConfig.RowData> rowsData = Arrays.asList(
            createRowData("INSERT", Map.of("id", 1L))  // 缺失 name 字段
        );

        assertThrows(SchemaConfigException.class, () -> {
            DataRowGenerator.generateRows(rowsData, schema);
        });
    }

    private MockSourceConfig.RowData createRowData(String kind, Map<String, Object> data) {
        MockSourceConfig.RowData rowData = new MockSourceConfig.RowData();
        rowData.setKind(kind);
        rowData.setData(data);
        return rowData;
    }
}