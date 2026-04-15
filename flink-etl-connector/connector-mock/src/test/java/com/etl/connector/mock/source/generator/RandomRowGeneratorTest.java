package com.etl.connector.mock.source.generator;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RandomRowGeneratorTest {

    @Test
    void testGenerateSingleRow() {
        // 创建 schema
        String[] fieldNames = {"id", "name", "age", "score"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING, Types.INT, Types.DOUBLE};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        // 生成单个 Row
        Row row = RandomRowGenerator.generateRow(schema);

        // 验证 Row 结构正确
        assertEquals(4, row.getArity());
        assertEquals(RowKind.INSERT, row.getKind());

        // 验证字段类型正确
        assertNotNull(row.getField(0));
        assertTrue(row.getField(0) instanceof Long);

        assertNotNull(row.getField(1));
        assertTrue(row.getField(1) instanceof String);

        assertNotNull(row.getField(2));
        assertTrue(row.getField(2) instanceof Integer);

        assertNotNull(row.getField(3));
        assertTrue(row.getField(3) instanceof Double);
    }

    @Test
    void testGenerateMultipleRows() {
        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        // 生成 10 个 Row
        int numRows = 10;
        for (int i = 0; i < numRows; i++) {
            Row row = RandomRowGenerator.generateRow(schema);

            assertEquals(2, row.getArity());
            assertEquals(RowKind.INSERT, row.getKind());

            assertNotNull(row.getField(0));
            assertTrue(row.getField(0) instanceof Long);

            assertNotNull(row.getField(1));
            assertTrue(row.getField(1) instanceof String);
        }
    }

    @Test
    void testGenerateRowWithStringType() {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        Row row = RandomRowGenerator.generateRow(schema);

        assertEquals(1, row.getArity());
        assertNotNull(row.getField(0));
        assertTrue(row.getField(0) instanceof String);
        assertTrue(((String) row.getField(0)).length() <= 100);
    }

    @Test
    void testGenerateRowWithBooleanType() {
        String[] fieldNames = {"flag"};
        TypeInformation<?>[] fieldTypes = {Types.BOOLEAN};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        Row row = RandomRowGenerator.generateRow(schema);

        assertEquals(1, row.getArity());
        assertNotNull(row.getField(0));
        assertTrue(row.getField(0) instanceof Boolean);
    }

    @Test
    void testGenerateRowWithDecimalType() {
        String[] fieldNames = {"amount"};
        TypeInformation<?>[] fieldTypes = {Types.BIG_DEC};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        Row row = RandomRowGenerator.generateRow(schema);

        assertEquals(1, row.getArity());
        assertNotNull(row.getField(0));
        assertTrue(row.getField(0) instanceof java.math.BigDecimal);
    }

    @Test
    void testGenerateRowWithTimestampType() {
        String[] fieldNames = {"ts"};
        TypeInformation<?>[] fieldTypes = {Types.LOCAL_DATE_TIME};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        Row row = RandomRowGenerator.generateRow(schema);

        assertEquals(1, row.getArity());
        assertNotNull(row.getField(0));
        assertTrue(row.getField(0) instanceof java.time.LocalDateTime);
    }

    @Test
    void testGenerateRowWithMixedTypes() {
        String[] fieldNames = {"id", "name", "age", "salary", "active", "created_at"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            Types.STRING,
            Types.INT,
            Types.DOUBLE,
            Types.BOOLEAN,
            Types.LOCAL_DATE_TIME
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        Row row = RandomRowGenerator.generateRow(schema);

        assertEquals(6, row.getArity());

        assertTrue(row.getField(0) instanceof Long);
        assertTrue(row.getField(1) instanceof String);
        assertTrue(row.getField(2) instanceof Integer);
        assertTrue(row.getField(3) instanceof Double);
        assertTrue(row.getField(4) instanceof Boolean);
        assertTrue(row.getField(5) instanceof java.time.LocalDateTime);
    }
}