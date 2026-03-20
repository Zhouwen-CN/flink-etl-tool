package com.etl.core.schema;

import com.etl.core.exception.SchemaConfigException;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaParserTest {

    @Test
    void testParseObjectFormat() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("id", "LONG");
        schemaConfig.put("name", "STRING");
        schemaConfig.put("age", "INT");

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertNotNull(schema);
        assertEquals(3, schema.getFieldCount());
        assertEquals("id", schema.getFieldName(0));
        assertEquals(Types.LONG, schema.getFieldType(0));
        assertEquals("name", schema.getFieldName(1));
        assertEquals(Types.STRING, schema.getFieldType(1));
        assertEquals("age", schema.getFieldName(2));
        assertEquals(Types.INT, schema.getFieldType(2));
    }

    @Test
    void testParseNullReturnsNull() {
        assertNull(SchemaParser.parse(null));
    }

    @Test
    void testParseInvalidFormatThrowsException() {
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse("invalid"));
    }

    @Test
    void testParseInvalidTypeThrowsException() {
        Map<String, Object> schemaConfig = Map.of("id", "INVALID_TYPE");
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
    }

    @Test
    void testAllSupportedTypes() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("f1", "STRING");
        schemaConfig.put("f2", "BOOLEAN");
        schemaConfig.put("f3", "INT");
        schemaConfig.put("f4", "LONG");
        schemaConfig.put("f5", "DOUBLE");
        schemaConfig.put("f6", "DECIMAL");
        schemaConfig.put("f7", "TIMESTAMP");

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertEquals(Types.STRING, schema.getFieldType(0));
        assertEquals(Types.BOOLEAN, schema.getFieldType(1));
        assertEquals(Types.INT, schema.getFieldType(2));
        assertEquals(Types.LONG, schema.getFieldType(3));
        assertEquals(Types.DOUBLE, schema.getFieldType(4));
        assertEquals(Types.BIG_DEC, schema.getFieldType(5));
        assertEquals(Types.LOCAL_DATE_TIME, schema.getFieldType(6));
    }
}