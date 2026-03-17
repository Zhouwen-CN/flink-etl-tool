package com.etl.core.schema;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class SchemaParserTest {

    @Test
    void parse_shouldReturnSchema_forValidConfig() {
        Map<String, Object> field1 = new HashMap<>();
        field1.put("name", "id");
        field1.put("type", "long");

        Map<String, Object> field2 = new HashMap<>();
        field2.put("name", "name");
        field2.put("type", "string");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "users");
        schemaConfig.put("fields", List.of(field1, field2));

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertNotNull(schema);
        assertEquals("users", schema.getTableName());
        assertEquals(2, schema.getFields().size());
        assertEquals("id", schema.getField(0).getName());
        assertEquals(EtlFieldType.LONG, schema.getField(0).getType());
        assertEquals("name", schema.getField(1).getName());
        assertEquals(EtlFieldType.STRING, schema.getField(1).getType());
    }

    @Test
    void parse_shouldReturnNull_forNullInput() {
        assertNull(SchemaParser.parse(null));
    }

    @Test
    void parse_shouldThrowException_forNonMapInput() {
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse("not a map"));
    }

    @Test
    void parse_shouldThrowException_whenFieldsMissing() {
        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "users");
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
    }

    @Test
    void parse_shouldThrowException_whenFieldsNotList() {
        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "users");
        schemaConfig.put("fields", "not a list");
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
    }

    @Test
    void parse_shouldThrowException_whenFieldNameMissing() {
        Map<String, Object> field = new HashMap<>();
        field.put("type", "string");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "users");
        schemaConfig.put("fields", List.of(field));

        SchemaConfigException ex = assertThrows(SchemaConfigException.class,
            () -> SchemaParser.parse(schemaConfig));
        assertTrue(ex.getMessage().contains("缺少 'name'"));
    }

    @Test
    void parse_shouldThrowException_whenFieldTypeMissing() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "users");
        schemaConfig.put("fields", List.of(field));

        SchemaConfigException ex = assertThrows(SchemaConfigException.class,
            () -> SchemaParser.parse(schemaConfig));
        assertTrue(ex.getMessage().contains("缺少 'type'"));
    }

    @Test
    void parse_shouldThrowException_forUnsupportedType() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");
        field.put("type", "unsupported");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "users");
        schemaConfig.put("fields", List.of(field));

        SchemaConfigException ex = assertThrows(SchemaConfigException.class,
            () -> SchemaParser.parse(schemaConfig));
        assertTrue(ex.getMessage().contains("不支持"));
    }

    @Test
    void parse_shouldHandleCaseInsensitiveType() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");
        field.put("type", "LONG"); // 大写

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "users");
        schemaConfig.put("fields", List.of(field));

        EtlSchema schema = SchemaParser.parse(schemaConfig);
        assertEquals(EtlFieldType.LONG, schema.getField(0).getType());
    }

    @Test
    void parse_shouldParseTableName() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");
        field.put("type", "long");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "users");
        schemaConfig.put("fields", List.of(field));

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertNotNull(schema);
        assertEquals("users", schema.getTableName());
    }

    @Test
    void parse_shouldThrowException_whenTableNameMissing() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");
        field.put("type", "long");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("fields", List.of(field));
        // 不设置 tableName

        SchemaConfigException ex = assertThrows(SchemaConfigException.class,
            () -> SchemaParser.parse(schemaConfig));
        assertTrue(ex.getMessage().contains("tableName"));
    }

    @Test
    void parse_shouldThrowException_whenTableNameEmpty() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");
        field.put("type", "long");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("tableName", "   "); // 空白字符串
        schemaConfig.put("fields", List.of(field));

        SchemaConfigException ex = assertThrows(SchemaConfigException.class,
            () -> SchemaParser.parse(schemaConfig));
        assertTrue(ex.getMessage().contains("tableName"));
    }
}