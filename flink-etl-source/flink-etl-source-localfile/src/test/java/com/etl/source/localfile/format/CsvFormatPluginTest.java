package com.etl.source.localfile.format;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.SchemaConfigException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CsvFormatPlugin 测试
 */
class CsvFormatPluginTest {

    private final CsvFormatPlugin plugin = new CsvFormatPlugin();

    @Test
    void testGetType() {
        assertEquals("csv", plugin.getType());
    }

    @Test
    void testResolveFieldsWithSchema() {
        // 创建配置（带 schema）
        SourceConfig config = createConfigWithSchema();

        // 解析字段
        List<String> fields = plugin.resolveFields(config, new ByteArrayInputStream(new byte[0]));

        // 验证
        assertEquals(3, fields.size());
        assertEquals("id", fields.get(0));
        assertEquals("name", fields.get(1));
        assertEquals("age", fields.get(2));
    }

    @Test
    void testResolveFieldsWithoutSchema() {
        // 创建配置（不带 schema）
        SourceConfig config = createConfigWithoutSchema();

        // 应该抛出异常
        assertThrows(SchemaConfigException.class, () -> {
            plugin.resolveFields(config, new ByteArrayInputStream(new byte[0]));
        });
    }

    @Test
    void testParseWithSchema() throws IOException {
        // 准备测试数据
        String csvContent = "id,name,age\n1,Alice,25\n2,Bob,30";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置
        SourceConfig config = createConfigWithSchema();

        // 字段列表
        List<String> fields = Arrays.asList("id", "name", "age");

        // 解析
        Iterable<Row> rows = plugin.parse(config, inputStream, fields);
        Iterator<Row> iterator = rows.iterator();

        // 验证第一行
        assertTrue(iterator.hasNext());
        Row row1 = iterator.next();
        assertEquals(3, row1.getArity());
        assertEquals(1L, row1.getField(0)); // Long 类型
        assertEquals("Alice", row1.getField(1)); // String 类型
        assertEquals(25, row1.getField(2)); // Integer 类型

        // 验证第二行
        assertTrue(iterator.hasNext());
        Row row2 = iterator.next();
        assertEquals(2L, row2.getField(0));
        assertEquals("Bob", row2.getField(1));
        assertEquals(30, row2.getField(2));

        // 没有更多数据
        assertFalse(iterator.hasNext());
    }

    @Test
    void testParseWithCustomDelimiter() throws IOException {
        // 准备测试数据（分号分隔）
        String csvContent = "id;name\n1;Alice";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置（带 schema 和分号分隔符）
        SourceConfig config = createConfigWithSchemaAndDelimiter(";");

        // 字段列表
        List<String> fields = Arrays.asList("id", "name");

        // 解析
        Iterable<Row> rows = plugin.parse(config, inputStream, fields);
        Iterator<Row> iterator = rows.iterator();

        // 验证
        assertTrue(iterator.hasNext());
        Row row = iterator.next();
        assertEquals(1L, row.getField(0));
        assertEquals("Alice", row.getField(1));
    }

    @Test
    void testParseSkipHeader() throws IOException {
        // 准备测试数据
        String csvContent = "header\nvalue1\nvalue2";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置（skipHeader=true）
        SourceConfig config = createConfigWithSingleField();

        // 字段列表
        List<String> fields = Arrays.asList("id");

        // 解析
        Iterable<Row> rows = plugin.parse(config, inputStream, fields);
        Iterator<Row> iterator = rows.iterator();

        // 验证 - 第一行应该是 value1，不是 header
        assertTrue(iterator.hasNext());
        Row row1 = iterator.next();
        assertEquals("value1", row1.getField(0));

        assertTrue(iterator.hasNext());
        Row row2 = iterator.next();
        assertEquals("value2", row2.getField(0));
    }

    @Test
    void testParseEmptyFile() {
        // 准备空的测试数据
        String csvContent = "id,name,age\n"; // 只有头部
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置
        SourceConfig config = createConfigWithSchema();

        // 字段列表
        List<String> fields = Arrays.asList("id", "name", "age");

        // 解析
        Iterable<Row> rows = plugin.parse(config, inputStream, fields);
        Iterator<Row> iterator = rows.iterator();

        // 没有数据行
        assertFalse(iterator.hasNext());
    }

    /**
     * 创建测试配置（带 schema）
     */
    private SourceConfig createConfigWithSchema() {
        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("tableName", "test_table");
        schemaMap.put("fields", Arrays.asList(
            Map.of("name", "id", "type", "long"),
            Map.of("name", "name", "type", "string"),
            Map.of("name", "age", "type", "int")
        ));

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("schema", schemaMap);
        configMap.put("skipHeader", true);
        return new SourceConfig("localfile", configMap);
    }

    /**
     * 创建测试配置（不带 schema）
     */
    private SourceConfig createConfigWithoutSchema() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("skipHeader", true);
        return new SourceConfig("localfile", configMap);
    }

    /**
     * 创建测试配置（带 schema 和自定义分隔符）
     */
    private SourceConfig createConfigWithSchemaAndDelimiter(String delimiter) {
        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("tableName", "test_table");
        schemaMap.put("fields", Arrays.asList(
            Map.of("name", "id", "type", "long"),
            Map.of("name", "name", "type", "string")
        ));

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("schema", schemaMap);
        configMap.put("skipHeader", true);
        configMap.put("delimiter", delimiter);
        return new SourceConfig("localfile", configMap);
    }

    /**
     * 创建测试配置（单个字段）
     */
    private SourceConfig createConfigWithSingleField() {
        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("tableName", "test_table");
        schemaMap.put("fields", Arrays.asList(
            Map.of("name", "id", "type", "string")
        ));

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("schema", schemaMap);
        configMap.put("skipHeader", true);
        return new SourceConfig("localfile", configMap);
    }
}