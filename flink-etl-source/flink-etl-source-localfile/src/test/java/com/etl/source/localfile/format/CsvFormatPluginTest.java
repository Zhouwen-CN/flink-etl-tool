package com.etl.source.localfile.format;

import com.etl.core.config.SourceConfig;
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
    void testResolveFieldsWithHeader() {
        // 准备测试数据
        String csvContent = "id,name,age\n1,Alice,25\n2,Bob,30";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置
        SourceConfig config = createConfig(true, null, null);

        // 解析字段
        List<String> fields = plugin.resolveFields(config, inputStream);

        // 验证
        assertEquals(3, fields.size());
        assertEquals("id", fields.get(0));
        assertEquals("name", fields.get(1));
        assertEquals("age", fields.get(2));
    }

    @Test
    void testResolveFieldsWithCustomDelimiter() {
        // 准备测试数据（分号分隔）
        String csvContent = "id;name;age\n1;Alice;25";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置
        SourceConfig config = createConfig(true, ";", null);

        // 解析字段
        List<String> fields = plugin.resolveFields(config, inputStream);

        // 验证
        assertEquals(3, fields.size());
        assertEquals("id", fields.get(0));
        assertEquals("name", fields.get(1));
        assertEquals("age", fields.get(2));
    }

    @Test
    void testResolveFieldsWithoutHeader() {
        // 创建配置（header=false，指定 columns）
        SourceConfig config = createConfig(false, null, Arrays.asList("col1", "col2", "col3"));

        // 解析字段
        List<String> fields = plugin.resolveFields(config, new ByteArrayInputStream(new byte[0]));

        // 验证
        assertEquals(3, fields.size());
        assertEquals("col1", fields.get(0));
        assertEquals("col2", fields.get(1));
        assertEquals("col3", fields.get(2));
    }

    @Test
    void testResolveFieldsWithoutHeaderAndColumns() {
        // 创建配置（header=false，但没有 columns）
        SourceConfig config = createConfig(false, null, null);

        // 应该抛出异常
        assertThrows(RuntimeException.class, () -> {
            plugin.resolveFields(config, new ByteArrayInputStream(new byte[0]));
        });
    }

    @Test
    void testParse() throws IOException {
        // 准备测试数据
        String csvContent = "id,name,age\n1,Alice,25\n2,Bob,30";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置
        SourceConfig config = createConfig(true, null, null);

        // 字段列表
        List<String> fields = Arrays.asList("id", "name", "age");

        // 解析
        Iterable<Row> rows = plugin.parse(config, inputStream, fields);
        Iterator<Row> iterator = rows.iterator();

        // 验证第一行
        assertTrue(iterator.hasNext());
        Row row1 = iterator.next();
        assertEquals(3, row1.getArity());
        assertEquals("1", row1.getField(0));
        assertEquals("Alice", row1.getField(1));
        assertEquals("25", row1.getField(2));

        // 验证第二行
        assertTrue(iterator.hasNext());
        Row row2 = iterator.next();
        assertEquals("2", row2.getField(0));
        assertEquals("Bob", row2.getField(1));
        assertEquals("30", row2.getField(2));

        // 没有更多数据
        assertFalse(iterator.hasNext());
    }

    @Test
    void testParseWithCustomDelimiter() throws IOException {
        // 准备测试数据（分号分隔）
        String csvContent = "id;name\n1;Alice";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置
        SourceConfig config = createConfig(true, ";", null);

        // 字段列表
        List<String> fields = Arrays.asList("id", "name");

        // 解析
        Iterable<Row> rows = plugin.parse(config, inputStream, fields);
        Iterator<Row> iterator = rows.iterator();

        // 验证
        assertTrue(iterator.hasNext());
        Row row = iterator.next();
        assertEquals("1", row.getField(0));
        assertEquals("Alice", row.getField(1));
    }

    @Test
    void testParseEmptyFile() {
        // 准备空的测试数据
        String csvContent = "id,name,age\n"; // 只有头部
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvContent.getBytes(StandardCharsets.UTF_8));

        // 创建配置
        SourceConfig config = createConfig(true, null, null);

        // 字段列表
        List<String> fields = Arrays.asList("id", "name", "age");

        // 解析
        Iterable<Row> rows = plugin.parse(config, inputStream, fields);
        Iterator<Row> iterator = rows.iterator();

        // 没有数据行
        assertFalse(iterator.hasNext());
    }

    /**
     * 创建测试配置
     */
    private SourceConfig createConfig(boolean hasHeader, String delimiter, List<String> columns) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("header", hasHeader);
        if (delimiter != null) {
            configMap.put("delimiter", delimiter);
        }
        if (columns != null) {
            configMap.put("columns", columns);
        }
        return new SourceConfig("localfile", configMap);
    }
}