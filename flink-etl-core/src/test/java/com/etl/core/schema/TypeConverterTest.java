package com.etl.core.schema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TypeConverter 测试类
 */
class TypeConverterTest {

    @Test
    void testConvertRowToJsonNode_SimpleRow() {
        // 创建简单 Row（有字段名）
        Row row = Row.withNames();
        row.setField("name", "张三");
        row.setField("age", 25);
        row.setField("score", 95.5);

        // 转换为 JsonNode
        JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

        // 验证结果
        assertNotNull(jsonNode);
        assertTrue(jsonNode.isObject());
        assertEquals("张三", jsonNode.get("name").asText());
        assertEquals(25, jsonNode.get("age").asInt());
        assertEquals(95.5, jsonNode.get("score").asDouble(), 0.001);
    }

    @Test
    void testConvertRowToJsonNode_ComplexRow() {
        // 创建嵌套 Row
        Row addressRow = Row.withNames();
        addressRow.setField("city", "北京");
        addressRow.setField("zip", "100001");

        Row userRow = Row.withNames();
        userRow.setField("id", 1L);
        userRow.setField("name", "李四");
        userRow.setField("address", addressRow);

        // 转换为 JsonNode
        JsonNode jsonNode = TypeConverter.convertRowToJsonNode(userRow);

        // 验证结果
        assertNotNull(jsonNode);
        assertEquals(1L, jsonNode.get("id").asLong());
        assertEquals("李四", jsonNode.get("name").asText());
        JsonNode addressNode = jsonNode.get("address");
        assertNotNull(addressNode);
        assertEquals("北京", addressNode.get("city").asText());
        assertEquals("100001", addressNode.get("zip").asText());
    }

    @Test
    void testConvertRowToJsonNode_WithArray() {
        // 创建包含数组的 Row
        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("tags", new String[]{"tag1", "tag2", "tag3"});

        // 转换为 JsonNode
        JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

        // 验证结果
        assertNotNull(jsonNode);
        assertEquals(1, jsonNode.get("id").asInt());
        JsonNode tagsNode = jsonNode.get("tags");
        assertTrue(tagsNode.isArray());
        assertEquals(3, tagsNode.size());
        assertEquals("tag1", tagsNode.get(0).asText());
        assertEquals("tag2", tagsNode.get(1).asText());
        assertEquals("tag3", tagsNode.get(2).asText());
    }

    @Test
    void testConvertRowToJsonNode_LocalDateTime() {
        // 创建包含 LocalDateTime 的 Row
        LocalDateTime dateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 0);
        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("createTime", dateTime);

        // 转换为 JsonNode
        JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

        // 验证结果
        assertNotNull(jsonNode);
        assertEquals(1, jsonNode.get("id").asInt());
        // LocalDateTime 应该被转为字符串，格式为 yyyy-MM-dd HH:mm:ss
        JsonNode timeNode = jsonNode.get("createTime");
        assertNotNull(timeNode);
        assertTrue(timeNode.isTextual());
        assertEquals("2024-01-15 10:30:00", timeNode.asText());
    }

    @Test
    void testConvertRowToJsonNode_WithNull() {
        // 创建包含 null 的 Row
        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("name", null);
        row.setField("score", 95.5);

        // 转换为 JsonNode
        JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

        // 验证结果
        assertNotNull(jsonNode);
        assertEquals(1, jsonNode.get("id").asInt());
        assertTrue(jsonNode.get("name").isNull());
        assertEquals(95.5, jsonNode.get("score").asDouble(), 0.001);
    }
}