package com.etl.core.schema;

import com.etl.core.utils.JsonUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

import java.lang.reflect.Array;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

/**
 * Row 转 JSON 转换器
 * 处理 Flink Row 到 JsonNode 的转换
 */
public class RowToJsonConverter {

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private RowToJsonConverter() {
        // 私有构造函数，防止实例化
    }

    /**
     * 将 Flink Row 转换为 Jackson JsonNode
     * 与 JsonToRowConverter 形成对称，用于 Kafka Sink 序列化
     *
     * @param row Flink Row 对象
     * @return JsonNode 对象
     */
    public static JsonNode convertRowToJsonNode(Row row) {
        if (row == null) {
            return null;
        }

        // 使用 JsonUtils.MAPPER 创建 ObjectNode
        ObjectMapper mapper = JsonUtils.getMapper();
        ObjectNode objectNode = mapper.createObjectNode();

        // 获取字段名
        Set<String> fieldNames = row.getFieldNames(true);

        if (fieldNames != null && !fieldNames.isEmpty()) {
            // 有字段名：遍历字段名
            for (String fieldName : fieldNames) {
                Object value = row.getField(fieldName);
                JsonNode fieldNode = convertValueToJsonNode(value, mapper);
                objectNode.set(fieldName, fieldNode);
            }
        } else {
            // 无字段名：使用位置索引
            int arity = row.getArity();
            for (int i = 0; i < arity; i++) {
                Object value = row.getField(i);
                JsonNode fieldNode = convertValueToJsonNode(value, mapper);
                objectNode.set("field" + i, fieldNode);
            }
        }

        return objectNode;
    }

    /**
     * 将单个值转换为 JsonNode
     *
     * @param value 字段值
     * @param mapper ObjectMapper 实例
     * @return JsonNode
     */
    private static JsonNode convertValueToJsonNode(Object value, ObjectMapper mapper) {
        if (value == null) {
            return mapper.getNodeFactory().nullNode();
        }

        // LocalDateTime 使用固定格式
        if (value instanceof LocalDateTime) {
            String formatted = ((LocalDateTime) value).format(DEFAULT_TIMESTAMP_FORMAT);
            return mapper.getNodeFactory().textNode(formatted);
        }

        // 如果是 Row，递归转换
        if (value instanceof Row) {
            return convertRowToJsonNode((Row) value);
        }

        // 如果是数组，处理数组元素（支持 Row[] 等嵌套对象数组）
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            ArrayNode arrayNode = mapper.createArrayNode();
            for (int i = 0; i < length; i++) {
                Object element = Array.get(value, i);
                JsonNode elementNode = convertValueToJsonNode(element, mapper);
                arrayNode.add(elementNode);
            }
            return arrayNode;
        }

        // 其他类型：使用 mapper.valueToTree
        return mapper.valueToTree(value);
    }
}