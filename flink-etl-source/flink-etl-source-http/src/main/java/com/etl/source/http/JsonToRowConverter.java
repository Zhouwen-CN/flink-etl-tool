package com.etl.source.http;

import com.etl.core.schema.EtlSchema;
import com.etl.core.utils.JsonUtils;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON 转 Row 转换器
 * 根据 Schema 定义将 JSON 数据转换为 Flink Row
 */
@Slf4j
public class JsonToRowConverter {

    /**
     * 从 JSON 字符串提取数据并转换为 Row 列表
     *
     * @param jsonResponse JSON 响应字符串
     * @param dataPath     JSONPath 表达式，可为 null
     * @param schema       Schema 定义
     * @return Row 列表
     */
    public static List<Row> convert(String jsonResponse, String dataPath, EtlSchema schema) {
        List<Row> rows = new ArrayList<>();

        try {
            JsonNode rootNode;

            // 解析 JSON
            try {
                rootNode = JsonUtils.getByJsonPath(jsonResponse, dataPath);
            } catch (PathNotFoundException e) {
                throw new IllegalArgumentException("JSONPath 提取失败: " + dataPath, e);
            }

            if (rootNode == null) {
                throw new IllegalArgumentException("提取的数据为空");
            }

            // 根据 root 类型处理
            if (rootNode.isArray()) {
                // JSONArray: 遍历数组
                ArrayNode arrayNode = (ArrayNode) rootNode;
                for (JsonNode element : arrayNode) {
                    Row row = convertToRow(element, schema);
                    rows.add(row);
                }
            } else if (rootNode.isObject()) {
                // JSONObject: 单条记录
                Row row = convertToRow(rootNode, schema);
                rows.add(row);
            } else {
                throw new IllegalArgumentException("提取的数据既不是 JSONObject 也不是 JSONArray: " + rootNode.getNodeType());
            }

        } catch (Exception e) {
            log.error("JSON 转换失败: {}", e.getMessage(), e);
            throw new RuntimeException("JSON 转换失败: " + e.getMessage(), e);
        }

        return rows;
    }

    /**
     * 将单个 JsonNode 转换为 Row
     */
    private static Row convertToRow(JsonNode node, EtlSchema schema) {
        int fieldCount = schema.getFieldCount();
        Row row = Row.withPositions(fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = schema.getFieldName(i);
            JsonNode fieldNode = node.get(fieldName);
            Object value = convertValue(fieldNode, schema.getFieldType(i));
            row.setField(i, value);
        }

        return row;
    }

    /**
     * 根据 TypeInformation 转换值
     */
    private static Object convertValue(JsonNode node, TypeInformation<?> type) {
        if (node == null || node.isNull()) {
            return null;
        }

        String typeName = type.getTypeClass().getSimpleName();

        switch (typeName) {
            case "String":
                return node.asText();
            case "Integer":
                return node.asInt();
            case "Long":
                return node.asLong();
            case "Double":
                return node.asDouble();
            case "Boolean":
                return node.asBoolean();
            case "BigDecimal":
                return new BigDecimal(node.asText());
            case "LocalDateTime":
                return LocalDateTime.parse(node.asText(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case "Object[]":
                // 数组类型
                return convertArray(node, type);
            case "Row":
                // 嵌套对象类型
                return convertRow(node, type);
            default:
                // 尝试作为嵌套 Row 处理
                if (type instanceof RowTypeInfo) {
                    return convertRow(node, type);
                }
                return node.asText();
        }
    }

    /**
     * 转换数组类型
     */
    private static Object[] convertArray(JsonNode node, TypeInformation<?> type) {
        if (!node.isArray()) {
            throw new IllegalArgumentException("期望数组类型，但得到: " + node.getNodeType());
        }

        List<Object> list = new ArrayList<>();
        for (JsonNode element : node) {
            // 简单类型数组的元素类型
            if (type instanceof BasicArrayTypeInfo) {
                BasicArrayTypeInfo<?, ?> arrayTypeInfo = (BasicArrayTypeInfo<?, ?>) type;
                TypeInformation<?> componentType = arrayTypeInfo.getComponentInfo();
                list.add(convertValue(element, componentType));
            } else if (type instanceof ObjectArrayTypeInfo) {
                ObjectArrayTypeInfo<?, ?> arrayTypeInfo = (ObjectArrayTypeInfo<?, ?>) type;
                TypeInformation<?> componentType = arrayTypeInfo.getComponentInfo();
                list.add(convertValue(element, componentType));
            } else {
                list.add(element.asText());
            }
        }

        return list.toArray();
    }

    /**
     * 转换嵌套 Row 类型
     */
    private static Row convertRow(JsonNode node, TypeInformation<?> type) {
        if (!node.isObject()) {
            throw new IllegalArgumentException("期望对象类型，但得到: " + node.getNodeType());
        }

        RowTypeInfo rowTypeInfo = (RowTypeInfo) type;
        String[] fieldNames = rowTypeInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();

        Row row = Row.withPositions(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            JsonNode fieldNode = node.get(fieldNames[i]);
            Object value = convertValue(fieldNode, fieldTypes[i]);
            row.setField(i, value);
        }

        return row;
    }
}