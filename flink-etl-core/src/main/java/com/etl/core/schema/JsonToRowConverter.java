package com.etl.core.schema;

import com.etl.core.constants.DateFormatConstants;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * JSON 转 Row 转换器
 * 处理 JsonNode 到 Flink Row 的转换
 */
public class JsonToRowConverter {

    // region JsonNode 转换器映射

    /**
     * JsonNode 转换器映射
     */
    private static final Map<TypeInformation<?>, Function<JsonNode, Object>> JSON_NODE_CONVERTERS = new HashMap<>();

    /**
     * JsonNode 数组元素转换器映射
     */
    private static final Map<TypeInformation<?>, Function<JsonNode, Object>> JSON_ARRAY_ELEMENT_CONVERTERS = new HashMap<>();

    static {
        // 初始化 JsonNode 转换器
        JSON_NODE_CONVERTERS.put(Types.STRING, JsonNode::asText);
        JSON_NODE_CONVERTERS.put(Types.INT, JsonNode::asInt);
        JSON_NODE_CONVERTERS.put(Types.LONG, JsonNode::asLong);
        JSON_NODE_CONVERTERS.put(Types.DOUBLE, JsonNode::asDouble);
        JSON_NODE_CONVERTERS.put(Types.BOOLEAN, JsonNode::asBoolean);
        JSON_NODE_CONVERTERS.put(Types.BIG_DEC, node -> new BigDecimal(node.asText()));
        JSON_NODE_CONVERTERS.put(Types.LOCAL_DATE_TIME, node -> LocalDateTime.parse(node.asText(), DateFormatConstants.DEFAULT_TIMESTAMP_FORMAT));

        // 初始化 JsonNode 数组元素转换器（处理 null 情况）
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.STRING, node -> node.isNull() ? null : node.asText());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.INT, node -> node.isNull() ? null : node.asInt());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.LONG, node -> node.isNull() ? null : node.asLong());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.DOUBLE, node -> node.isNull() ? null : node.asDouble());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.BOOLEAN, node -> node.isNull() ? null : node.asBoolean());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.BIG_DEC, node -> node.isNull() ? null : new BigDecimal(node.asText()));
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.LOCAL_DATE_TIME, node -> node.isNull() ? null : LocalDateTime.parse(node.asText(), DateFormatConstants.DEFAULT_TIMESTAMP_FORMAT));
    }
    // endregion

    private JsonToRowConverter() {
        // 私有构造函数，防止实例化
    }

    /**
     * 将 JsonNode 转换为 Row 列表
     * 支持 JSONObject（单条记录）和 JSONArray（多条记录）
     *
     * @param node   JsonNode 节点
     * @param schema Schema 定义
     * @return Row 列表
     */
    public static List<Row> convertJsonToRows(JsonNode node, EtlSchema schema) {
        List<Row> rows = new ArrayList<>();

        if (node == null) {
            throw new IllegalArgumentException("提取的数据为空");
        }

        if (node.isArray()) {
            // JSONArray: 遍历数组
            for (JsonNode element : node) {
                Row row = convertJsonToRow(element, schema);
                rows.add(row);
            }
        } else if (node.isObject()) {
            // JSONObject: 单条记录
            Row row = convertJsonToRow(node, schema);
            rows.add(row);
        } else {
            throw new IllegalArgumentException("提取的数据既不是 JSONObject 也不是 JSONArray: " + node.getNodeType());
        }

        return rows;
    }

    /**
     * 将 JsonNode 转换为 Flink Row（基于 RowTypeInfo）
     *
     * @param node        JsonNode 节点
     * @param rowTypeInfo Row 类型信息
     * @return Row 对象
     */
    public static Row convertJsonToRow(JsonNode node, TypeInformation<?> rowTypeInfo) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("期望对象类型，但得到: " + (node == null ? "null" : node.getNodeType()));
        }

        RowTypeInfo rowInfo = (RowTypeInfo) rowTypeInfo;
        String[] fieldNames = rowInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = rowInfo.getFieldTypes();

        Row row = Row.withPositions(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            JsonNode fieldNode = node.get(fieldNames[i]);
            Object value = convertFromJsonNode(fieldNode, fieldTypes[i]);
            row.setField(i, value);
        }

        return row;
    }

    /**
     * 将 JsonNode 转换为 Flink Row（基于 EtlSchema）
     *
     * @param node   JsonNode 节点
     * @param schema Schema 定义
     * @return Row 对象
     */
    public static Row convertJsonToRow(JsonNode node, EtlSchema schema) {
        return convertJsonToRow(node, schema, null);
    }

    /**
     * 将 JsonNode 转换为 Flink Row（基于 EtlSchema）
     *
     * @param node   JsonNode 节点
     * @param schema Schema 定义
     * @param extra  额外的字段，比如元数据
     * @return Row 对象
     */
    public static Row convertJsonToRow(JsonNode node, EtlSchema schema, List<Object> extra) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("期望对象类型，但得到: " + (node == null ? "null" : node.getNodeType()));
        }

        int fieldCount = schema.getFieldCount();
        boolean hasExtra = extra != null && !extra.isEmpty();
        int arity = hasExtra ? fieldCount + extra.size() : fieldCount;
        Row row = Row.withPositions(arity);

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = schema.getFieldName(i);
            JsonNode fieldNode = node.get(fieldName);
            Object value = convertFromJsonNode(fieldNode, schema.getFieldType(i));
            row.setField(i, value);
        }

        if (hasExtra) {
            for (int i = 0; i < extra.size(); i++) {
                int position = fieldCount + i;
                row.setField(position, extra.get(i));
            }
        }

        return row;
    }

    /**
     * 将 JsonNode 转换为目标类型
     *
     * @param node       JsonNode 节点
     * @param targetType 目标类型（Flink TypeInformation）
     * @return 转换后的值
     */
    private static Object convertFromJsonNode(JsonNode node, TypeInformation<?> targetType) {
        if (node == null || node.isNull()) {
            return null;
        }

        // 使用映射表转换基本类型
        Function<JsonNode, Object> converter = JSON_NODE_CONVERTERS.get(targetType);
        if (converter != null) {
            return converter.apply(node);
        }

        // 处理复杂类型
        if (targetType instanceof RowTypeInfo) {
            return convertJsonToRow(node, targetType);
        } else if (targetType instanceof ObjectArrayTypeInfo || targetType instanceof BasicArrayTypeInfo) {
            return convertJsonArray(node, targetType);
        }

        throw new IllegalArgumentException("不支持的类型: " + targetType);
    }

    /**
     * 将 JsonNode 数组转换为包装类型数组
     *
     * @param node      JsonNode 数组节点
     * @param arrayType 数组类型信息
     * @return 包装类型数组（Integer[], Long[], String[], Row[] 等）
     */
    private static Object convertJsonArray(JsonNode node, TypeInformation<?> arrayType) {
        if (node == null || !node.isArray()) {
            throw new IllegalArgumentException("期望数组类型，但得到: " + (node == null ? "null" : node.getNodeType()));
        }

        int size = node.size();
        TypeInformation<?> componentType = getComponentType(arrayType);

        // Row[] 类型数组处理
        if (componentType instanceof RowTypeInfo) {
            // Row[] 数组
            Row[] array = new Row[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = convertJsonToRow(element, componentType);
            }
            return array;
        }

        // 获取元素转换器（用于基本类型）
        Function<JsonNode, Object> elementConverter = JSON_ARRAY_ELEMENT_CONVERTERS.get(componentType);

        // 基本类型数组处理
        if (elementConverter != null) {
            Object array = Array.newInstance(componentType.getTypeClass(), size);
            int i = 0;
            for (JsonNode element : node) {
                Array.set(array, i++, elementConverter.apply(element));
            }
            return array;
        }

        throw new IllegalArgumentException("不支持的数组元素类型: " + componentType);
    }

    /**
     * 获取数组元素类型
     */
    private static TypeInformation<?> getComponentType(TypeInformation<?> arrayType) {
        TypeInformation<?> componentInfo = null;
        if (arrayType instanceof BasicArrayTypeInfo) {
            componentInfo = ((BasicArrayTypeInfo<?, ?>) arrayType).getComponentInfo();
        } else if (arrayType instanceof ObjectArrayTypeInfo) {
            componentInfo = ((ObjectArrayTypeInfo<?, ?>) arrayType).getComponentInfo();
        }
        return componentInfo;
    }
}