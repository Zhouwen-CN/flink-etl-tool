package com.etl.core.schema;

import com.etl.core.exception.TypeConversionException;
import com.etl.core.utils.JsonUtils;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Function;

/**
 * 类型转换器
 * 将原始值转换为目标类型（基于 Flink TypeInformation）
 */
public class TypeConverter {

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // region 类型转换器映射（优化 if-else 链）

    /** 字符串值转换器映射 */
    private static final Map<TypeInformation<?>, Function<String, Object>> STRING_CONVERTERS = new HashMap<>();

    /** 类型兼容性检查映射 */
    private static final Map<TypeInformation<?>, Function<Object, Boolean>> TYPE_COMPATIBILITY_CHECKERS = new HashMap<>();

    /** JsonNode 转换器映射 */
    private static final Map<TypeInformation<?>, Function<JsonNode, Object>> JSON_NODE_CONVERTERS = new HashMap<>();

    /** JsonNode 数组元素转换器映射 */
    private static final Map<TypeInformation<?>, Function<JsonNode, Object>> JSON_ARRAY_ELEMENT_CONVERTERS = new HashMap<>();

    static {
        // 初始化字符串转换器
        STRING_CONVERTERS.put(Types.STRING, v -> v);
        STRING_CONVERTERS.put(Types.INT, Integer::parseInt);
        STRING_CONVERTERS.put(Types.LONG, Long::parseLong);
        STRING_CONVERTERS.put(Types.DOUBLE, Double::parseDouble);
        STRING_CONVERTERS.put(Types.BOOLEAN, TypeConverter::parseBoolean);
        STRING_CONVERTERS.put(Types.BIG_DEC, BigDecimal::new);
        STRING_CONVERTERS.put(Types.LOCAL_DATE_TIME, v -> LocalDateTime.parse(v, DEFAULT_TIMESTAMP_FORMAT));

        // 初始化类型兼容性检查器
        TYPE_COMPATIBILITY_CHECKERS.put(Types.STRING, v -> v instanceof String);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.INT, v -> v instanceof Integer);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.LONG, v -> v instanceof Long);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.DOUBLE, v -> v instanceof Double);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.BOOLEAN, v -> v instanceof Boolean);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.BIG_DEC, v -> v instanceof BigDecimal);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.LOCAL_DATE_TIME, v -> v instanceof LocalDateTime || v instanceof java.sql.Timestamp);

        // 初始化 JsonNode 转换器
        JSON_NODE_CONVERTERS.put(Types.STRING, JsonNode::asText);
        JSON_NODE_CONVERTERS.put(Types.INT, JsonNode::asInt);
        JSON_NODE_CONVERTERS.put(Types.LONG, JsonNode::asLong);
        JSON_NODE_CONVERTERS.put(Types.DOUBLE, JsonNode::asDouble);
        JSON_NODE_CONVERTERS.put(Types.BOOLEAN, JsonNode::asBoolean);
        JSON_NODE_CONVERTERS.put(Types.BIG_DEC, node -> new BigDecimal(node.asText()));
        JSON_NODE_CONVERTERS.put(Types.LOCAL_DATE_TIME, node -> LocalDateTime.parse(node.asText(), DEFAULT_TIMESTAMP_FORMAT));

        // 初始化 JsonNode 数组元素转换器（处理 null 情况）
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.STRING, node -> node.isNull() ? null : node.asText());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.INT, node -> node.isNull() ? null : node.asInt());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.LONG, node -> node.isNull() ? null : node.asLong());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.DOUBLE, node -> node.isNull() ? null : node.asDouble());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.BOOLEAN, node -> node.isNull() ? null : node.asBoolean());
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.BIG_DEC, node -> node.isNull() ? null : new BigDecimal(node.asText()));
        JSON_ARRAY_ELEMENT_CONVERTERS.put(Types.LOCAL_DATE_TIME, node -> node.isNull() ? null : LocalDateTime.parse(node.asText(), DEFAULT_TIMESTAMP_FORMAT));
    }
    // endregion

    private TypeConverter() {
        // 私有构造函数，防止实例化
    }

    // region Object 转换（用于 CSV 等）
    /**
     * 将原始值转换为目标类型
     *
     * @param value 原始值（通常是 String）
     * @param fieldName 字段名（用于错误信息）
     * @param targetType 目标类型（Flink TypeInformation）
     * @return 转换后的值
     * @throws TypeConversionException 转换失败时抛出
     */
    public static Object convertFromValue(Object value, String fieldName, TypeInformation<?> targetType) {
        if (value == null) {
            return null;
        }

        // 如果已经是目标类型或兼容类型，直接返回
        if (isCompatibleType(value, targetType)) {
            return value;
        }

        String strValue = String.valueOf(value);

        try {
            Function<String, Object> converter = STRING_CONVERTERS.get(targetType);
            if (converter != null) {
                return converter.apply(strValue);
            }
            // 未知类型，返回原值
            return value;
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new TypeConversionException(fieldName, strValue, targetType, e);
        }
    }

    /**
     * 检查值是否已经是目标类型
     */
    private static boolean isCompatibleType(Object value, TypeInformation<?> targetType) {
        Function<Object, Boolean> checker = TYPE_COMPATIBILITY_CHECKERS.get(targetType);
        return checker != null && checker.apply(value);
    }

    /**
     * 解析布尔值（支持多种格式）
     */
    private static Boolean parseBoolean(String value) {
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)) {
            return false;
        }
        throw new NumberFormatException("无法解析为布尔值: " + value);
    }
    // endregion

    // region JsonNode 转换（用于 JSON 数据）

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
     * 将 JsonNode 转换为 Flink Row
     *
     * @param node   JsonNode 节点
     * @param schema Schema 定义
     * @return Row 对象
     */
    private static Row convertJsonToRow(JsonNode node, EtlSchema schema) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("期望对象类型，但得到: " + (node == null ? "null" : node.getNodeType()));
        }

        int fieldCount = schema.getFieldCount();
        Row row = Row.withPositions(fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = schema.getFieldName(i);
            JsonNode fieldNode = node.get(fieldName);
            Object value = convertFromJsonNode(fieldNode, schema.getFieldType(i));
            row.setField(i, value);
        }

        return row;
    }

    /**
     * 将 JsonNode 转换为目标类型
     *
     * @param node JsonNode 节点
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
     * @param node JsonNode 数组节点
     * @param arrayType 数组类型信息
     * @return 包装类型数组（Integer[], Long[], String[] 等）
     */
    private static Object convertJsonArray(JsonNode node, TypeInformation<?> arrayType) {
        if (node == null || !node.isArray()) {
            throw new IllegalArgumentException("期望数组类型，但得到: " + (node == null ? "null" : node.getNodeType()));
        }

        int size = node.size();
        TypeInformation<?> componentType = getComponentType(arrayType);

        // 获取元素转换器
        Function<JsonNode, Object> elementConverter = JSON_ARRAY_ELEMENT_CONVERTERS.get(componentType);

        // 根据类型创建对应数组
        if (Types.STRING.equals(componentType)) {
            String[] array = new String[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = (String) elementConverter.apply(element);
            }
            return array;
        } else if (Types.INT.equals(componentType)) {
            Integer[] array = new Integer[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = (Integer) elementConverter.apply(element);
            }
            return array;
        } else if (Types.LONG.equals(componentType)) {
            Long[] array = new Long[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = (Long) elementConverter.apply(element);
            }
            return array;
        } else if (Types.DOUBLE.equals(componentType)) {
            Double[] array = new Double[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = (Double) elementConverter.apply(element);
            }
            return array;
        } else if (Types.BOOLEAN.equals(componentType)) {
            Boolean[] array = new Boolean[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = (Boolean) elementConverter.apply(element);
            }
            return array;
        } else if (Types.BIG_DEC.equals(componentType)) {
            BigDecimal[] array = new BigDecimal[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = (BigDecimal) elementConverter.apply(element);
            }
            return array;
        } else if (Types.LOCAL_DATE_TIME.equals(componentType)) {
            LocalDateTime[] array = new LocalDateTime[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = (LocalDateTime) elementConverter.apply(element);
            }
            return array;
        }

        throw new IllegalArgumentException("不支持的基本类型数组: " + componentType);
    }

    /**
     * 将 JsonNode 转换为 Flink Row（基于 RowTypeInfo）
     *
     * @param node JsonNode 节点
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

    // endregion

    // region Row 转 JsonNode（用于 Kafka Sink 序列化）

    /**
     * 将 Flink Row 转换为 Jackson JsonNode
     * 与 convertJsonToRow() 形成对称，用于 Kafka Sink 序列化
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

    // endregion

    // region JDBC 类型映射
    /**
     * 根据 JDBC java.sql.Types 转换为 Flink TypeInformation
     * 从 FlinkTypeConverter 迁移
     *
     * @param sqlType JDBC SQL 类型常量（来自 java.sql.Types）
     * @return 对应的 Flink TypeInformation
     */
    public static TypeInformation<?> fromSqlType(int sqlType) {
        // 注意：使用完全限定名避免与 Flink Types 冲突
        switch (sqlType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB:
            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NCLOB:
                return Types.STRING;

            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return Types.BOOLEAN;

            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
                return Types.INT;

            case java.sql.Types.BIGINT:
                return Types.LONG;

            case java.sql.Types.REAL:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
                return Types.DOUBLE;

            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                return Types.BIG_DEC;

            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                return Types.LOCAL_DATE_TIME;

            default:
                return Types.STRING;
        }
    }
    // endregion
}