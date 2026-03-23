package com.etl.core.schema;

import com.etl.core.exception.TypeConversionException;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * 类型转换器
 * 将原始值转换为目标类型（基于 Flink TypeInformation）
 */
public class TypeConverter {

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
            // 根据 TypeInformation 判断目标类型
            // 注意：使用 equals() 而不是 ==，因为反序列化后对象引用可能不同
            if (Types.STRING.equals(targetType)) {
                return strValue;
            } else if (Types.BOOLEAN.equals(targetType)) {
                return parseBoolean(strValue);
            } else if (Types.INT.equals(targetType)) {
                return Integer.parseInt(strValue);
            } else if (Types.LONG.equals(targetType)) {
                return Long.parseLong(strValue);
            } else if (Types.DOUBLE.equals(targetType)) {
                return Double.parseDouble(strValue);
            } else if (Types.BIG_DEC.equals(targetType)) {
                return new BigDecimal(strValue);
            } else if (Types.LOCAL_DATE_TIME.equals(targetType)) {
                return LocalDateTime.parse(strValue, DEFAULT_TIMESTAMP_FORMAT);
            } else {
                // 未知类型，返回原值
                return value;
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new TypeConversionException(fieldName, strValue, targetType, e);
        }
    }

    /**
     * 检查值是否已经是目标类型
     */
    private static boolean isCompatibleType(Object value, TypeInformation<?> targetType) {
        if (Types.STRING.equals(targetType)) {
            return value instanceof String;
        } else if (Types.BOOLEAN.equals(targetType)) {
            return value instanceof Boolean;
        } else if (Types.INT.equals(targetType)) {
            return value instanceof Integer;
        } else if (Types.LONG.equals(targetType)) {
            return value instanceof Long;
        } else if (Types.DOUBLE.equals(targetType)) {
            return value instanceof Double;
        } else if (Types.BIG_DEC.equals(targetType)) {
            return value instanceof BigDecimal;
        } else if (Types.LOCAL_DATE_TIME.equals(targetType)) {
            return value instanceof LocalDateTime || value instanceof java.sql.Timestamp;
        }
        return false;
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

        if (Types.STRING.equals(targetType)) {
            return node.asText();
        } else if (Types.INT.equals(targetType)) {
            return node.asInt();
        } else if (Types.LONG.equals(targetType)) {
            return node.asLong();
        } else if (Types.DOUBLE.equals(targetType)) {
            return node.asDouble();
        } else if (Types.BOOLEAN.equals(targetType)) {
            return node.asBoolean();
        } else if (Types.BIG_DEC.equals(targetType)) {
            return new BigDecimal(node.asText());
        } else if (Types.LOCAL_DATE_TIME.equals(targetType)) {
            return LocalDateTime.parse(node.asText(), DEFAULT_TIMESTAMP_FORMAT);
        } else if (targetType instanceof RowTypeInfo) {
            return convertJsonToRow(node, targetType);
        } else if (targetType instanceof ObjectArrayTypeInfo || targetType instanceof BasicArrayTypeInfo) {
            return convertJsonArray(node, targetType);
        } else {
            throw new IllegalArgumentException("不支持的类型 " + node + " : " + targetType);
        }

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

        if (Types.STRING.equals(componentType)) {
            String[] array = new String[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = element.isNull() ? null : element.asText();
            }
            return array;
        } else if (Types.INT.equals(componentType)) {
            Integer[] array = new Integer[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = element.isNull() ? null : element.asInt();
            }
            return array;
        } else if (Types.LONG.equals(componentType)) {
            Long[] array = new Long[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = element.isNull() ? null : element.asLong();
            }
            return array;
        } else if (Types.DOUBLE.equals(componentType)) {
            Double[] array = new Double[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = element.isNull() ? null : element.asDouble();
            }
            return array;
        } else if (Types.BOOLEAN.equals(componentType)) {
            Boolean[] array = new Boolean[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = element.isNull() ? null : element.asBoolean();
            }
            return array;
        } else if (Types.BIG_DEC.equals(componentType)) {
            BigDecimal[] array = new BigDecimal[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = element.isNull() ? null : new BigDecimal(element.asText());
            }
            return array;
        } else if (Types.LOCAL_DATE_TIME.equals(componentType)) {
            LocalDateTime[] array = new LocalDateTime[size];
            int i = 0;
            for (JsonNode element : node) {
                array[i++] = element.isNull() ? null : LocalDateTime.parse(node.asText(), DEFAULT_TIMESTAMP_FORMAT);
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