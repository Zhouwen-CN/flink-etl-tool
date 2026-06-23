package com.etl.core.schema;

import com.etl.core.constants.DateFormatConstants;
import com.etl.core.exception.TypeConversionException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * 类型转换器
 * 将原始值转换为目标类型（基于 Flink TypeInformation）
 *
 * <ul>
 *   <li>{@link SqlTypeConverter} - SQL 类型转换</li>
 *   <li>{@link JsonToRowConverter} - JSON 转 Row</li>
 *   <li>{@link RowToJsonConverter} - Row 转 JSON</li>
 *   <li>{@link TypeConverter} - 将 value 根据 flink 类型信息转换成对应的类型</li>
 * </ul>
 */
public class TypeConverter {

    private TypeConverter() {
        // 私有构造函数，防止实例化
    }

    /**
     * 字符串值转换器映射
     */
    private static final Map<TypeInformation<?>, Function<String, Object>> STRING_CONVERTERS = new HashMap<>();

    /**
     * 类型兼容性检查映射
     */
    private static final Map<TypeInformation<?>, Function<Object, Boolean>> TYPE_COMPATIBILITY_CHECKERS = new HashMap<>();

    static {
        // 初始化字符串转换器
        STRING_CONVERTERS.put(Types.STRING, v -> v);
        STRING_CONVERTERS.put(Types.INT, Integer::parseInt);
        STRING_CONVERTERS.put(Types.LONG, Long::parseLong);
        STRING_CONVERTERS.put(Types.DOUBLE, Double::parseDouble);
        STRING_CONVERTERS.put(Types.BOOLEAN, TypeConverter::parseBoolean);
        STRING_CONVERTERS.put(Types.BIG_DEC, BigDecimal::new);
        STRING_CONVERTERS.put(Types.LOCAL_DATE_TIME, v -> LocalDateTime.parse(v, DateFormatConstants.DEFAULT_TIMESTAMP_FORMAT));

        // 初始化类型兼容性检查器
        TYPE_COMPATIBILITY_CHECKERS.put(Types.STRING, v -> v instanceof String);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.INT, v -> v instanceof Integer);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.LONG, v -> v instanceof Long);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.DOUBLE, v -> v instanceof Double);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.BOOLEAN, v -> v instanceof Boolean);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.BIG_DEC, v -> v instanceof BigDecimal);
        TYPE_COMPATIBILITY_CHECKERS.put(Types.LOCAL_DATE_TIME, v -> v instanceof LocalDateTime || v instanceof java.sql.Timestamp);
    }


    /**
     * 将原始值转换为目标类型
     * 用于 JDBC ResultSet 值转换
     *
     * @param value 原始值（通常是 String 或 JDBC 类型）
     * @param fieldName 字段名（用于错误信息）
     * @param targetType 目标类型（Flink TypeInformation）
     * @return 转换后的值
     * @throws TypeConversionException 转换失败时抛出
     */
    public static Object convertFromValue(Object value, String fieldName, TypeInformation<?> targetType) {
        if (value == null) {
            return null;
        }

        if (isCompatibleType(value, targetType)) {
            // Timestamp 虽然被视为 LocalDateTime 的兼容类型，但需要实际转换
            if (targetType == Types.LOCAL_DATE_TIME && value instanceof java.sql.Timestamp) {
                return ((java.sql.Timestamp) value).toLocalDateTime();
            }
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
}