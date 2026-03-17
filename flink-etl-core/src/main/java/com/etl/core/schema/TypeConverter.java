package com.etl.core.schema;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * 类型转换器
 * 将原始值转换为目标类型
 */
public class TypeConverter {

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将原始值转换为目标类型
     *
     * @param value 原始值（通常是 String）
     * @param fieldName 字段名（用于错误信息）
     * @param targetType 目标类型
     * @return 转换后的值
     * @throws TypeConversionException 转换失败时抛出
     */
    public static Object convert(Object value, String fieldName, EtlFieldType targetType) {
        if (value == null) {
            return null;
        }

        // 处理字符串类型：检查空字符串
        if (value instanceof String) {
            String strValue = ((String) value).trim();
            if (strValue.isEmpty()) {
                return null;
            }
        }

        // 如果已经是目标类型或兼容类型，直接返回
        if (isCompatibleType(value, targetType)) {
            return value;
        }

        String strValue = String.valueOf(value).trim();
        if (strValue.isEmpty()) {
            return null;
        }

        try {
            switch (targetType) {
                case STRING:
                    return strValue;
                case BOOLEAN:
                    return parseBoolean(strValue);
                case INT:
                    return Integer.parseInt(strValue);
                case LONG:
                    return Long.parseLong(strValue);
                case DOUBLE:
                    return Double.parseDouble(strValue);
                case DECIMAL:
                    return new BigDecimal(strValue);
                case TIMESTAMP:
                    return LocalDateTime.parse(strValue, DEFAULT_TIMESTAMP_FORMAT);
                case BYTES:
                    return parseBytes(value, strValue);
                default:
                    throw new IllegalArgumentException("不支持的类型: " + targetType);
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new TypeConversionException(fieldName, strValue, targetType, e);
        }
    }

    private static boolean isCompatibleType(Object value, EtlFieldType targetType) {
        switch (targetType) {
            case STRING:
                return value instanceof String;
            case BOOLEAN:
                return value instanceof Boolean;
            case INT:
                return value instanceof Integer;
            case LONG:
                return value instanceof Long;
            case DOUBLE:
                return value instanceof Double;
            case DECIMAL:
                return value instanceof BigDecimal;
            case TIMESTAMP:
                return value instanceof LocalDateTime || value instanceof java.sql.Timestamp;
            case BYTES:
                return value instanceof byte[];
            default:
                return false;
        }
    }

    private static Boolean parseBoolean(String value) {
        // 支持多种布尔值表示
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)) {
            return false;
        }
        throw new NumberFormatException("无法解析为布尔值: " + value);
    }

    private static byte[] parseBytes(Object value, String strValue) {
        // 如果已经是字节数组，直接返回
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        // 字符串转字节数组
        return strValue.getBytes(StandardCharsets.UTF_8);
    }
}