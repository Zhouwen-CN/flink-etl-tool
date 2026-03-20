package com.etl.core.schema;

import com.etl.core.exception.TypeConversionException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * 类型转换器
 * 将原始值转换为目标类型（基于 Flink TypeInformation）
 */
public class TypeConverter {

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将原始值转换为目标类型
     *
     * @param value 原始值（通常是 String）
     * @param fieldName 字段名（用于错误信息）
     * @param targetType 目标类型（Flink TypeInformation）
     * @return 转换后的值
     * @throws TypeConversionException 转换失败时抛出
     */
    public static Object convert(Object value, String fieldName, TypeInformation<?> targetType) {
        if (value == null) {
            return null;
        }

        // 如果已经是目标类型或兼容类型，直接返回
        if (isCompatibleType(value, targetType)) {
            return value;
        }

        String strValue = String.valueOf(value);
        if (strValue.isEmpty()) {
            return null;
        }

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
}