package com.etl.core.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * Flink 类型转换器
 * 将 EtlSchema 转换为 Flink RowType
 */
public class FlinkTypeConverter {

    /**
     * 将 EtlFieldType 转换为 Flink DataTypes
     */
    public static TypeInformation<?> toTypeInfo(EtlFieldType type) {
        switch (type) {
            case STRING:
                return Types.STRING;
            case BOOLEAN:
                return Types.BOOLEAN;
            case INT:
                return Types.INT;
            case LONG:
                return Types.LONG;
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.BIG_DEC;
            case TIMESTAMP:
                return Types.LOCAL_DATE_TIME;
            default:
                throw new IllegalArgumentException("不支持的类型: " + type);
        }
    }
}