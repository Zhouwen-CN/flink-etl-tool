package com.etl.core.schema;

/**
 * ETL 字段类型枚举
 * 定义支持的 8 种基础类型
 */
public enum EtlFieldType {
    STRING,
    BOOLEAN,
    INT,
    LONG,
    DOUBLE,
    DECIMAL,
    TIMESTAMP;

    /**
     * 从字符串解析类型（大小写不敏感）
     *
     * @param typeName 类型名称
     * @return 对应的枚举值，无效时返回 null
     */
    public static EtlFieldType fromString(String typeName) {
        try {
            return EtlFieldType.valueOf(typeName.toUpperCase());
        } catch (IllegalArgumentException e) {
            return STRING;
        }
    }
}