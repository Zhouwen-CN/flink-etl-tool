package com.etl.core.schema;

import java.util.Arrays;

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
     * @return 对应的枚举值
     * @throws IllegalArgumentException 当类型名称无效时抛出
     */
    public static EtlFieldType fromString(String typeName) {
        try {
            return EtlFieldType.valueOf(typeName.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("不支持的字段类型: " + typeName
                    + "，支持的类型有: " + Arrays.toString(EtlFieldType.values()));
        }
    }
}