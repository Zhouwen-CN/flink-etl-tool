package com.etl.core.schema;

import lombok.Getter;

/**
 * 类型转换异常
 * 当值无法转换为目标类型时抛出
 */
@Getter
public class TypeConversionException extends RuntimeException {

    private final String fieldName;
    private final String rawValue;
    private final EtlFieldType targetType;

    public TypeConversionException(String fieldName, String rawValue,
                                   EtlFieldType targetType, Throwable cause) {
        super(String.format("字段 '%s' 类型转换失败: 值 '%s' 无法转换为 %s",
              fieldName, rawValue, targetType), cause);
        this.fieldName = fieldName;
        this.rawValue = rawValue;
        this.targetType = targetType;
    }
}