package com.etl.core.exception;

import lombok.Getter;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * 类型转换异常
 * 当值无法转换为目标类型时抛出
 */
@Getter
public class TypeConversionException extends RuntimeException {

    private final String fieldName;
    private final String rawValue;
    private final TypeInformation<?> targetType;

    public TypeConversionException(String fieldName, String rawValue,
                                   TypeInformation<?> targetType, Throwable cause) {
        super(String.format("字段 '%s' 类型转换失败: 值 '%s' 无法转换为 %s",
              fieldName, rawValue, targetType), cause);
        this.fieldName = fieldName;
        this.rawValue = rawValue;
        this.targetType = targetType;
    }
}