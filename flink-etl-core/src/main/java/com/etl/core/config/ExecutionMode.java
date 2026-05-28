package com.etl.core.config;


import lombok.Getter;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

/**
 * Job 执行模式枚举
 */
public enum ExecutionMode {
    BATCH("batch", RuntimeExecutionMode.BATCH),
    STREAM("streaming", RuntimeExecutionMode.STREAMING);

    private final String value;
    @Getter
    private final RuntimeExecutionMode runtimeMode;

    ExecutionMode(String value, RuntimeExecutionMode runtimeMode) {
        this.value = value;
        this.runtimeMode = runtimeMode;
    }

    @JsonCreator
    public static ExecutionMode fromValue(String value) {
        for (ExecutionMode mode : ExecutionMode.values()) {
            if (mode.value.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("未知的执行模式: " + value + "，仅支持 batch | stream");
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}