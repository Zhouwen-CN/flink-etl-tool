package com.etl.core.config;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

/**
 * Job 执行模式枚举
 */
public enum ExecutionMode {
    BATCH("batch"),
    STREAM("stream");

    private final String value;

    ExecutionMode(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static ExecutionMode fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (ExecutionMode mode : ExecutionMode.values()) {
            if (mode.value.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("未知的执行模式: " + value + "，仅支持 batch | stream");
    }
}