package com.etl.core.config;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

/**
 * Job 执行模式枚举
 */
public enum ExecutionMode {
    BATCH("batch") {
        @Override
        public void configure(Configuration configuration) {
            configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        }
    },
    STREAM("streaming") {
        @Override
        public void configure(Configuration configuration) {
            configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        }
    };

    private final String value;

    ExecutionMode(String value) {
        this.value = value;
    }

    public abstract void configure(Configuration configuration);

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