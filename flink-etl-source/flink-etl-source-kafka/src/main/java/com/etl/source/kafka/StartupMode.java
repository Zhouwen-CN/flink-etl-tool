package com.etl.source.kafka;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Kafka Source 启动模式枚举
 */
public enum StartupMode {
    /** 从最早的记录开始消费 */
    EARLIEST("earliest") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.earliest();
        }
    },
    /** 从最新的记录开始消费 */
    LATEST("latest") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.latest();
        }
    },
    /** 从已提交的 offset 开始消费 */
    COMMITTED("committed") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.committedOffsets();
        }
    };

    private final String configValue;

    StartupMode(String configValue) {
        this.configValue = configValue;
    }

    /**
     * 获取配置值
     */
    public String getConfigValue() {
        return configValue;
    }

    /**
     * 转换为 Flink OffsetsInitializer
     */
    public abstract OffsetsInitializer toOffsetsInitializer();

    /**
     * 从配置字符串解析启动模式
     *
     * @param value 配置值（不区分大小写）
     * @return 对应的启动模式，如果未找到则返回 EARLIEST
     */
    public static StartupMode fromConfigValue(String value) {
        if (value == null) {
            return EARLIEST;
        }
        for (StartupMode mode : values()) {
            if (mode.configValue.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        return EARLIEST;
    }

    /**
     * 校验配置值是否有效
     *
     * @param value 配置值
     * @return 是否有效
     */
    public static boolean isValid(String value) {
        if (value == null) {
            return false;
        }
        for (StartupMode mode : values()) {
            if (mode.configValue.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }
}