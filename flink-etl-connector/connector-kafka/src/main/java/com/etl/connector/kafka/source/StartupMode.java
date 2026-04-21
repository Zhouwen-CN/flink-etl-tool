package com.etl.connector.kafka.source;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Kafka Source 启动模式枚举
 */
public enum StartupMode {
    /** 从最早位点开始消费 */
    EARLIEST("earliest") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.earliest();
        }
    },
    /** 从最末尾位点开始消费 */
    LATEST("latest") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.latest();
        }
    },
    /** 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点 */
    COMMITTED("committed") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        }
    };

    private final String configValue;

    StartupMode(String configValue) {
        this.configValue = configValue;
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
        for (StartupMode mode : values()) {
            if (mode.configValue.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "startupMode 必须是 earliest、latest 或 committed，当前值: " + value);
    }
}