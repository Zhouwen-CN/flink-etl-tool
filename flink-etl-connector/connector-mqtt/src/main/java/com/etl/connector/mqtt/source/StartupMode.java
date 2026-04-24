package com.etl.connector.mqtt.source;

/**
 * MQTT Source 启动模式枚举
 */
public enum StartupMode {
    /** 从 broker 保留的最早消息开始（retained message） */
    EARLIEST("earliest"),
    /** 从最新消息开始（订阅后发布的新消息） */
    LATEST("latest");

    private final String configValue;

    StartupMode(String configValue) {
        this.configValue = configValue;
    }

    /**
     * 从配置字符串解析启动模式
     *
     * @param value 配置值（不区分大小写）
     * @return 对应的启动模式
     */
    public static StartupMode fromConfigValue(String value) {
        for (StartupMode mode : values()) {
            if (mode.configValue.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "startupMode 必须是 earliest 或 latest，当前值: " + value);
    }
}