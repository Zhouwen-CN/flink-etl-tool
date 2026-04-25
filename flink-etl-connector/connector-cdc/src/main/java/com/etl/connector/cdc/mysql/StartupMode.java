package com.etl.connector.cdc.mysql;

/**
 * MySQL CDC 启动模式
 */
public enum StartupMode {
    /**
     * 不进行快照，从 binlog 最早的位置开始
     */
    EARLIEST,

    /**
     * 不进行快照，从 binlog 最新位置开始
     */
    LATEST,

    /**
     * 先读取全量快照，再从 binlog 最新的位置开始
     */
    INITIAL;

    public static StartupMode of(String value){
        for (StartupMode startupMode : values()) {
            if (startupMode.name().equalsIgnoreCase(value)) {
                return startupMode;
            }
        }

        throw new IllegalArgumentException(
            String.format("不支持的启动模式: '%s'。支持的模式: EARLIEST（binlog 最早位置）、LATEST（binlog 最新位置）、INITIAL（先全量后增量）",
                value)
        );
    }
}
