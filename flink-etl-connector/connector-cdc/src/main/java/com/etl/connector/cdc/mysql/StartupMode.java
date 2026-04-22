package com.etl.connector.cdc.mysql;

/**
 * MySQL CDC 启动模式
 */
public enum StartupMode {
    /**
     * 从最早可用位置开始（读取历史变更）
     */
    EARLIEST,

    /**
     * 从最新位置开始（只捕获新变更）
     */
    LATEST,

    /**
     * 从指定时间戳开始
     */
    TIMESTAMP,

    /**
     * 先读取全量快照，再捕获增量变更
     */
    SNAPSHOT_FIRST;

    static StartupMode of(String value){
        for (StartupMode startupMode : values()) {
            if (startupMode.name().equalsIgnoreCase(value)) {
                return startupMode;
            }
        }

        throw new IllegalArgumentException(
            String.format("不支持的启动模式: '%s'。支持的模式: EARLIEST（最早位置）、LATEST（最新位置）、TIMESTAMP（指定时间戳）、SNAPSHOT_FIRST（先全量后增量）",
                value)
        );
    }
}
