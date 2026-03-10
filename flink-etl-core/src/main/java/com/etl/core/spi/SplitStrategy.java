package com.etl.core.spi;

/**
 * 分片策略枚举
 * 定义不同数据源支持的分片方式
 */
public enum SplitStrategy {
    /**
     * 不支持分片（如控制台）
     */
    NONE,

    /**
     * 范围分片（MySQL 主键）
     */
    RANGE,

    /**
     * 哈希分片
     */
    HASH,

    /**
     * 基于文件的分片
     */
    FILE_BASED
}