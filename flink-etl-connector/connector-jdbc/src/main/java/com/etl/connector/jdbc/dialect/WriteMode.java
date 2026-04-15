package com.etl.connector.jdbc.dialect;

/**
 * JDBC Sink 写入模式
 */
public enum WriteMode {
    /**
     * 插入模式，自动生成 INSERT SQL
     * 必须配置 table，忽略 sql 和 keyFields
     */
    INSERT,

    /**
     * Upsert 模式，存在则更新，不存在则插入
     * 必须配置 table，忽略 sql
     * keyFields 可选（自动获取主键）
     */
    UPSERT,

    /**
     * CDC 模式，根据 RowKind 执行 INSERT/UPDATE/DELETE
     * 必须配置 table，忽略 sql
     * keyFields 可选（自动获取主键）
     */
    CDC,

    /**
     * 自定义模式，执行用户自定义 SQL
     * 必须配置 sql，忽略 table 和 keyFields
     */
    CUSTOM
}