package com.etl.core.dialect;

/**
 * JDBC Sink 写入模式
 */
public enum WriteMode {
    /**
     * 插入模式，直接插入数据
     */
    INSERT,

    /**
     * Upsert 模式，存在则更新，不存在则插入
     */
    UPSERT,

    /**
     * CDC 模式，根据 RowKind 执行 INSERT/UPDATE/DELETE
     */
    CDC
}