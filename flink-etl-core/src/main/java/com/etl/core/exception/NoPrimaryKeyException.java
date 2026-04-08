package com.etl.core.exception;

/**
 * 表无主键异常
 * 用于 JDBC Source 自动推断 splitKey 和 JDBC Sink UPSERT 模式
 */
public class NoPrimaryKeyException extends RuntimeException {

    private final String tableName;

    public NoPrimaryKeyException(String tableName) {
        super(String.format("表 '%s' 没有主键", tableName));
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}