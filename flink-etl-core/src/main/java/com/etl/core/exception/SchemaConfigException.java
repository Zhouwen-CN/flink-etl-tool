package com.etl.core.exception;

/**
 * Schema 配置异常
 * 当 Schema 配置格式错误时抛出
 */
public class SchemaConfigException extends RuntimeException {

    public SchemaConfigException(String message) {
        super("Schema 配置错误: " + message);
    }
}