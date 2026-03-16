package com.etl.core.exception;

/**
 * Source 配置异常
 * 当 Source 配置无效或缺失时抛出
 */
public class SourceConfigException extends RuntimeException {

    public SourceConfigException(String message) {
        super(message);
    }

    public SourceConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}