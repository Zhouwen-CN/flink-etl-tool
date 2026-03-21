package com.etl.sink.jdbc.config;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/**
 * JDBC Sink 配置
 */
@Getter
@Builder
public class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 数据库连接 URL */
    private final String url;
    /** 用户名 */
    private final String username;
    /** 密码 */
    private final String password;
    /** 目标表名（与 sql 二选一，优先） */
    private final String table;
    /** 自定义 SQL，支持具名占位符 :paramName */
    private final String sql;
    /** 批量写入大小，默认 100 */
    private final Integer batchSize;
}