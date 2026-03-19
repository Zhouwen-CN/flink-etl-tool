package com.etl.source.jdbc;

import lombok.Builder;
import lombok.Getter;

/**
 * JDBC 分片配置
 * 用于传递分片所需的所有参数到 JdbcSplitEnumerator
 */
@Getter
@Builder
public class JdbcSplitConfig {
    /** 数据库连接 URL */
    private final String url;
    /** 用户名 */
    private final String username;
    /** 密码 */
    private final String password;
    /** 表名 */
    private final String table;
    /** 自定义 SQL */
    private final String sql;
    /** 分片列名 */
    private final String splitColumn;
    /** 数据库方言 */
    private final JdbcDialect dialect;
}