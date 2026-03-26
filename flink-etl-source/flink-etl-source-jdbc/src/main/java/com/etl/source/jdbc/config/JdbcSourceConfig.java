package com.etl.source.jdbc.config;

import com.etl.source.jdbc.enums.SplitStrategy;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/**
 * JDBC Source 配置
 */
@Getter
@Builder
public class JdbcSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

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
    /** 分片列名（可选），不配置则使用单分片全表扫描模式 */
    private final String splitColumn;
    /** 分片策略，根据 splitColumn 是否配置决定 */
    private final SplitStrategy splitStrategy;
    /** 批大小，默认100 */
    private final Integer batchSize;
    /** 查询超时 */
    private final Integer queryTimeout;
}