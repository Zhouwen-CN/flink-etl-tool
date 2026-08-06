package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 分片器抽象基类
 * 所有分片策略的实现都继承此类
 */
@Slf4j
public abstract class ChunkSplitter {

    protected final JdbcDialect dialect;
    protected final String url;
    protected final String username;
    protected final String password;
    protected final String table;
    protected final String sql;
    protected final String splitKey;
    protected final int parallelism;


    public ChunkSplitter(JdbcSourceConfig config, int parallelism) {
        this.dialect = config.getDialect();
        this.url = config.getUrl();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.table = config.getTable();
        this.sql = config.getSql();
        this.splitKey = config.getSplitKey();
        this.parallelism = parallelism;
    }

    public static ChunkSplitter create(SplitStrategy strategy,
                                       JdbcSourceConfig config,
                                       int parallelism) {
        switch (strategy) {
            case NUMERIC:
                return new NumericSplitter(config, parallelism);
            case STRING_HASH:
                return new StringHashSplitter(config, parallelism);
            case DATE_RANGE:
                return new DateSplitter(config, parallelism);
            case FULL_TABLE_SCAN:
                return new FullTableScanSplitter(config, parallelism);
            default:
                throw new IllegalArgumentException("未知的分片策略: " + strategy);
        }
    }

    /**
     * 生成分片列表
     *
     * @return 分片列表（可能为空，如空表）
     */
    public abstract List<JdbcSplit> generateSplits();

    /**
     * 构建基础查询 SQL（SELECT * FROM table）
     *
     * @return 基础查询 SQL
     */
    protected String buildBaseQuery() {
        if (table != null) {
            return "SELECT * FROM " + dialect.quoteIdentifier(table);
        } else {
            return "SELECT * FROM (" + sql + ") t";
        }
    }
}