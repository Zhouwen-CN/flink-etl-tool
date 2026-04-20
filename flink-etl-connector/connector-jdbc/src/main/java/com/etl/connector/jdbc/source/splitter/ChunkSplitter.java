package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.source.RangeSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 分片器抽象基类
 * 所有分片策略的实现都继承此类
 */
@Slf4j
public abstract class ChunkSplitter {

    protected final JdbcSourceConfig config;
    protected final int parallelism;

    public ChunkSplitter(JdbcSourceConfig config, int parallelism) {
        this.config = config;
        this.parallelism = parallelism;
    }

    /**
     * 生成分片列表
     *
     * @return 分片列表（可能为空，如空表）
     */
    public abstract List<RangeSplit> generateSplits();

    /**
     * 构建基础查询 SQL（SELECT * FROM table）
     *
     * @return 基础查询 SQL
     */
    protected String buildBaseQuery() {
        String table = config.getTable();
        String sql = config.getSql();
        com.etl.connector.jdbc.dialect.JdbcDialect dialect = config.getDialect();

        if (table != null) {
            return "SELECT * FROM " + dialect.quoteIdentifier(table);
        } else {
            return "SELECT * FROM (" + sql + ") AS t";
        }
    }
}