package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.dialect.WriteMode;
import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import com.etl.connector.jdbc.sink.executor.BufferReducedExecutor;
import com.etl.connector.jdbc.sink.executor.JdbcBatchStatementExecutor;
import com.etl.connector.jdbc.sink.executor.SimpleBatchExecutor;
import com.etl.connector.jdbc.sink.executor.SimpleBufferedExecutor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;

/**
 * JdbcOutputFormat 构建器
 * 根据写入模式创建对应的 Executor
 */
@Slf4j
public class JdbcOutputFormatBuilder {

    private final JdbcSinkConfig config;
    private final Connection connection;
    private transient String[] columns;

    public JdbcOutputFormatBuilder(JdbcSinkConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }

    /**
     * 构建 OutputFormat
     */
    public JdbcOutputFormat<org.apache.flink.types.Row> build() {
        JdbcBatchStatementExecutor executor = createExecutor(config);

        int batchSize = config.getBatchSize() != null ? config.getBatchSize() : 100;
        long batchIntervalMs = config.getBatchIntervalMs() != null ? config.getBatchIntervalMs() : 0L;
        int maxRetries = 3;  // 默认重试 3 次

        return new JdbcOutputFormat<>(executor, connection, batchSize, batchIntervalMs, maxRetries);
    }

    /**
     * 根据模式创建 Executor
     */
    private JdbcBatchStatementExecutor createExecutor(JdbcSinkConfig config) {
        switch (config.getMode()) {
            case INSERT:
                String insertSql = config.getDialect().getInsertSql(config.getTable(), getColumns());
                log.info("INSERT 模式: table={}, sql={}", config.getTable(), insertSql);
                return new SimpleBufferedExecutor(insertSql, getColumns());

            case UPSERT:
                log.info("UPSERT 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
                return new BufferReducedExecutor(
                        config.getDialect(),
                        config.getTable(),
                        getColumns(),
                        config.getKeyFields(),
                        false  // 非 CDC 模式，不跳过 UPDATE_BEFORE
                );

            case CDC:
                log.info("CDC 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
                return new BufferReducedExecutor(
                        config.getDialect(),
                        config.getTable(),
                        getColumns(),
                        config.getKeyFields(),
                        true   // CDC 模式，跳过 UPDATE_BEFORE
                );

            case CUSTOM:
                log.info("CUSTOM 模式: sql={}", config.getSql());
                return new SimpleBatchExecutor(config.getSql());

            default:
                throw new IllegalArgumentException("不支持的写入模式: " + config.getMode());
        }
    }

    /**
     * 获取列名数组（缓存）
     */
    private String[] getColumns() {
        if (columns == null) {
            // 从第一个字段推断列名（实际会在 Writer 首次写入时更新）
            columns = new String[0];
        }
        return columns;
    }

    /**
     * 更新列名（Writer 首次写入时调用）
     */
    public void updateColumns(String[] columns) {
        this.columns = columns;
    }
}
