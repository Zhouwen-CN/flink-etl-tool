package com.etl.connector.jdbc.sink;

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
    private final transient String[] columns;

    public JdbcOutputFormatBuilder(JdbcSinkConfig config, Connection connection, String[] columns) {
        this.config = config;
        this.connection = connection;
        this.columns = columns;
    }

    /**
     * 构建 OutputFormat
     */
    public JdbcOutputFormat build() {
        JdbcBatchStatementExecutor executor = createExecutor(config);

        int batchSize = config.getBatchSize();
        long batchIntervalMs = config.getBatchIntervalMs();
        int maxRetries = 3;  // 默认重试 3 次

        return new JdbcOutputFormat(executor, connection, batchSize, batchIntervalMs, maxRetries);
    }

    /**
     * 根据模式创建 Executor
     */
    private JdbcBatchStatementExecutor createExecutor(JdbcSinkConfig config) {
        switch (config.getMode()) {
            case INSERT:
                String insertSql = config.getDialect().getInsertSql(config.getTable(), columns);
                log.info("INSERT 模式: table={}, sql={}", config.getTable(), insertSql);
                return new SimpleBufferedExecutor(insertSql, columns);

            case UPSERT:
                log.info("UPSERT 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
                return new BufferReducedExecutor(
                        config.getDialect(),
                        config.getTable(),
                        columns,
                        config.getKeyFields()
                );

            case CDC:
                log.info("CDC 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
                return new BufferReducedExecutor(
                        config.getDialect(),
                        config.getTable(),
                        columns,
                        config.getKeyFields()
                );

            case CUSTOM:
                log.info("CUSTOM 模式: sql={}", config.getSql());
                return new SimpleBatchExecutor(config.getSql());

            default:
                throw new IllegalArgumentException("不支持的写入模式: " + config.getMode());
        }
    }
}
