package com.etl.sink.jdbc;

import com.etl.core.dialect.WriteMode;
import com.etl.core.sink.AbstractSinkWriter;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;

/**
 * JDBC Sink Writer 实现
 * 管理数据库连接和批量写入
 */
@Slf4j
public class JdbcSinkWriter extends AbstractSinkWriter<JdbcSinkConfig> {

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient String[] columns;

    /** 批量大小 */
    private final int batchSize;

    /** 待写入数据计数 */
    private int pendingCount = 0;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
        super(context, config);
        this.batchSize = config.getBatchSize();

        // 直接初始化数据库连接（不再延迟初始化）
        try {
            connection = DriverManager.getConnection(
                config.getUrl(),
                config.getUsername(),
                config.getPassword()
            );
            connection.setAutoCommit(false);
            log.info("JDBC Sink 已连接: url={}, subtaskId={}", config.getUrl(), context.getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to initialize JDBC connection", e);
        }
    }

    @Override
    public void write(Row row, Context context) throws IOException, InterruptedException {
        try {
            if (statement == null) {
                initStatement(row);
            }

            for (int i = 0; i < columns.length; i++) {
                statement.setObject(i + 1, row.getField(columns[i]));
            }

            statement.addBatch();
            pendingCount++;

            // 达到批量大小时自动 flush
            if (pendingCount >= batchSize) {
                flush(false);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to write row", e);
        }
    }

    private void initStatement(Row row) throws SQLException {
        Set<String> fieldNames = row.getFieldNames(true);
        this.columns = fieldNames.toArray(new String[0]);

        String sql;
        if (config.getSql() != null) {
            NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(config.getSql());
            sql = parsed.getPreparedSql();
            log.info("JDBC Sink sql 模式: {}", sql);
        } else {
            if (config.getMode() == WriteMode.UPSERT) {
                sql = config.getDialect().getUpsertSql(config.getTable(), columns, config.getKeyFields());
                log.info("JDBC Sink upsert 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
            } else {
                sql = config.getDialect().getInsertSql(config.getTable(), columns);
                log.info("JDBC Sink insert 模式: table={}, columns={}", config.getTable(), Arrays.toString(columns));
            }
        }

        this.statement = connection.prepareStatement(sql);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (pendingCount == 0) {
            return;
        }

        try {
            int[] results = statement.executeBatch();
            connection.commit();
            pendingCount = 0;

            log.debug("已写入 {} 条记录, subtaskId={}", results.length, this.context.getSubtaskId());
        } catch (SQLException e) {
            // 回滚事务
            try {
                if (connection != null) {
                    connection.rollback();
                    log.warn("Flush 失败，已回滚事务");
                }
            } catch (SQLException rollbackEx) {
                log.error("回滚失败", rollbackEx);
            }
            throw new IOException("Failed to flush batch", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // 提交剩余数据
            flush(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while flushing during close", e);
        } finally {
            // 清理资源
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
                log.info("JDBC Sink 资源清理完成, subtaskId={}", context.getSubtaskId());
            } catch (SQLException e) {
                throw new IOException("Failed to cleanup JDBC resources", e);
            }
        }
    }
}