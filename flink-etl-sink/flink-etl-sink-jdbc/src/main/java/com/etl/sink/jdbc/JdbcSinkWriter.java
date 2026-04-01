package com.etl.sink.jdbc;

import com.etl.core.dialect.WriteMode;
import com.etl.core.sink.AbstractSinkWriter;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
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

    private final JdbcSinkConfig config;
    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient String[] columns;
    private SinkWriterMetricGroup metricGroup;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
        super(context, config, config.getBatchSize());
        this.config = config;
    }

    @Override
    protected void open() throws IOException {
        try {
            connection = DriverManager.getConnection(
                config.getUrl(),
                config.getUsername(),
                config.getPassword()
            );
            connection.setAutoCommit(false);

            // 注册指标
            metricGroup = getMetricGroup();

            log.info("JDBC Sink 已连接: url={}, subtaskId={}", config.getUrl(), getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to initialize JDBC connection", e);
        }
    }

    @Override
    protected void writeRow(Row row) throws IOException {
        try {
            // 懒初始化 statement（第一次调用时根据 Row 字段生成 SQL）
            if (statement == null) {
                initStatement(row);
            }

            // 填充参数
            for (int i = 0; i < columns.length; i++) {
                statement.setObject(i + 1, row.getField(columns[i]));
            }

            // 添加到 JDBC 批量缓冲
            statement.addBatch();
        } catch (SQLException e) {
            throw new IOException("Failed to write row", e);
        }
    }

    private void initStatement(Row row) throws SQLException {
        Set<String> fieldNames = row.getFieldNames(true);
        this.columns = fieldNames.toArray(new String[0]);

        String sql;
        if (config.getSql() != null) {
            // sql 模式：解析具名占位符
            NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(config.getSql());
            sql = parsed.getPreparedSql();
            log.info("JDBC Sink sql 模式: {}", sql);
        } else {
            // table 模式：根据 mode 生成 SQL
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
    protected void flushBatch() throws IOException {
        try {
            int[] results = statement.executeBatch();
            connection.commit();

            log.debug("已写入 {} 条记录, subtaskId={}", results.length, getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to flush batch", e);
        }
    }

    @Override
    protected void handleFlushFailure(IOException e) {
        try {
            if (connection != null) {
                connection.rollback();
                log.warn("Flush 失败，已回滚事务");
            }
        } catch (SQLException rollbackEx) {
            log.error("回滚失败", rollbackEx);
        }
    }

    @Override
    protected void cleanup() throws IOException {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
            log.info("JDBC Sink 资源清理完成, subtaskId={}", getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to cleanup JDBC resources", e);
        }
    }
}