package com.etl.sink.jdbc;

import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.WriteMode;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * JDBC Sink Function
 * 支持 table 模式（自动生成 INSERT）和 sql 模式（具名占位符）
 */
@Slf4j
public class JdbcSinkFunction extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;

    private final JdbcSinkConfig config;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient int pendingCount;

    // table 模式
    private transient String[] columns;

    // sql 模式
    private transient List<String> paramNames;

    public JdbcSinkFunction(JdbcSinkConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
        connection.setAutoCommit(false);
        pendingCount = 0;
        log.info("JDBC Sink 已连接: url={}", config.getUrl());
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        if (statement == null) {
            initStatement(row);
        }

        if (config.getTable() != null) {
            // table 模式：按列顺序填充
            for (int i = 0; i < columns.length; i++) {
                statement.setObject(i + 1, row.getField(columns[i]));
            }
        } else {
            // sql 模式：按参数名严格匹配
            Set<String> fieldNames = row.getFieldNames(true);
            for (int i = 0; i < paramNames.size(); i++) {
                String paramName = paramNames.get(i);
                if (!fieldNames.contains(paramName)) {
                    throw new IllegalArgumentException("Row 中不存在字段: " + paramName);
                }
                statement.setObject(i + 1, row.getField(paramName));
            }
        }

        statement.addBatch();
        pendingCount++;

        if (pendingCount >= config.getBatchSize()) {
            flush();
        }
    }

    private void initStatement(Row row) throws SQLException {
        // table 优先
        if (config.getTable() != null) {
            // table 模式：从 Row 字段名生成 SQL
            Set<String> fieldNames = row.getFieldNames(true);
            if (fieldNames == null || fieldNames.isEmpty()) {
                throw new IllegalStateException("Row 没有字段名信息，请使用 Row.withNames()");
            }
            columns = fieldNames.toArray(new String[0]);

            String sql;
            JdbcDialect dialect = config.getDialect();
            if (config.getMode() == WriteMode.UPSERT) {
                // Upsert 模式
                sql = dialect.getUpsertSql(config.getTable(), columns, config.getKeyFields());
                log.info("JDBC Sink upsert 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
            } else {
                // Insert 模式
                sql = dialect.getInsertSql(config.getTable(), columns);
                log.info("JDBC Sink insert 模式: table={}, columns={}", config.getTable(), Arrays.toString(columns));
            }
            statement = connection.prepareStatement(sql);
        } else {
            // sql 模式：解析具名占位符
            NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(config.getSql());
            paramNames = parsed.getParamNames();
            statement = connection.prepareStatement(parsed.getPreparedSql());
            log.info("JDBC Sink sql 模式: params={}", paramNames);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (statement != null && pendingCount > 0) {
                flush();
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void flush() throws SQLException {
        try {
            statement.executeBatch();
            connection.commit();
            log.debug("已写入 {} 条记录", pendingCount);
        } catch (SQLException e) {
            // 执行失败时回滚事务
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                log.warn("事务回滚失败", rollbackEx);
            }
            // 清除 statement 中的残留 batch 数据，重新创建 statement
            try {
                statement.close();
            } catch (SQLException closeEx) {
                log.warn("关闭 Statement 失败", closeEx);
            }
            statement = null;
            throw e;
        } finally {
            pendingCount = 0;
        }
    }
}