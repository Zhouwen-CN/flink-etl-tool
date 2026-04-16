package com.etl.connector.jdbc.sink.executor;

import com.etl.connector.jdbc.sink.NamedParameterSqlParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 简单批量执行器
 * 用于 CUSTOM 模式，直接执行用户自定义 SQL
 */
@Slf4j
public class SimpleBatchExecutor implements JdbcBatchStatementExecutor {

    private final String sql;
    private transient PreparedStatement statement;

    public SimpleBatchExecutor(String sql) {
        this.sql = sql;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(sql);
        String preparedSql = parsed.getPreparedSql();
        this.statement = connection.prepareStatement(preparedSql);
        log.info("SimpleBatchExecutor 初始化: sql={}", preparedSql);
    }

    @Override
    public void addToBatch(Row record) throws SQLException {
        String[] fieldNames = record.getFieldNames(true).stream()
                .filter(name -> !name.startsWith("__"))
                .toArray(String[]::new);

        for (int i = 0; i < fieldNames.length; i++) {
            statement.setObject(i + 1, record.getField(fieldNames[i]));
        }
        statement.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        statement.executeBatch();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
        }
    }
}
