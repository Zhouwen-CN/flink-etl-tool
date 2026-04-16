package com.etl.connector.jdbc.sink.executor;

import com.etl.connector.jdbc.sink.NamedParameterSqlParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * 简单批量执行器
 * 用于 CUSTOM 模式，直接执行用户自定义 SQL
 */
@Slf4j
public class SimpleBatchExecutor implements JdbcBatchStatementExecutor {

    private final String sql;
    private final List<String> columns;
    private transient PreparedStatement statement;

    public SimpleBatchExecutor(String sql) {
        NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(sql);
        this.sql = parsed.getPreparedSql();
        this.columns = parsed.getParamNames();
        log.info("SimpleBatchExecutor 初始化: sql={}", sql);
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.statement = connection.prepareStatement(sql);
    }

    @Override
    public void addToBatch(Row record) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            statement.setObject(i + 1, record.getField(column));
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
