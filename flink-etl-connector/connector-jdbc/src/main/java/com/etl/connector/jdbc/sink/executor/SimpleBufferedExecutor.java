package com.etl.connector.jdbc.sink.executor;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 简单缓冲执行器
 * 用于 INSERT 模式，缓冲数据后批量执行
 */
@Slf4j
public class SimpleBufferedExecutor implements JdbcBatchStatementExecutor {

    private final String sql;
    private final String[] columns;
    private transient PreparedStatement statement;

    public SimpleBufferedExecutor(String sql, String[] columns) {
        this.sql = sql;
        this.columns = columns;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.statement = connection.prepareStatement(sql);
        log.info("SimpleBufferedExecutor 初始化: sql={}", sql);
    }

    @Override
    public void addToBatch(Row record) throws SQLException {
        try {
            // 优先尝试 name-based 访问（兼容 Row.withNames() 创建的命名 Row）
            for (int i = 0; i < columns.length; i++) {
                statement.setObject(i + 1, record.getField(columns[i]));
            }
        } catch (IllegalArgumentException e) {
            // 回退到 position-based 访问（兼容 Row.of() 创建的位置 Row）
            for (int i = 0; i < columns.length; i++) {
                statement.setObject(i + 1, record.getField(i));
            }
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
