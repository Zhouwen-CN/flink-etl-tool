package com.etl.connector.jdbc.sink.executor;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 简单缓冲执行器
 * 用于 INSERT 模式，缓冲数据后批量执行
 *
 * 参考 Flink 的 SimpleBatchStatementExecutor：维护自己的 List 缓冲，
 * addToBatch 只入缓冲，executeBatch 遍历缓冲填充 PreparedStatement 后执行，
 * 仅在成功后清空缓冲，失败时缓冲保留供 JdbcOutputFormat 重试。
 */
@Slf4j
public class SimpleBufferedExecutor implements JdbcBatchStatementExecutor {

    private final String sql;
    private final String[] columns;
    private transient PreparedStatement statement;
    private transient List<Row> batch;

    public SimpleBufferedExecutor(String sql, String[] columns) {
        this.sql = sql;
        this.columns = columns;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.statement = connection.prepareStatement(sql);
        this.batch = new ArrayList<>();
        log.info("SimpleBufferedExecutor 初始化: sql={}", sql);
    }

    @Override
    public void addToBatch(Row record) throws SQLException {
        batch.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        if (batch.isEmpty()) {
            return;
        }
        for (Row record : batch) {
            setParameters(record);
            statement.addBatch();
        }
        statement.executeBatch();
        batch.clear();
    }

    @Override
    public void clearBatch() {
        batch.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
        }
    }

    private void setParameters(Row record) throws SQLException {
        try {
            for (int i = 0; i < columns.length; i++) {
                statement.setObject(i + 1, record.getField(columns[i]));
            }
        } catch (IllegalArgumentException e) {
            for (int i = 0; i < columns.length; i++) {
                statement.setObject(i + 1, record.getField(i));
            }
        }
    }
}
