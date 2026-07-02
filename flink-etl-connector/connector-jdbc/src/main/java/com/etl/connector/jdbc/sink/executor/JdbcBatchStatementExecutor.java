package com.etl.connector.jdbc.sink.executor;

import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC 批量执行器接口
 * 不同写入模式有不同的实现策略
 */
public interface JdbcBatchStatementExecutor {

    /**
     * 初始化 PreparedStatement
     * @param connection 数据库连接
     * @throws SQLException SQL 异常
     */
    void prepareStatements(Connection connection) throws SQLException;

    /**
     * 添加数据到批次
     * @param record 数据行
     * @throws SQLException SQL 异常
     */
    void addToBatch(Row record) throws SQLException;

    /**
     * 执行当前批次
     * @throws SQLException SQL 异常
     */
    void executeBatch() throws SQLException;

    /**
     * 清空内部缓冲（不清空 PreparedStatement 的批处理队列）
     * 用于重试前重置内部状态，防止重复数据
     */
    default void clearBatch() {}

    /**
     * 关闭 Statement
     * @throws SQLException SQL 异常
     */
    void closeStatements() throws SQLException;
}
