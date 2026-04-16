package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.sink.executor.JdbcBatchStatementExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC 输出格式
 * 管理批量刷写逻辑，双重检查（batch_size + batch_interval_ms）
 */
@Slf4j
public class JdbcOutputFormat<I> {

    private final JdbcBatchStatementExecutor executor;
    private final Connection connection;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;

    private transient int batchCount = 0;
    private transient long lastFlushTimeMs;

    public JdbcOutputFormat(
            JdbcBatchStatementExecutor executor,
            Connection connection,
            int batchSize,
            long batchIntervalMs,
            int maxRetries) {
        this.executor = executor;
        this.connection = connection;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
    }

    /**
     * 初始化 Executor
     */
    public void open() throws SQLException {
        executor.prepareStatements(connection);
        this.lastFlushTimeMs = System.currentTimeMillis();
    }

    /**
     * 写入记录
     */
    public void writeRecord(I record) throws SQLException, IOException, InterruptedException {
        executor.addToBatch((Row) record);
        batchCount++;

        // 双重检查：数量 或 时间
        if (batchCount > 0 && (isOverBatchSize() || isOverInterval())) {
            flush();
        }
    }

    /**
     * 刷写数据
     */
    public synchronized void flush() throws IOException, InterruptedException {
        if (batchCount == 0) {
            return;
        }

        for (int retry = 0; retry <= maxRetries; retry++) {
            try {
                executor.executeBatch();
                connection.commit();
                batchCount = 0;
                lastFlushTimeMs = System.currentTimeMillis();
                log.debug("Flush 成功: subtaskId={}", Thread.currentThread().getId());
                break;
            } catch (SQLException e) {
                if (retry >= maxRetries) {
                    // 重试次数用尽，回滚事务
                    try {
                        connection.rollback();
                        log.warn("Flush 失败，已回滚事务");
                    } catch (SQLException rollbackEx) {
                        log.error("回滚失败", rollbackEx);
                    }
                    throw new IOException("Flush failed after " + maxRetries + " retries", e);
                }

                // 等待后重试
                long sleepMs = 1000L * retry;
                log.warn("Flush 失败，等待 {}ms 后重试 (retry={})", sleepMs, retry);
                Thread.sleep(sleepMs);
            }
        }
    }

    /**
     * 关闭资源
     */
    public void close() throws SQLException, IOException, InterruptedException {
        // 提交剩余数据
        flush();

        // 关闭 Executor
        executor.closeStatements();
    }

    private boolean isOverBatchSize() {
        return batchSize > 0 && batchCount >= batchSize;
    }

    private boolean isOverInterval() {
        return batchIntervalMs > 0
                && (System.currentTimeMillis() - lastFlushTimeMs) >= batchIntervalMs;
    }
}
