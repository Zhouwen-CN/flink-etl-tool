package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.sink.executor.JdbcBatchStatementExecutor;
import com.etl.core.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC 输出格式
 * 管理批量刷写逻辑，双重检查（batch_size + batch_interval_ms）
 *
 * 全权管理 Connection 生命周期：open 时建连，flush 重试失败时自动重建连接
 * （断连自愈），close 时释放。executor 内部缓冲（待重试数据）在重连后保留。
 */
@Slf4j
public class JdbcOutputFormat implements AutoCloseable {

    private final JdbcBatchStatementExecutor executor;
    private final String url;
    private final String username;
    private final String password;
    private final String driverClass;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;

    private transient Connection connection;
    private transient int batchCount = 0;
    private transient long lastFlushTimeMs;

    public JdbcOutputFormat(
            JdbcBatchStatementExecutor executor,
            String url,
            String username,
            String password,
            String driverClass,
            int batchSize,
            long batchIntervalMs,
            int maxRetries) {
        this.executor = executor;
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverClass = driverClass;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
    }

    /**
     * 建立连接并初始化 Executor
     */
    public void open() throws SQLException {
        this.connection = createConnection();
        executor.prepareStatements(connection);
        this.lastFlushTimeMs = System.currentTimeMillis();
        log.info("JDBC 连接已建立: url={}", url);
    }

    /**
     * 写入记录
     */
    public void writeRecord(Row record) throws SQLException, IOException, InterruptedException {
        executor.addToBatch(record);
        batchCount++;

        // 双重检查：数量 或 时间
        if (isOverBatchSize() || isOverInterval()) {
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

        SQLException lastException = null;
        for (int retry = 1; retry <= maxRetries; retry++) {
            try {
                executor.executeBatch();
                connection.commit();
                batchCount = 0;
                lastFlushTimeMs = System.currentTimeMillis();
                log.debug("Flush 成功: subtaskId={}", Thread.currentThread().getId());
                return;
            } catch (SQLException e) {
                lastException = e;

                // 回滚当前事务，确保下次重试处于干净状态
                try {
                    connection.rollback();
                    log.warn("Flush 失败，已回滚事务 (retry={})", retry);
                } catch (SQLException rollbackEx) {
                    log.error("回滚失败，连接可能已断开", rollbackEx);
                    e.addSuppressed(rollbackEx);
                }

                if (retry >= maxRetries) {
                    break;
                }

                // 重试前重建连接并重新预编译语句（断连自愈），executor 缓冲数据保留
                try {
                    reconnect();
                } catch (SQLException | RuntimeException reconnectEx) {
                    e.addSuppressed(reconnectEx);
                    log.warn("重建连接失败，将继续在旧连接上重试 (retry={})", retry, reconnectEx);
                }

                // 等待后重试
                long sleepMs = 1000L * retry;
                log.warn("Flush 失败，等待 {}ms 后重试 (retry={})", sleepMs, retry);
                Thread.sleep(sleepMs);
            }
        }

        batchCount = 0;
        throw new IOException("Flush failed after " + maxRetries + " retries", lastException);
    }

    /**
     * 重建连接并重新预编译语句（断连自愈）
     * executor 内部缓冲（待重试数据）保留，由 flush 重试继续使用
     */
    private void reconnect() throws SQLException {
        log.warn("尝试重建 JDBC 连接: url={}", url);
        try {
            executor.closeStatements();
        } catch (SQLException e) {
            log.debug("关闭旧 Statement 失败（忽略）", e);
        }
        IOUtils.closeQuietly(connection);

        this.connection = createConnection();
        executor.prepareStatements(connection);
        log.info("JDBC 连接重建成功: url={}", url);
    }

    private Connection createConnection() throws SQLException {
        Connection conn = JdbcUtil.getConnection(url, username, password, driverClass);
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() throws SQLException, IOException, InterruptedException {
        try {
            // 提交剩余数据
            flush();
        } finally {
            try {
                executor.closeStatements();
            } finally {
                // 连接由 OutputFormat 全权管理（可能已被重连替换过）
                IOUtils.closeQuietly(connection);
            }
        }
    }

    private boolean isOverBatchSize() {
        return batchSize > 0 && batchCount >= batchSize;
    }

    private boolean isOverInterval() {
        return batchIntervalMs > 0
                && (System.currentTimeMillis() - lastFlushTimeMs) >= batchIntervalMs;
    }
}
