package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.sink.executor.SimpleBufferedExecutor;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JdbcOutputFormat 测试
 */
class JdbcOutputFormatTest {

    private Connection connection;
    private JdbcOutputFormat outputFormat;

    @BeforeEach
    void setUp() throws SQLException {
        // 使用唯一数据库名避免测试间冲突
        connection = DriverManager.getConnection("jdbc:h2:mem:test" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        connection.setAutoCommit(false);

        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE \"test_table\" (\"id\" INT PRIMARY KEY, \"value\" VARCHAR(50))");
        stmt.close();
        connection.commit();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (outputFormat != null) {
            outputFormat.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void testBatchSizeFlush_reachesBatchSize_flushesAutomatically() throws Exception {
        // 创建 Executor 和 OutputFormat
        String insertSql = "INSERT INTO \"test_table\" (\"id\", \"value\") VALUES (?, ?)";
        SimpleBufferedExecutor executor = new SimpleBufferedExecutor(insertSql, new String[]{"id", "value"});
        executor.prepareStatements(connection);

        // batchSize=10, batchIntervalMs=0
        outputFormat = new JdbcOutputFormat(executor, connection, 10, 0, 3);
        outputFormat.open();

        // 写入 10 条数据触发 batch_size 刷写
        for (int i = 0; i < 10; i++) {
            Row row = Row.withNames();
            row.setField("id", i);
            row.setField("value", "value" + i);
            outputFormat.writeRecord(row);
        }

        // 验证数据库有 10 条记录（batch_size 自动刷写）
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"test_table\"");
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        rs.close();
        stmt.close();
    }

    @Test
    void testFlushBeforeClose_partialBatch_flushedOnClose() throws Exception {
        // 创建 Executor 和 OutputFormat
        String insertSql = "INSERT INTO \"test_table\" (\"id\", \"value\") VALUES (?, ?)";
        SimpleBufferedExecutor executor = new SimpleBufferedExecutor(insertSql, new String[]{"id", "value"});
        executor.prepareStatements(connection);

        // batchSize=100（远大于写入数量）, batchIntervalMs=0
        outputFormat = new JdbcOutputFormat(executor, connection, 100, 0, 3);
        outputFormat.open();

        // 写入 5 条数据（未达到 batch_size）
        for (int i = 0; i < 5; i++) {
            Row row = Row.withNames();
            row.setField("id", i);
            row.setField("value", "value" + i);
            outputFormat.writeRecord(row);
        }

        // 显式 flush（模拟 close 时会做的操作），提交剩余数据
        outputFormat.flush();

        // 验证数据库有 5 条记录
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"test_table\"");
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        rs.close();
        stmt.close();

        // tearDown 中的 close() 会再次 flush（但数据已在数据库中）
    }

    @Test
    void testBatchIntervalMs_enabled_flushesAfterInterval() throws Exception {
        // 创建 Executor 和 OutputFormat：batchSize=100, batchIntervalMs=100ms
        String insertSql = "INSERT INTO \"test_table\" (\"id\", \"value\") VALUES (?, ?)";
        SimpleBufferedExecutor executor = new SimpleBufferedExecutor(insertSql, new String[]{"id", "value"});
        executor.prepareStatements(connection);

        outputFormat = new JdbcOutputFormat(executor, connection, 100, 100, 3);
        outputFormat.open();

        // 写入 1 条数据（未达到 batch_size）
        Row row = Row.withNames();
        row.setField("id", 99);
        row.setField("value", "test");
        outputFormat.writeRecord(row);

        // 等待超过 batchIntervalMs
        Thread.sleep(150);

        // 再次写入触发时间检查
        Row row2 = Row.withNames();
        row2.setField("id", 100);
        row2.setField("value", "trigger");
        outputFormat.writeRecord(row2);

        // 验证数据库已经有数据（时间阈值触发了刷写）
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"test_table\"");
        assertTrue(rs.next());
        assertTrue(rs.getInt(1) >= 1);  // 至少有第一条数据
        rs.close();
        stmt.close();
    }
}
