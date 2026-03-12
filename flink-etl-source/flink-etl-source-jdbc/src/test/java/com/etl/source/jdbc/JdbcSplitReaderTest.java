package com.etl.source.jdbc;

import com.etl.core.source.RangeSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JdbcSplitReader 测试
 * 使用 H2 内存数据库进行集成测试
 */
class JdbcSplitReaderTest {

    private static final String JDBC_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";

    private Connection connection;
    private JdbcDialect dialect;

    @BeforeEach
    void setUp() throws Exception {
        // 创建 H2 连接
        connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);

        // 创建测试表
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE test_table (" +
                    "id INT PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "amount INT)");
        }

        // 插入测试数据（20条记录）
        try (Statement stmt = connection.createStatement()) {
            for (int i = 1; i <= 20; i++) {
                stmt.execute(String.format(
                        "INSERT INTO test_table VALUES (%d, 'name_%d', %d)",
                        i, i, i * 10));
            }
        }

        // 使用 H2 方言
        dialect = new H2Dialect();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test_table");
            }
            connection.close();
        }
    }

    @Test
    void testBatchReading() throws Exception {
        /**
         * 测试分批读取功能
         *
         * 场景：
         * 1. 数据库有 20 条记录（id: 1-20）
         * 2. 分片范围：[1, 20]
         * 3. 每批读取 5 条
         * 4. 应该分 4 批读取完成
         */

        // Arrange: 创建 SplitReader，设置批次大小为 5
        JdbcSplitReader reader = new JdbcSplitReader(
                JDBC_URL, USERNAME, PASSWORD,
                "test_table", null, "id",
                5,      // fetchSize: 每批 5 条
                null,   // queryTimeout
                dialect
        );

        // 添加分片
        RangeSplit split = new RangeSplit("id", 1, 20);
        reader.handleSplitsChanges(new org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition<>(
                java.util.Collections.singletonList(split)
        ));

        // Act & Assert: 分批读取
        int totalRecords = 0;
        int batchCount = 0;

        while (true) {
            RecordsWithSplitIds<Row> records = reader.fetch();

            // 统计本批次记录数
            String splitId = records.nextSplit();
            if (splitId == null) {
                // 没有更多数据
                break;
            }

            int batchRecords = 0;
            while (true) {
                Row row = records.nextRecordFromSplit();
                if (row == null) {
                    break;
                }
                batchRecords++;
                totalRecords++;
            }

            batchCount++;

            // 验证每批最多 5 条（除了最后一批可能少于 5 条）
            assertTrue(batchRecords <= 5,
                    String.format("第 %d 批读取了 %d 条记录，超过批次大小 5", batchCount, batchRecords));
        }

        // Assert: 验证总共读取了 20 条记录
        assertEquals(20, totalRecords, "总共读取的记录数应该是 20");
        assertTrue(batchCount >= 4, String.format("至少应该有 4 批，实际 %d 批", batchCount));
    }

    @Test
    void testStreamingReadConfiguration() throws Exception {
        /**
         * 测试流式读取配置
         *
         * 场景：
         * 1. 设置合理的 fetchSize 值
         * 2. 验证 Statement 配置正确，不会抛出异常
         */

        // 使用合理的 fetchSize 值（H2 支持）
        int fetchSize = 10;  // 流式读取批次大小

        JdbcSplitReader reader = new JdbcSplitReader(
                JDBC_URL, USERNAME, PASSWORD,
                "test_table", null, "id",
                fetchSize,
                null,
                dialect
        );

        RangeSplit split = new RangeSplit("id", 1, 20);
        reader.handleSplitsChanges(new org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition<>(
                java.util.Collections.singletonList(split)
        ));

        // 执行读取，不应该抛出异常
        RecordsWithSplitIds<Row> records = reader.fetch();
        assertNotNull(records, "读取结果不应该为 null");
    }

    @Test
    void testStateTracking() throws Exception {
        /**
         * 测试状态跟踪
         *
         * 场景：
         * 1. 分批读取时，跟踪已读取的记录数
         * 2. 验证读取进度正确
         */

        JdbcSplitReader reader = new JdbcSplitReader(
                JDBC_URL, USERNAME, PASSWORD,
                "test_table", null, "id",
                3,  // 每批 3 条
                null,
                dialect
        );

        RangeSplit split = new RangeSplit("id", 1, 10);
        reader.handleSplitsChanges(new org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition<>(
                java.util.Collections.singletonList(split)
        ));

        // 第一批：读取 3 条
        RecordsWithSplitIds<Row> batch1 = reader.fetch();
        String splitId1 = batch1.nextSplit();
        assertNotNull(splitId1, "第一批应该有数据");

        int count1 = 0;
        while (batch1.nextRecordFromSplit() != null) {
            count1++;
        }
        assertEquals(3, count1, "第一批应该读取 3 条记录");

        // 第二批：读取 3 条
        RecordsWithSplitIds<Row> batch2 = reader.fetch();
        String splitId2 = batch2.nextSplit();
        assertNotNull(splitId2, "第二批应该有数据");

        int count2 = 0;
        while (batch2.nextRecordFromSplit() != null) {
            count2++;
        }
        assertEquals(3, count2, "第二批应该读取 3 条记录");

        // 继续读取剩余记录...
    }

    /**
     * H2 数据库方言（测试用）
     */
    private static class H2Dialect implements JdbcDialect {
        @Override
        public String getDriverClassName() {
            return "org.h2.Driver";
        }

        @Override
        public String buildRangeQuery(String table, String sql, String splitColumn) {
            if (sql != null) {
                return String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t",
                        splitColumn, splitColumn, sql);
            }
            return String.format("SELECT MIN(%s), MAX(%s) FROM %s",
                    splitColumn, splitColumn, table);
        }

        @Override
        public String buildSplitQuery(String table, String customSql, String splitColumn,
                                       long start, long end) {
            if (customSql != null) {
                return String.format("SELECT * FROM (%s) AS t WHERE %s BETWEEN %d AND %d",
                        customSql, splitColumn, start, end);
            }
            return String.format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d",
                    table, splitColumn, start, end);
        }

        @Override
        public Row createRow(java.sql.ResultSet rs) throws java.sql.SQLException {
            int columnCount = rs.getMetaData().getColumnCount();
            Row row = new Row(columnCount);
            for (int i = 0; i < columnCount; i++) {
                row.setField(i, rs.getObject(i + 1));
            }
            return row;
        }
    }
}