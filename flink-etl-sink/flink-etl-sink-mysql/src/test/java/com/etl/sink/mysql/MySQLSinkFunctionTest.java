package com.etl.sink.mysql;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MySQLSinkFunction 单元测试
 * 使用 H2 内存数据库（MySQL 兼容模式）
 */
class MySQLSinkFunctionTest {

    private static Connection conn;
    private static final String JDBC_URL = "jdbc:h2:mem:testdb;MODE=MySQL;DB_CLOSE_DELAY=-1";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";
    private static final String TABLE = "user_table";

    @BeforeAll
    static void setUpDatabase() throws Exception {
        conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE user_table (" +
                "  id INT PRIMARY KEY," +
                "  name VARCHAR(100)," +
                "  age INT" +
                ")"
            );
        }
    }

    @AfterAll
    static void tearDownDatabase() throws Exception {
        if (conn != null) conn.close();
    }

    @AfterEach
    void clearTable() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM user_table");
        }
    }

    private MySQLSinkFunction createSink(String writeMode, int batchSize) {
        Map<String, Object> config = new HashMap<>();
        config.put("url", JDBC_URL);
        config.put("username", USERNAME);
        config.put("password", PASSWORD);
        config.put("table", TABLE);
        config.put("columns", "id,name,age");
        config.put("batchSize", batchSize);
        if (writeMode != null) {
            config.put("writeMode", writeMode);
        }
        return new MySQLSinkFunction(JDBC_URL, USERNAME, PASSWORD, TABLE,
                new String[]{"id", "name", "age"}, batchSize,
                writeMode != null ? writeMode : "insert");
    }

    @Test
    void testInsertSingleRow() throws Exception {
        MySQLSinkFunction sink = createSink("insert", 1);
        sink.open(new org.apache.flink.configuration.Configuration());

        Row row = Row.of(1, "Alice", 30);
        sink.invoke(row, null);
        sink.close();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM user_table WHERE id=1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertEquals(30, rs.getInt("age"));
        }
    }

    @Test
    void testBatchInsert() throws Exception {
        MySQLSinkFunction sink = createSink("insert", 3);
        sink.open(new org.apache.flink.configuration.Configuration());

        // 插入 2 条（未触发批量提交）
        sink.invoke(Row.of(1, "Alice", 30), null);
        sink.invoke(Row.of(2, "Bob", 25), null);

        // 此时批次未满，数据可能还在缓冲，调用 close 触发 flush
        sink.close();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM user_table")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testBatchFlushOnFull() throws Exception {
        MySQLSinkFunction sink = createSink("insert", 2);
        sink.open(new org.apache.flink.configuration.Configuration());

        // 插入 2 条，触发批量提交
        sink.invoke(Row.of(1, "Alice", 30), null);
        sink.invoke(Row.of(2, "Bob", 25), null);

        // 批次已满，数据已写入
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM user_table")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }

        sink.close();
    }

    @Test
    void testUpsertMode() throws Exception {
        MySQLSinkFunction sink = createSink("upsert", 1);
        sink.open(new org.apache.flink.configuration.Configuration());

        // 插入初始数据
        sink.invoke(Row.of(1, "Alice", 30), null);
        // 更新同一行
        sink.invoke(Row.of(1, "Alice Updated", 31), null);
        sink.close();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM user_table")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM user_table WHERE id=1")) {
            assertTrue(rs.next());
            assertEquals("Alice Updated", rs.getString("name"));
        }
    }
}
