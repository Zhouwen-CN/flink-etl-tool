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
        return new MySQLSinkFunction(JDBC_URL, USERNAME, PASSWORD, TABLE, batchSize,
                writeMode != null ? writeMode : "insert");
    }

    private Row namedRow(int id, String name, int age) {
        Row row = Row.withNames();
        row.setField("id", id);
        row.setField("name", name);
        row.setField("age", age);
        return row;
    }

    @Test
    void testInsertSingleRow() throws Exception {
        MySQLSinkFunction sink = createSink("insert", 1);
        sink.open(new org.apache.flink.configuration.Configuration());

        sink.invoke(namedRow(1, "Alice", 30), null);
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

        sink.invoke(namedRow(1, "Alice", 30), null);
        sink.invoke(namedRow(2, "Bob", 25), null);
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

        sink.invoke(namedRow(1, "Alice", 30), null);
        sink.invoke(namedRow(2, "Bob", 25), null);

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

        sink.invoke(namedRow(1, "Alice", 30), null);
        sink.invoke(namedRow(1, "Alice Updated", 31), null);
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
