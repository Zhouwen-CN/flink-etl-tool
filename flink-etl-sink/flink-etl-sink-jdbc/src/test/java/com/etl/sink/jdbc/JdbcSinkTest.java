package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JdbcSink 测试
 * 主要测试参数校验逻辑
 */
public class JdbcSinkTest {

    @BeforeAll
    static void setupTestDatabase() throws Exception {
        // 创建 H2 内存数据库和测试表（使用 DB_CLOSE_DELAY=-1 保持连接）
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "sa", "");
             Statement stmt = conn.createStatement()) {
            // 创建带主键的表（主键为 id，H2 将表名转为大写）
            stmt.execute("CREATE TABLE USERS_WITH_PK (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))");
            // 创建无主键但有唯一索引的表
            stmt.execute("CREATE TABLE USERS_NO_PK (id INT, name VARCHAR(100), email VARCHAR(100) UNIQUE)");
        }
    }

    @Test
    public void testMissingUrl() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        assertThrows(NullPointerException.class, () -> {
            new JdbcSink(config);
        }, "应该抛出异常，因为缺少 url 参数");
    }

    @Test
    public void testMissingTableAndSql() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("username", "root");
        configMap.put("password", "password");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "应该抛出异常，因为缺少 table 和 sql 参数");
    }

    @Test
    public void testInvalidBatchSize() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        configMap.put("batchSize", 0);

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "batchSize <= 0 应该抛出异常");
    }

    // ==================== INSERT 模式测试 ====================

    @Test
    void testInsertModeWithTable() throws Exception {
        // INSERT 模式 + table 配置，应该成功，keyFields 应为 null
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_WITH_PK");
        configMap.put("mode", "INSERT");
        // 不配置 keyFields

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证 INSERT 模式下 keyFields 为 null
        assertNull(sinkConfig.getKeyFields(), "INSERT 模式下 keyFields 应为 null");
        assertEquals("USERS_WITH_PK", sinkConfig.getTable());
    }

    @Test
    void testInsertModeWithSqlShouldFail() {
        // INSERT 模式 + sql 配置，应该失败（INSERT 必须配置 table，不能配置 sql）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("sql", "INSERT INTO users (id, name) VALUES (?, ?)");
        configMap.put("mode", "INSERT");
        // 不配置 table

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "INSERT 模式 + sql 配置应该失败");

        assertTrue(exception.getMessage().contains("INSERT 模式必须配置 table"),
                "异常消息应包含 'INSERT 模式必须配置 table'");
    }

    @Test
    void testInsertModeWithKeyFieldsShouldFail() {
        // INSERT 模式 + keyFields 配置，应该失败（INSERT 忽略 keyFields）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_WITH_PK");
        configMap.put("mode", "INSERT");
        configMap.put("keyFields", Arrays.asList("id")); // INSERT 模式不应配置 keyFields

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "INSERT 模式 + keyFields 配置应该失败");

        assertTrue(exception.getMessage().contains("INSERT 模式不支持 keyFields"),
                "异常消息应包含 'INSERT 模式不支持 keyFields'");
    }

    // ==================== UPSERT 模式测试 ====================

    @Test
    void testUpsertWithTableNoPkShouldFail() {
        // UPSERT 模式 + table 无主键且未配置 keyFields，应该失败
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_NO_PK"); // 该表无主键
        configMap.put("mode", "UPSERT");
        // 不配置 keyFields

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            new JdbcSink(config);
        }, "UPSERT 模式 + table 无主键应该失败");

        assertTrue(exception.getMessage().contains("没有主键"),
                "异常消息应包含 '没有主键'");
    }

    @Test
    void testKeyFieldsUserConfigured() throws Exception {
        // 测试用户配置 keyFields 的场景（使用配置的主键，而非数据库主键）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_WITH_PK"); // H2 将表名转为大写
        configMap.put("mode", "UPSERT");
        configMap.put("keyFields", Arrays.asList("email")); // 用户配置 email 为主键（忽略真实主键 id）

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证使用用户配置的主键 email，而非数据库主键 id
        List<String> keyFields = sinkConfig.getKeyFields();
        assertNotNull(keyFields);
        assertEquals(1, keyFields.size());
        assertTrue(keyFields.contains("email"));
        assertFalse(keyFields.contains("id")); // 不应包含真实主键
    }

    @Test
    void testKeyFieldsAutoFetched() throws Exception {
        // 测试未配置 keyFields 时自动从数据库获取主键
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_WITH_PK"); // H2 将表名转为大写
        configMap.put("mode", "UPSERT");
        // 不配置 keyFields，应自动获取表主键

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证自动获取的主键为 ID（数据库真实主键，H2 将列名转为大写）
        List<String> keyFields = sinkConfig.getKeyFields();
        assertNotNull(keyFields);
        assertEquals(1, keyFields.size());
        assertTrue(keyFields.contains("ID")); // H2 将列名转为大写
    }

    @Test
    void testUpsertWithSqlShouldFail() {
        // UPSERT 模式 + sql 配置，应该失败（UPSERT 必须配置 table，不能配置 sql）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("sql", "INSERT INTO users (id, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = ?");
        configMap.put("mode", "UPSERT");
        // 不配置 table

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "UPSERT 模式 + sql 配置应该失败");

        assertTrue(exception.getMessage().contains("UPSERT 模式必须配置 table"),
                "异常消息应包含 'UPSERT 模式必须配置 table'");
    }

    // ==================== CDC 模式测试 ====================

    @Test
    void testCdcModeWithTableAutoKeyFields() throws Exception {
        // CDC 模式 + table 配置，应该自动获取主键（类似 UPSERT）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_WITH_PK");
        configMap.put("mode", "CDC");
        // 不配置 keyFields，应自动获取主键

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证自动获取的主键为 ID（数据库真实主键，H2 将列名转为大写）
        List<String> keyFields = sinkConfig.getKeyFields();
        assertNotNull(keyFields, "CDC 模式应自动获取主键");
        assertEquals(1, keyFields.size());
        assertTrue(keyFields.contains("ID")); // H2 将列名转为大写
    }

    @Test
    void testCdcModeWithKeyFieldsUserConfigured() throws Exception {
        // CDC 模式 + 用户配置 keyFields，应该使用用户配置（类似 UPSERT）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_WITH_PK");
        configMap.put("mode", "CDC");
        configMap.put("keyFields", Arrays.asList("email")); // 用户配置 email 为主键

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证使用用户配置的主键 email，而非数据库主键 id
        List<String> keyFields = sinkConfig.getKeyFields();
        assertNotNull(keyFields);
        assertEquals(1, keyFields.size());
        assertTrue(keyFields.contains("email"));
    }

    @Test
    void testCdcModeWithSqlShouldFail() {
        // CDC 模式 + sql 配置，应该失败（CDC 必须配置 table，不能配置 sql）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("sql", "INSERT INTO users (id, name) VALUES (?, ?)");
        configMap.put("mode", "CDC");
        // 不配置 table

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "CDC 模式 + sql 配置应该失败");

        assertTrue(exception.getMessage().contains("CDC 模式必须配置 table"),
                "异常消息应包含 'CDC 模式必须配置 table'");
    }

    // ==================== CUSTOM 模式测试 ====================

    @Test
    void testCustomModeWithSql() throws Exception {
        // CUSTOM 模式 + sql 配置，应该成功，keyFields 应为 null
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("sql", "INSERT INTO users (id, name) VALUES (?, ?)");
        configMap.put("mode", "CUSTOM");
        // 不配置 table 和 keyFields

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证 CUSTOM 模式下 keyFields 为 null，table 为 null
        assertNull(sinkConfig.getKeyFields(), "CUSTOM 模式下 keyFields 应为 null");
        assertNull(sinkConfig.getTable(), "CUSTOM 模式下 table 应为 null");
        assertNotNull(sinkConfig.getSql());
    }

    @Test
    void testCustomModeWithTableShouldFail() {
        // CUSTOM 模式 + table 配置，应该失败（CUSTOM 必须配置 sql，不能配置 table）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_WITH_PK");
        configMap.put("mode", "CUSTOM");
        // 不配置 sql

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "CUSTOM 模式 + table 配置应该失败");

        assertTrue(exception.getMessage().contains("CUSTOM 模式必须配置 sql"),
                "异常消息应包含 'CUSTOM 模式必须配置 sql'");
    }

    @Test
    void testCustomModeWithKeyFieldsShouldFail() {
        // CUSTOM 模式 + keyFields 配置，应该失败（CUSTOM 忽略 keyFields）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("sql", "INSERT INTO users (id, name) VALUES (?, ?)");
        configMap.put("mode", "CUSTOM");
        configMap.put("keyFields", Arrays.asList("id")); // CUSTOM 模式不应配置 keyFields

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "CUSTOM 模式 + keyFields 配置应该失败");

        assertTrue(exception.getMessage().contains("CUSTOM 模式不支持 keyFields"),
                "异常消息应包含 'CUSTOM 模式不支持 keyFields'");
    }
}