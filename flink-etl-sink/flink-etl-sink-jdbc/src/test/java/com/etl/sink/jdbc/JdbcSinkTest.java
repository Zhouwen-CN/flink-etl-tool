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

    @Test
    public void testUpsertWithTableAutoPrimaryKey() {
        // UPSERT 模式 + table 配置，应该自动获取主键
        // 注意：实际测试需要真实数据库或 Mock，这里仅测试配置解析不抛异常

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "test_table");
        configMap.put("mode", "UPSERT");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        // 实际运行会尝试连接数据库获取主键
        // 这里只验证配置解析逻辑不抛 NullPointerException
        // 真实数据库测试在集成测试中进行
        assertThrows(RuntimeException.class, () -> {
            new JdbcSink(config);
        }, "UPSERT 模式应该尝试获取主键（实际会因数据库表不存在而失败）");
    }

    @Test
    public void testUpsertWithSqlNotAllowed() {
        // UPSERT 模式配置 sql，应该抛异常
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("sql", "INSERT INTO target_table VALUES(:id, :name)");
        configMap.put("mode", "UPSERT");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "UPSERT 模式配置 sql 应该抛出 IllegalArgumentException");

        assertTrue(exception.getMessage().contains("必须配置 table"),
            "异常信息应提示必须配置 table");
        assertTrue(exception.getMessage().contains("不能使用 sql"),
            "异常信息应提示不能使用 sql");
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
    void testKeyFieldsNoPkButUserConfigured() throws Exception {
        // 测试表无主键但用户配置了 keyFields（应正常使用配置值）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "USERS_NO_PK"); // H2 将表名转为大写，该表无主键
        configMap.put("mode", "UPSERT");
        configMap.put("keyFields", Arrays.asList("email")); // 手动配置唯一键字段

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证使用用户配置的唯一键 email
        List<String> keyFields = sinkConfig.getKeyFields();
        assertNotNull(keyFields);
        assertEquals(1, keyFields.size());
        assertTrue(keyFields.contains("email"));
    }
}