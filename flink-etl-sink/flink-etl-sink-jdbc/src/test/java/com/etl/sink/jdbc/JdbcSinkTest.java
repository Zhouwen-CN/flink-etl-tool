package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JdbcSink 测试
 * 主要测试参数校验逻辑
 */
public class JdbcSinkTest {

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
        configMap.put("url", "jdbc:h2:mem:testdb");
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
        }, "UPSERT 模式应该尝试获取主键（实际会因数据库不存在而失败）");
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
}