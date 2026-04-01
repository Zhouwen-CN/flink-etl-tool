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
    public void testUpsertWithoutKeyFields() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        configMap.put("mode", "UPSERT");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        assertThrows(NullPointerException.class, () -> {
            new JdbcSink(config);
        }, "UPSERT 模式应该要求 keyFields 参数");
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
}