package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 测试 JdbcSink 的 dialect 配置支持
 */
class JdbcSinkDialectTest {

    @Test
    void testExplicitDialect_mysql() {
        // 显式配置 mysql dialect（OceanBase URL + mysql dialect）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:oceanbase://localhost:2881/test");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        configMap.put("dialect", "mysql"); // 显式指定 mysql dialect
        configMap.put("batchSize", 100);

        SinkConfig config = new SinkConfig();
        config.setType("jdbc");
        config.setInputTable("test_input");
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink);
    }

    @Test
    void testExplicitDialect_oracle() {
        // 显式配置 oracle dialect（MySQL URL + oracle dialect）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        configMap.put("dialect", "oracle"); // 显式指定 oracle dialect
        configMap.put("batchSize", 100);

        SinkConfig config = new SinkConfig();
        config.setType("jdbc");
        config.setInputTable("test_input");
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink);
    }

    @Test
    void testInvalidDialect() {
        // 配置无效的 dialect 名称，验证抛出异常
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        configMap.put("dialect", "invalid_dialect"); // 无效的 dialect
        configMap.put("batchSize", 100);

        SinkConfig config = new SinkConfig();
        config.setType("jdbc");
        config.setInputTable("test_input");
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new JdbcSink(config)
        );

        assertTrue(exception.getMessage().contains("不支持的 Dialect 类型"));
    }

    @Test
    void testAutoDetectByUrl() {
        // 不配置 dialect，验证自动从 URL 识别
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        // 不配置 dialect 参数
        configMap.put("batchSize", 100);

        SinkConfig config = new SinkConfig();
        config.setType("jdbc");
        config.setInputTable("test_input");
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink);
    }

    @Test
    void testAutoDetectUnsupportedUrl() {
        // 不配置 dialect 且 URL 无法识别，验证抛出异常
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:unsupported://localhost:3306/test");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        // 不配置 dialect 参数
        configMap.put("batchSize", 100);

        SinkConfig config = new SinkConfig();
        config.setType("jdbc");
        config.setInputTable("test_input");
        config.setConfig(configMap);

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new JdbcSink(config)
        );

        assertTrue(exception.getMessage().contains("不支持的数据库类型"));
    }
}