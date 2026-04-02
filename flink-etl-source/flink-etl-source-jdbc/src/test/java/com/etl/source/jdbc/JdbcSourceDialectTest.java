package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JdbcSource dialect 配置测试
 */
class JdbcSourceDialectTest {

    @Test
    void testExplicitDialect_mysql() {
        // 显式配置 mysql dialect（使用 OceanBase URL）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:oceanbase://localhost:2881/test_db");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        configMap.put("dialect", "mysql"); // 显式指定 mysql dialect

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("jdbc");
        sourceConfig.setOutputTable("test_output");
        sourceConfig.setConfig(configMap);

        // 构造 JdbcSource 应该成功，使用显式配置的 mysql dialect
        assertDoesNotThrow(() -> {
            JdbcSource source = new JdbcSource(sourceConfig);
            assertNotNull(source);
        });
    }

    @Test
    void testExplicitDialect_oracle() {
        // 显式配置 oracle dialect（使用 MySQL URL，强制使用 oracle dialect）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test_db");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        configMap.put("dialect", "oracle"); // 显式指定 oracle dialect

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("jdbc");
        sourceConfig.setOutputTable("test_output");
        sourceConfig.setConfig(configMap);

        // 构造 JdbcSource 应该成功，使用显式配置的 oracle dialect
        assertDoesNotThrow(() -> {
            JdbcSource source = new JdbcSource(sourceConfig);
            assertNotNull(source);
        });
    }

    @Test
    void testInvalidDialect() {
        // 配置无效的 dialect 名称
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test_db");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        configMap.put("dialect", "invalid_dialect"); // 无效的 dialect 名称

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("jdbc");
        sourceConfig.setOutputTable("test_output");
        sourceConfig.setConfig(configMap);

        // 构造 JdbcSource 应该抛出异常
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSource(sourceConfig);
        });

        assertTrue(exception.getMessage().contains("不支持的 Dialect 类型"));
    }

    @Test
    void testAutoDetectByUrl() {
        // 不配置 dialect，验证自动从 URL 识别
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test_db");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        // 不设置 dialect

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("jdbc");
        sourceConfig.setOutputTable("test_output");
        sourceConfig.setConfig(configMap);

        // 构造 JdbcSource 应该成功，自动识别 mysql dialect
        assertDoesNotThrow(() -> {
            JdbcSource source = new JdbcSource(sourceConfig);
            assertNotNull(source);
        });
    }

    @Test
    void testAutoDetectUnsupportedUrl() {
        // 不配置 dialect 且 URL 无法识别
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:unsupported://localhost:3306/test_db");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");
        // 不设置 dialect

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("jdbc");
        sourceConfig.setOutputTable("test_output");
        sourceConfig.setConfig(configMap);

        // 构造 JdbcSource 应该抛出异常
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSource(sourceConfig);
        });

        assertTrue(exception.getMessage().contains("不支持的数据库类型"));
    }
}