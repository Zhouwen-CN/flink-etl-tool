package com.etl.connector.cdc.mysql;

import com.etl.connector.cdc.mysql.config.MySqlCdcConfig;
import com.etl.core.config.SourceConfig;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MySqlCdcConfig 测试类
 */
class MySqlCdcConfigTest {

    @Test
    void testParseUrlWithInvalidPrefix() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:postgresql://localhost:5432/mydb");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            MySqlCdcConfig.fromSourceConfig(sourceConfig);
        }, "URL 必须以 jdbc:mysql:// 开头");
    }

    @Test
    void testParseUrlWithInvalidFormat() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://invalid_url");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            MySqlCdcConfig.fromSourceConfig(sourceConfig);
        }, "URL 格式错误");
    }

    @Test
    void testParseUrlSuccessfully() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        MySqlCdcConfig config = MySqlCdcConfig.fromSourceConfig(sourceConfig);

        assertEquals("localhost", config.getHostname());
        assertEquals(3306, config.getPort());
        assertEquals("mydb", config.getDatabase());
        assertEquals("root", config.getUsername());
        assertEquals("password", config.getPassword());
        assertEquals("users", config.getTable());
        assertEquals(StartupMode.LATEST, config.getStartupMode());
    }

    @Test
    void testStartupModeDefault() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");
        // 不配置 startupMode，验证默认值为 LATEST

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        MySqlCdcConfig config = MySqlCdcConfig.fromSourceConfig(sourceConfig);

        assertEquals(StartupMode.LATEST, config.getStartupMode());
    }

    @Test
    void testStartupModeEarliest() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");
        configMap.put("startupMode", "earliest");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        MySqlCdcConfig config = MySqlCdcConfig.fromSourceConfig(sourceConfig);

        assertEquals(StartupMode.EARLIEST, config.getStartupMode());
        StartupOptions options = config.getStartupOptions();
        assertNotNull(options);
    }

    @Test
    void testOptionalServerId() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");
        configMap.put("serverId", 5400);

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        MySqlCdcConfig config = MySqlCdcConfig.fromSourceConfig(sourceConfig);

        assertEquals(5400, config.getServerId());
    }

    @Test
    void testOptionalServerIdNull() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");
        // 不配置 serverId，验证自动生成

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        MySqlCdcConfig config = MySqlCdcConfig.fromSourceConfig(sourceConfig);

        // 新实现：自动生成 serverId（范围：5400-15400）
        assertNotNull(config.getServerId());
        assertTrue(config.getServerId() >= 5400 && config.getServerId() <= 15400);
    }

    @Test
    void testParseUrlWithParameters() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb?useSSL=false&serverTimezone=UTC");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        MySqlCdcConfig config = MySqlCdcConfig.fromSourceConfig(sourceConfig);

        assertEquals("localhost", config.getHostname());
        assertEquals(3306, config.getPort());
        assertEquals("mydb", config.getDatabase());  // 确保只提取 database，不包含参数
    }

    @Test
    void testMissingUsername() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
        // 缺失 username
        configMap.put("password", "password");
        configMap.put("table", "users");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        // 新实现使用 Preconditions.checkNotNull，抛出 NullPointerException
        assertThrows(NullPointerException.class, () -> {
            MySqlCdcConfig.fromSourceConfig(sourceConfig);
        }, "username 参数不能为空");
    }

    @Test
    void testMissingPassword() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
        configMap.put("username", "root");
        // 缺失 password
        configMap.put("table", "users");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        // 新实现使用 Preconditions.checkNotNull，抛出 NullPointerException
        assertThrows(NullPointerException.class, () -> {
            MySqlCdcConfig.fromSourceConfig(sourceConfig);
        }, "password 参数不能为空");
    }

    @Test
    void testMissingTable() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
        configMap.put("username", "root");
        configMap.put("password", "password");
        // 缺失 table

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        // 新实现使用 Preconditions.checkNotNull，抛出 NullPointerException
        assertThrows(NullPointerException.class, () -> {
            MySqlCdcConfig.fromSourceConfig(sourceConfig);
        }, "table 参数不能为空");
    }
}