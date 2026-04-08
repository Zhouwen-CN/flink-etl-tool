package com.etl.core.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SqlUtils 测试类
 * 使用 H2 内存数据库测试 getPrimaryKey 方法
 */
public class SqlUtilsTest {

    private static final String H2_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";

    @BeforeEach
    public void setUp() throws Exception {
        // 初始化 H2 数据库连接
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            // 创建测试表会在各个测试方法中执行
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        // 清理数据库
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    public void testGetPrimaryKey_SinglePrimaryKey() throws Exception {
        // 创建单主键表
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE single_pk (id INT PRIMARY KEY, name VARCHAR(100))");
        }

        // 测试获取主键（H2 将表名转为大写）
        Map<String, Integer> pkInfo =
            SqlUtils.getPrimaryKey(H2_URL, "SINGLE_PK", USERNAME, PASSWORD);

        // 验证结果
        assertNotNull(pkInfo, "主键信息不应为 null");
        assertEquals(1, pkInfo.size(), "单主键表应有 1 个主键列");
        assertTrue(pkInfo.containsKey("ID"), "主键列应包含 'ID'");
        assertEquals(java.sql.Types.INTEGER, pkInfo.get("ID"), "'ID' 列类型应为 INTEGER");
    }

    @Test
    public void testGetPrimaryKey_CompositePrimaryKey() throws Exception {
        // 创建复合主键表
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE composite_pk (id INT, name VARCHAR(100), age INT, PRIMARY KEY (id, name))");
        }

        // 测试获取主键（H2 将表名转为大写）
        Map<String, Integer> pkInfo =
            SqlUtils.getPrimaryKey(H2_URL, "COMPOSITE_PK", USERNAME, PASSWORD);

        // 验证结果
        assertNotNull(pkInfo, "主键信息不应为 null");
        assertEquals(2, pkInfo.size(), "复合主键表应有 2 个主键列");

        // 验证顺序（KEY_SEQ 顺序）
        String[] expectedKeys = {"ID", "NAME"};
        int index = 0;
        for (String key : pkInfo.keySet()) {
            assertEquals(expectedKeys[index], key, "第 " + (index + 1) + " 个主键列应为 " + expectedKeys[index]);
            index++;
        }

        // 验证类型
        assertEquals(java.sql.Types.INTEGER, pkInfo.get("ID"), "'ID' 列类型应为 INTEGER");
        assertEquals(java.sql.Types.VARCHAR, pkInfo.get("NAME"), "'NAME' 列类型应为 VARCHAR");
    }

    @Test
    public void testGetPrimaryKey_NoPrimaryKey() throws Exception {
        // 创建无主键表
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE no_pk (id INT, name VARCHAR(100))");
        }

        // 测试获取主键（应抛异常）
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SqlUtils.getPrimaryKey(H2_URL, "NO_PK", USERNAME, PASSWORD);
        }, "无主键表应抛出 RuntimeException");

        assertTrue(exception.getMessage().contains("没有主键"),
            "异常信息应包含 '没有主键'");
    }

    @Test
    public void testGetPrimaryKey_TableNotExist() {
        // 测试获取不存在表的主键（应抛异常）
        // 注意：DatabaseMetaData.getPrimaryKeys() 对不存在的表返回空结果，所以会得到"没有主键"异常
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SqlUtils.getPrimaryKey(H2_URL, "NONEXISTENT_TABLE", USERNAME, PASSWORD);
        }, "表不存在应抛出 RuntimeException");

        assertTrue(exception.getMessage().contains("没有主键") ||
                   exception.getMessage().contains("获取主键失败"),
            "异常信息应提示表不存在或没有主键");
    }
}