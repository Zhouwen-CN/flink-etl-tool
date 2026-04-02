package com.etl.core.dialect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JdbcDialectsTest {

    @Test
    void testGetByName_mysql() {
        JdbcDialect dialect = JdbcDialects.getByName("mysql");
        assertNotNull(dialect);
        assertEquals("mysql", dialect.getName());
        assertTrue(dialect instanceof MySQLDialect);
    }

    @Test
    void testGetByName_postgresql() {
        JdbcDialect dialect = JdbcDialects.getByName("postgresql");
        assertNotNull(dialect);
        assertEquals("postgresql", dialect.getName());
        assertTrue(dialect instanceof PostgreSQLDialect);
    }

    @Test
    void testGetByName_invalidName() {
        assertThrows(IllegalArgumentException.class, () -> {
            JdbcDialects.getByName("invalid_db");
        }, "不支持的 dialect 名称应该抛出异常");
    }

    @Test
    void testGetByName_nullName() {
        assertThrows(IllegalArgumentException.class, () -> {
            JdbcDialects.getByName(null);
        }, "null 名称应该抛出异常");
    }

    @Test
    void testGetByUrl_mysql() {
        String url = "jdbc:mysql://localhost:3306/test";
        JdbcDialect dialect = JdbcDialects.getByUrl(url);
        assertNotNull(dialect);
        assertEquals("mysql", dialect.getName());
    }

    @Test
    void testGetByUrl_postgresql() {
        String url = "jdbc:postgresql://localhost:5432/test";
        JdbcDialect dialect = JdbcDialects.getByUrl(url);
        assertNotNull(dialect);
        assertEquals("postgresql", dialect.getName());
    }

    @Test
    void testGetByUrl_unsupportedUrl() {
        String url = "jdbc:unsupported://localhost/test";
        assertThrows(IllegalArgumentException.class, () -> {
            JdbcDialects.getByUrl(url);
        }, "不支持的数据库 URL 应该抛出异常");
    }
}