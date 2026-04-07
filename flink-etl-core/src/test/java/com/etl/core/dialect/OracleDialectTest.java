package com.etl.core.dialect;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class OracleDialectTest {

    private final OracleDialect dialect = new OracleDialect();

    @Test
    void testGetName() {
        assertEquals("oracle", dialect.getName());
    }

    @Test
    void testAcceptsUrl_oracle() {
        assertTrue(dialect.acceptsUrl("jdbc:oracle:thin:@localhost:1521:test"));
        assertTrue(dialect.acceptsUrl("jdbc:oracle:thin:@//localhost:1521/test"));
    }

    @Test
    void testAcceptsUrl_other() {
        assertFalse(dialect.acceptsUrl("jdbc:mysql://localhost:3306/test"));
        assertFalse(dialect.acceptsUrl("jdbc:postgresql://localhost:5432/test"));
        assertFalse(dialect.acceptsUrl(null));
    }

    @Test
    void testQuoteIdentifier() {
        assertEquals("\"name\"", dialect.quoteIdentifier("name"));
        assertEquals("\"table_name\"", dialect.quoteIdentifier("table_name"));
    }

    @Test
    void testGetInsertSql() {
        String sql = dialect.getInsertSql("user", new String[]{"id", "name", "email"});
        assertEquals("INSERT INTO \"user\" (\"id\", \"name\", \"email\") VALUES (?, ?, ?)", sql);
    }

    @Test
    void testGetUpsertSql() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Collections.singletonList("id"));
        // Oracle 使用 MERGE INTO 语法
        assertNotNull(sql);
        assertTrue(sql.contains("MERGE INTO"));
        assertTrue(sql.contains("\"user\""));
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE"));
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    @Test
    void testGetUpsertSqlWithCompositeKey() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Arrays.asList("id", "name"));
        assertNotNull(sql);
        assertTrue(sql.contains("MERGE INTO"));
        // 复合主键应该在 ON 条件中包含所有字段
        assertTrue(sql.contains("\"id\""));
        assertTrue(sql.contains("\"name\""));
    }
}