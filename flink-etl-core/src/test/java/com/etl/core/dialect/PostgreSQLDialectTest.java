package com.etl.core.dialect;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgreSQLDialectTest {

    private final PostgreSQLDialect dialect = new PostgreSQLDialect();

    @Test
    void testAcceptsUrl() {
        assertTrue(dialect.acceptsUrl("jdbc:postgresql://localhost:5432/test"));
        assertFalse(dialect.acceptsUrl("jdbc:mysql://localhost:3306/test"));
        assertFalse(dialect.acceptsUrl(null));
    }

    @Test
    void testWrapUrl() {
        String url = "jdbc:postgresql://localhost:5432/test";
        String wrapped = dialect.wrapUrl(url);
        assertEquals(url, wrapped); // PostgreSQL 不添加额外参数
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
        assertEquals("INSERT INTO \"user\" (\"id\", \"name\", \"email\") VALUES (?, ?, ?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\", \"email\"=EXCLUDED.\"email\"", sql);
    }

    @Test
    void testGetUpsertSqlWithCompositeKey() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Arrays.asList("id", "name"));
        assertEquals("INSERT INTO \"user\" (\"id\", \"name\", \"email\") VALUES (?, ?, ?) ON CONFLICT (\"id\", \"name\") DO UPDATE SET \"email\"=EXCLUDED.\"email\"", sql);
    }
}