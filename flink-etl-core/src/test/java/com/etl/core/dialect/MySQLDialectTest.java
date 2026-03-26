package com.etl.core.dialect;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class MySQLDialectTest {

    private final MySQLDialect dialect = new MySQLDialect();

    @Test
    void testAcceptsUrl() {
        assertTrue(dialect.acceptsUrl("jdbc:mysql://localhost:3306/test"));
        assertTrue(dialect.acceptsUrl("jdbc:mysql:replication://master,slave/test"));
        assertFalse(dialect.acceptsUrl("jdbc:postgresql://localhost:5432/test"));
        assertFalse(dialect.acceptsUrl(null));
    }

    @Test
    void testWrapUrl() {
        String url = "jdbc:mysql://localhost:3306/test";
        String wrapped = dialect.wrapUrl(url);
        assertTrue(wrapped.contains("useCursorFetch=true"));
        assertTrue(wrapped.contains("rewriteBatchedStatements=true"));
    }

    @Test
    void testWrapUrlWithExistingParams() {
        String url = "jdbc:mysql://localhost:3306/test?useSSL=false";
        String wrapped = dialect.wrapUrl(url);
        assertTrue(wrapped.contains("useSSL=false"));
        assertTrue(wrapped.contains("useCursorFetch=true"));
        assertTrue(wrapped.contains("rewriteBatchedStatements=true"));
    }

    @Test
    void testQuoteIdentifier() {
        assertEquals("`name`", dialect.quoteIdentifier("name"));
        assertEquals("`table_name`", dialect.quoteIdentifier("table_name"));
    }

    @Test
    void testGetInsertSql() {
        String sql = dialect.getInsertSql("user", new String[]{"id", "name", "email"});
        assertEquals("INSERT INTO `user` (`id`, `name`, `email`) VALUES (?, ?, ?)", sql);
    }

    @Test
    void testGetUpsertSql() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Collections.singletonList("id"));
        assertEquals("INSERT INTO `user` (`id`, `name`, `email`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`), `email`=VALUES(`email`)", sql);
    }

    @Test
    void testGetUpsertSqlWithCompositeKey() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Arrays.asList("id", "name"));
        assertEquals("INSERT INTO `user` (`id`, `name`, `email`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `email`=VALUES(`email`)", sql);
    }
}