package com.etl.core.dialect;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class MySQLDialectCdcTest {

    @Test
    void testGetUpdateSql() {
        MySQLDialect dialect = new MySQLDialect();
        String[] columns = {"id", "name", "email"};
        List<String> keyFields = Arrays.asList("id");

        String sql = dialect.getUpdateSql("users", columns, keyFields);

        assertEquals("UPDATE `users` SET `name` = ?, `email` = ? WHERE `id` = ?", sql);
    }

    @Test
    void testGetUpdateSqlWithCompositeKey() {
        MySQLDialect dialect = new MySQLDialect();
        String[] columns = {"user_id", "order_id", "status"};
        List<String> keyFields = Arrays.asList("user_id", "order_id");

        String sql = dialect.getUpdateSql("orders", columns, keyFields);

        assertTrue(sql.contains("SET `status` = ?"));
        assertTrue(sql.contains("WHERE `user_id` = ? AND `order_id` = ?"));
    }

    @Test
    void testGetDeleteSql() {
        MySQLDialect dialect = new MySQLDialect();
        List<String> keyFields = Arrays.asList("id");

        String sql = dialect.getDeleteSql("users", keyFields);

        assertEquals("DELETE FROM `users` WHERE `id` = ?", sql);
    }

    @Test
    void testGetDeleteSqlWithCompositeKey() {
        MySQLDialect dialect = new MySQLDialect();
        List<String> keyFields = Arrays.asList("user_id", "order_id");

        String sql = dialect.getDeleteSql("orders", keyFields);

        assertEquals("DELETE FROM `orders` WHERE `user_id` = ? AND `order_id` = ?", sql);
    }
}