package com.etl.connector.jdbc.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MySQLDialectTest {

    private final MySQLDialect dialect = new MySQLDialect();

    @Test
    void testBuildDateRangeQuery_AllNull() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "`date_col`", null, null);
        assertEquals("SELECT * FROM users", result);
    }

    @Test
    void testBuildDateRangeQuery_OnlyStartDate() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "`date_col`", "2020-01-01", null);
        assertEquals("SELECT * FROM users WHERE `date_col` >= '2020-01-01'", result);
    }

    @Test
    void testBuildDateRangeQuery_OnlyEndDate() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "`date_col`", null, "2020-02-01");
        assertEquals("SELECT * FROM users WHERE `date_col` < '2020-02-01'", result);
    }

    @Test
    void testBuildDateRangeQuery_OpenInterval() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "`date_col`", "2020-01-01", "2020-02-01");
        assertEquals("SELECT * FROM users WHERE `date_col` >= '2020-01-01' AND `date_col` < '2020-02-01'", result);
    }
}