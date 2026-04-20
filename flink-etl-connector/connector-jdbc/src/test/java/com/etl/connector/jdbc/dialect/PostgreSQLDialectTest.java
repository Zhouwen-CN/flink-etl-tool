package com.etl.connector.jdbc.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgreSQLDialectTest {

    private final PostgreSQLDialect dialect = new PostgreSQLDialect();

    @Test
    void testHashModExpression() {
        String result = dialect.hashModExpression("\"username\"", 10);
        assertEquals("hashtext(\"username\") % 10", result);
    }

    @Test
    void testBuildDateRangeQuery_OpenInterval() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "\"date_col\"", "2020-01-01", "2020-02-01");
        assertEquals("SELECT * FROM users WHERE \"date_col\" >= '2020-01-01' AND \"date_col\" < '2020-02-01'", result);
    }
}