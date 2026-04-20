package com.etl.connector.jdbc.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class H2DialectTest {

    private final H2Dialect dialect = new H2Dialect();

    @Test
    void testHashModExpression() {
        String result = dialect.hashModExpression("USERNAME", 10);
        assertEquals("MOD(HASH(USERNAME), 10)", result);
    }

    @Test
    void testBuildDateRangeQuery_OpenInterval() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "DATE_COL", "2020-01-01", "2020-02-01");
        assertEquals("SELECT * FROM users WHERE DATE_COL >= '2020-01-01' AND DATE_COL < '2020-02-01'", result);
    }
}