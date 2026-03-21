package com.etl.sink.jdbc;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NamedParameterSqlParserTest {

    @Test
    void testSimplePlaceholders() {
        String sql = "INSERT INTO t(a, b) VALUES(:x, :y)";
        NamedParameterSqlParser.ParsedSql result = NamedParameterSqlParser.parse(sql);

        assertEquals("INSERT INTO t(a, b) VALUES(?, ?)", result.getPreparedSql());
        assertEquals(List.of("x", "y"), result.getParamNames());
    }

    @Test
    void testNoPlaceholders() {
        String sql = "SELECT * FROM t";
        NamedParameterSqlParser.ParsedSql result = NamedParameterSqlParser.parse(sql);

        assertEquals(sql, result.getPreparedSql());
        assertTrue(result.getParamNames().isEmpty());
    }

    @Test
    void testUnderscoreInName() {
        String sql = "INSERT INTO t VALUES(:first_name, :last_name)";
        NamedParameterSqlParser.ParsedSql result = NamedParameterSqlParser.parse(sql);

        assertEquals("INSERT INTO t VALUES(?, ?)", result.getPreparedSql());
        assertEquals(List.of("first_name", "last_name"), result.getParamNames());
    }

    @Test
    void testMixedColumns() {
        String sql = "INSERT INTO employee(last_name, email, dept_id) VALUES(:lastName, :email, :deptId)";
        NamedParameterSqlParser.ParsedSql result = NamedParameterSqlParser.parse(sql);

        assertEquals("INSERT INTO employee(last_name, email, dept_id) VALUES(?, ?, ?)", result.getPreparedSql());
        assertEquals(List.of("lastName", "email", "deptId"), result.getParamNames());
    }
}