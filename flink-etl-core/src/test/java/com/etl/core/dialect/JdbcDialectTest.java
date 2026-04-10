package com.etl.core.dialect;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class JdbcDialectTest {

    @Test
    void testGetUpdateSqlExists() throws NoSuchMethodException {
        // 验证 getUpdateSql 方法存在
        JdbcDialect.class.getMethod("getUpdateSql", String.class, String[].class, List.class);
    }

    @Test
    void testGetDeleteSqlExists() throws NoSuchMethodException {
        // 验证 getDeleteSql 方法存在
        JdbcDialect.class.getMethod("getDeleteSql", String.class, List.class);
    }
}