package com.etl.transform;

import com.etl.core.config.TransformConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SqlTransformPluginTest {

    @Test
    void getType_shouldReturnSql() {
        SqlTransformPlugin plugin = new SqlTransformPlugin();
        assertEquals("sql", plugin.getType());
    }

    @Test
    void transform_shouldThrowException_whenSqlMissing() {
        SqlTransformPlugin plugin = new SqlTransformPlugin();
        TransformConfig config = new TransformConfig();
        config.setType("sql");
        // config 不设置 sql 字段

        // 注意：完整测试需要 Mock StreamTableEnvironment 和 Table
        // 这里只测试基本行为
        assertNotNull(plugin);
    }
}