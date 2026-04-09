package com.etl.core.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CliArgumentParser 单元测试 - 变量替换功能
 */
class CliArgumentParserTest {

    @Test
    void testSingleVariableSubstitution() {
        String[] args = {
            "--config", "{\"job\":{\"name\":\"${job_name}\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"console\",\"outputTable\":\"t\",\"config\":{}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}",
            "--job_name", "test-job"
        };

        JobConfig config = CliArgumentParser.parse(args);

        assertNotNull(config);
        assertEquals("test-job", config.getJob().getName());
    }

    @Test
    void testMultipleVariablesSubstitution() {
        String[] args = {
            "--config", "{\"job\":{\"name\":\"${job_name}\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"jdbc\",\"outputTable\":\"t\",\"config\":{\"url\":\"${db_url}\"}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}",
            "--job_name", "test",
            "--db_url", "jdbc:mysql://localhost:3306/test"
        };

        JobConfig config = CliArgumentParser.parse(args);

        assertEquals("test", config.getJob().getName());
        assertEquals("jdbc:mysql://localhost:3306/test",
            config.getSources().get(0).getConfig().get("url"));
    }

    @Test
    void testVariableWithDefaultValue() {
        String[] args = {
            "--config", "{\"job\":{\"name\":\"${job_name:-default-job}\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"console\",\"outputTable\":\"t\",\"config\":{}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}"
        };

        JobConfig config = CliArgumentParser.parse(args);

        assertEquals("default-job", config.getJob().getName());
    }

    @Test
    void testUndefinedVariableThrowsException() {
        String[] args = {
            "--config", "{\"job\":{\"name\":\"${undefined_var}\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"console\",\"outputTable\":\"t\",\"config\":{}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}"
        };

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> CliArgumentParser.parse(args)
        );

        assertTrue(ex.getMessage().contains("变量 'undefined_var' 未定义"));
        assertTrue(ex.getMessage().contains("--undefined_var"));
    }

    @Test
    void testEmptyVariableValue() {
        String[] args = {
            "--config", "{\"job\":{\"name\":\"test\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"jdbc\",\"outputTable\":\"t\",\"config\":{\"url\":\"${db_url}\"}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}",
            "--db_url", ""
        };

        JobConfig config = CliArgumentParser.parse(args);

        assertEquals("", config.getSources().get(0).getConfig().get("url"));
    }
}