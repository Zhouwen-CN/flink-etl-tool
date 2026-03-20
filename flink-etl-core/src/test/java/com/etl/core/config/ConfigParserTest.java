package com.etl.core.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConfigParser 单元测试
 */
class ConfigParserTest {

    @Test
    void testParseValidMultiSourceConfig() {
        String json = "{\n" +
            "  \"job\": { \"name\": \"test\", \"mode\": \"batch\" },\n" +
            "  \"sources\": [\n" +
            "    { \"type\": \"mysql\", \"outputTable\": \"users\", \"config\": {} }\n" +
            "  ],\n" +
            "  \"sinks\": [\n" +
            "    { \"type\": \"console\", \"inputTable\": \"users\", \"config\": {} }\n" +
            "  ]\n" +
            "}";

        JobConfig config = ConfigParser.parseFromString(json);

        assertNotNull(config);
        assertEquals("test", config.getJob().getName());
        assertEquals(1, config.getSources().size());
        assertEquals("mysql", config.getSources().get(0).getType());
        assertEquals("users", config.getSources().get(0).getOutputTable());
        assertEquals(1, config.getSinks().size());
        assertEquals("console", config.getSinks().get(0).getType());
        assertEquals("users", config.getSinks().get(0).getInputTable());
    }

    @Test
    void testParseMultipleSourcesAndSinks() {
        String json = "{\n" +
            "  \"job\": { \"name\": \"multi\", \"mode\": \"batch\" },\n" +
            "  \"sources\": [\n" +
            "    { \"type\": \"mysql\", \"outputTable\": \"users\", \"config\": {} },\n" +
            "    { \"type\": \"mysql\", \"outputTable\": \"orders\", \"config\": {} }\n" +
            "  ],\n" +
            "  \"sinks\": [\n" +
            "    { \"type\": \"console\", \"inputTable\": \"users\", \"config\": {} },\n" +
            "    { \"type\": \"mysql\", \"inputTable\": \"orders\", \"config\": {} }\n" +
            "  ]\n" +
            "}";

        JobConfig config = ConfigParser.parseFromString(json);

        assertEquals(2, config.getSources().size());
        assertEquals(2, config.getSinks().size());
    }

    @Test
    void testMissingSourcesThrowsException() {
        String json = "{\n" +
            "  \"job\": { \"name\": \"test\", \"mode\": \"batch\" },\n" +
            "  \"sinks\": [\n" +
            "    { \"type\": \"console\", \"inputTable\": \"users\", \"config\": {} }\n" +
            "  ]\n" +
            "}";

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ConfigParser.parseFromString(json)
        );
        assertTrue(ex.getMessage().contains("缺少 sources"));
    }

    @Test
    void testMissingSinksThrowsException() {
        String json = "{\n" +
            "  \"job\": { \"name\": \"test\", \"mode\": \"batch\" },\n" +
            "  \"sources\": [\n" +
            "    { \"type\": \"mysql\", \"outputTable\": \"users\", \"config\": {} }\n" +
            "  ]\n" +
            "}";

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ConfigParser.parseFromString(json)
        );
        assertTrue(ex.getMessage().contains("缺少 sinks"));
    }

    @Test
    void testDuplicateOutputTableThrowsException() {
        String json = "{\n" +
            "  \"job\": { \"name\": \"test\", \"mode\": \"batch\" },\n" +
            "  \"sources\": [\n" +
            "    { \"type\": \"mysql\", \"outputTable\": \"users\", \"config\": {} },\n" +
            "    { \"type\": \"mysql\", \"outputTable\": \"users\", \"config\": {} }\n" +
            "  ],\n" +
            "  \"sinks\": [\n" +
            "    { \"type\": \"console\", \"inputTable\": \"users\", \"config\": {} }\n" +
            "  ]\n" +
            "}";

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ConfigParser.parseFromString(json)
        );
        assertTrue(ex.getMessage().contains("outputTable 重复"));
    }
}