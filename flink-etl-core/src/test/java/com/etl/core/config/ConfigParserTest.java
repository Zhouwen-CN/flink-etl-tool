package com.etl.core.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConfigParser 测试
 */
class ConfigParserTest {

    @Test
    void testParseFromString() {
        String json = "{\n" +
                "  \"job\": {\n" +
                "    \"name\": \"test-job\",\n" +
                "    \"mode\": \"batch\"\n" +
                "  },\n" +
                "  \"source\": {\n" +
                "    \"type\": \"mysql\",\n" +
                "    \"config\": {}\n" +
                "  },\n" +
                "  \"sink\": {\n" +
                "    \"type\": \"console\",\n" +
                "    \"config\": {}\n" +
                "  }\n" +
                "}";

        JobConfig config = ConfigParser.parseFromString(json);

        assertNotNull(config);
        assertEquals("test-job", config.getJob().getName());
        assertEquals("batch", config.getJob().getMode());
        assertEquals("mysql", config.getSource().getType());
        assertEquals("console", config.getSink().getType());
    }

    @Test
    void testParseFromStringInvalidJson() {
        String invalidJson = "{ invalid json }";

        assertThrows(IllegalArgumentException.class, () -> {
            ConfigParser.parseFromString(invalidJson);
        });
    }
}