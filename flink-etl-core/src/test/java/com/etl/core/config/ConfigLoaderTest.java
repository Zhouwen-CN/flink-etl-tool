package com.etl.core.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConfigLoader 测试
 */
class ConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void testLoadFromFile() throws Exception {
        File configFile = tempDir.resolve("test-config.json").toFile();
        String jsonContent = "{\n" +
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
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write(jsonContent);
        }

        JobConfig config = ConfigLoader.loadFromFile(configFile.getAbsolutePath());

        assertNotNull(config);
        assertEquals("test-job", config.getJob().getName());
        assertEquals("batch", config.getJob().getMode());
    }

    @Test
    void testLoadFromFileNotFound() {
        String nonExistentPath = "/non/existent/path/config.json";

        assertThrows(IllegalArgumentException.class, () -> {
            ConfigLoader.loadFromFile(nonExistentPath);
        });
    }

    @Test
    void testLoadFromJsonString() {
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

        JobConfig config = ConfigLoader.loadFromJsonString(json);

        assertNotNull(config);
        assertEquals("test-job", config.getJob().getName());
    }

    @Test
    void testLoadFromJsonStringInvalid() {
        String invalidJson = "{ invalid json }";

        assertThrows(IllegalArgumentException.class, () -> {
            ConfigLoader.loadFromJsonString(invalidJson);
        });
    }
}