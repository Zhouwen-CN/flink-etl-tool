package com.etl.core.config;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class TransformConfigTest {

    @Test
    void getString_shouldReturnValue() {
        Map<String, Object> config = new HashMap<>();
        config.put("sql", "SELECT * FROM users");

        TransformConfig transformConfig = new TransformConfig();
        transformConfig.setType("sql");
        transformConfig.setConfig(config);

        assertEquals("SELECT * FROM users", transformConfig.getString("sql"));
    }

    @Test
    void getString_shouldReturnNull_whenKeyNotFound() {
        Map<String, Object> config = new HashMap<>();

        TransformConfig transformConfig = new TransformConfig();
        transformConfig.setConfig(config);

        assertNull(transformConfig.getString("nonexistent"));
    }

    @Test
    void getString_shouldReturnNull_whenConfigIsNull() {
        TransformConfig transformConfig = new TransformConfig();
        transformConfig.setConfig(null);

        assertNull(transformConfig.getString("any"));
    }

    @Test
    void getString_shouldConvertToString() {
        Map<String, Object> config = new HashMap<>();
        config.put("number", 123);

        TransformConfig transformConfig = new TransformConfig();
        transformConfig.setConfig(config);

        assertEquals("123", transformConfig.getString("number"));
    }
}