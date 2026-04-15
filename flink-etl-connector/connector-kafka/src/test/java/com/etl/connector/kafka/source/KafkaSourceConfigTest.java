package com.etl.connector.kafka.source;

import com.etl.core.config.SourceConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSourceConfigTest {

    @Test
    void testDefaultFormat() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("groupId", "test-group");
        configMap.put("topics", Arrays.asList("test-topic"));
        configMap.put("schema", createTestSchema());

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("kafka");
        sourceConfig.setOutputTable("test_table");
        sourceConfig.setConfig(configMap);

        KafkaSourceConfig config = KafkaSourceConfig.fromSourceConfig(sourceConfig);

        assertEquals("json", config.getFormatPlugin().identifier());
    }

    @Test
    void testCustomFormat() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("groupId", "test-group");
        configMap.put("topics", Arrays.asList("test-topic"));
        configMap.put("format", "debezium-json");
        configMap.put("schema", createTestSchema());

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("kafka");
        sourceConfig.setOutputTable("test_table");
        sourceConfig.setConfig(configMap);

        KafkaSourceConfig config = KafkaSourceConfig.fromSourceConfig(sourceConfig);

        assertEquals("debezium-json", config.getFormatPlugin().identifier());
    }

    @Test
    void testUnsupportedFormat() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("groupId", "test-group");
        configMap.put("topics", Arrays.asList("test-topic"));
        configMap.put("format", "unsupported");
        configMap.put("schema", createTestSchema());

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("kafka");
        sourceConfig.setOutputTable("test_table");
        sourceConfig.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            KafkaSourceConfig.fromSourceConfig(sourceConfig);
        });
    }

    private Map<String, Object> createTestSchema() {
        Map<String, Object> schema = new HashMap<>();
        schema.put("id", "LONG");
        return schema;
    }
}