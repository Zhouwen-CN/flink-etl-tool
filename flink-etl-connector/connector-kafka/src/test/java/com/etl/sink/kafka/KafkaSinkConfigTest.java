package com.etl.sink.kafka;

import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaSinkConfigTest {

    @Test
    void testFromSinkConfig_ValidConfig() {
        // 准备配置
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setConfig(configMap);

        // 解析配置
        KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(sinkConfig);

        // 验证
        assertNotNull(kafkaConfig);
        assertEquals("localhost:9092", kafkaConfig.getBootstrapServers());
        assertEquals("test-topic", kafkaConfig.getTopic());
        assertNull(kafkaConfig.getKeyField());
        assertNotNull(kafkaConfig.getKafkaProperties());
    }

    @Test
    void testFromSinkConfig_WithKeyField() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");
        configMap.put("keyField", "userId");

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setConfig(configMap);
        KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(sinkConfig);

        assertEquals("userId", kafkaConfig.getKeyField());
    }

    @Test
    void testFromSinkConfig_WithProperties() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");

        Map<String, Object> properties = new HashMap<>();
        properties.put("acks", "all");
        properties.put("retries", "3");
        configMap.put("properties", properties);

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setConfig(configMap);
        KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(sinkConfig);

        Properties kafkaProps = kafkaConfig.getKafkaProperties();
        assertNotNull(kafkaProps);
        assertEquals("all", kafkaProps.getProperty("acks"));
        assertEquals("3", kafkaProps.getProperty("retries"));
    }

    @Test
    void testFromSinkConfig_MissingBootstrapServers() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("topic", "test-topic");

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            KafkaSinkConfig.fromSinkConfig(sinkConfig);
        });
    }

    @Test
    void testFromSinkConfig_MissingTopic() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            KafkaSinkConfig.fromSinkConfig(sinkConfig);
        });
    }
}