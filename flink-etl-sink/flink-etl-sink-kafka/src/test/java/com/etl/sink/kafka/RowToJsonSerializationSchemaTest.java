package com.etl.sink.kafka;

import com.etl.core.config.SinkConfig;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class RowToJsonSerializationSchemaTest {

    private KafkaSinkConfig kafkaConfig;
    private KafkaRecordSerializationSchema.KafkaSinkContext mockContext;

    @BeforeEach
    void setUp() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setConfig(configMap);
        kafkaConfig = KafkaSinkConfig.fromSinkConfig(sinkConfig);
        mockContext = mock(KafkaRecordSerializationSchema.KafkaSinkContext.class);
    }

    @Test
    void testSerialize_WithoutKey() throws Exception {
        // 创建序列化器
        RowToJsonSerializationSchema schema = new RowToJsonSerializationSchema(kafkaConfig);
        schema.open(mockContext);

        // 创建 Row
        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("name", "张三");

        // 序列化
        ProducerRecord<byte[], byte[]> record = schema.serialize(row, mockContext, null);

        // 验证
        assertNotNull(record);
        assertEquals("test-topic", record.topic());
        assertNull(record.key());

        // 验证 value
        byte[] value = record.value();
        assertNotNull(value);
        String json = new String(value, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"id\":1"));
        assertTrue(json.contains("\"name\":\"张三\""));
    }

    @Test
    void testSerialize_WithKey() throws Exception {
        // 配置 keyField
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");
        configMap.put("keyField", "userId");

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setConfig(configMap);
        KafkaSinkConfig configWithKey = KafkaSinkConfig.fromSinkConfig(sinkConfig);

        // 创建序列化器
        RowToJsonSerializationSchema schema = new RowToJsonSerializationSchema(configWithKey);
        schema.open(mockContext);

        // 创建 Row
        Row row = Row.withNames();
        row.setField("userId", "user123");
        row.setField("event", "click");

        // 序列化
        ProducerRecord<byte[], byte[]> record = schema.serialize(row, mockContext, null);

        // 验证 key
        assertNotNull(record.key());
        String key = new String(record.key(), StandardCharsets.UTF_8);
        assertEquals("user123", key);

        // 验证 value
        assertNotNull(record.value());
    }
}