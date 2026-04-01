package com.etl.sink.kafka;

import com.etl.core.schema.TypeConverter;
import com.etl.core.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Row 到 Kafka 消息的 JSON 序列化器
 */
@Slf4j
public class RowToJsonSerializationSchema implements KafkaRecordSerializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final KafkaSinkConfig config;
    private transient ObjectMapper objectMapper;

    public RowToJsonSerializationSchema(KafkaSinkConfig config) {
        this.config = config;
    }

    public void open(KafkaRecordSerializationSchema.KafkaSinkContext context) throws IOException {
        // 创建专用的 ObjectMapper（配置 JSR310 支持）
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        log.info("Kafka Sink 序列化器已初始化: topic={}, keyField={}",
                 config.getTopic(), config.getKeyField());
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Row row, KafkaRecordSerializationSchema.KafkaSinkContext context, Long timestamp) {
        // 1. 序列化 Key
        byte[] key = serializeKey(row);

        // 2. 序列化 Value
        byte[] value = serializeValue(row);

        // 3. 返回 ProducerRecord
        return new ProducerRecord<>(config.getTopic(), null, key, value);
    }

    /**
     * 序列化消息 Key
     */
    private byte[] serializeKey(Row row) {
        String keyField = config.getKeyField();
        if (keyField == null || keyField.isEmpty()) {
            return null;
        }

        // 提取字段值
        Object keyValue = row.getField(keyField);
        if (keyValue == null) {
            log.warn("Key 字段 '{}' 的值为 null", keyField);
            return null;
        }

        // 转为字符串
        String keyString = keyValue.toString();
        return keyString.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 序列化消息 Value
     */
    private byte[] serializeValue(Row row) {
        try {
            // Row -> JsonNode
            JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

            // JsonNode -> JSON 字符串
            String jsonString = objectMapper.writeValueAsString(jsonNode);

            // 转为 bytes
            return jsonString.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("JSON 序列化失败: " + e.getMessage(), e);
        }
    }
}