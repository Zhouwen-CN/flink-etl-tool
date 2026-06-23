package com.etl.connector.kafka.sink;

import com.etl.connector.kafka.sink.config.KafkaSinkConfig;
import com.etl.core.schema.RowToJsonConverter;
import com.etl.core.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Row 到 Kafka 消息的 JSON 序列化器
 */
@Slf4j
public class RowToJsonSerializationSchema implements KafkaRecordSerializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final KafkaSinkConfig config;

    public RowToJsonSerializationSchema(KafkaSinkConfig config) {
        this.config = config;
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
        // Row -> JsonNode
        JsonNode jsonNode = RowToJsonConverter.convertRowToJsonNode(row);

        // JsonNode -> JSON 字符串（JsonUtils 内部已处理 JsonProcessingException）
        String jsonString = JsonUtil.writeValueAsString(jsonNode);

        // 转为 bytes
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }
}