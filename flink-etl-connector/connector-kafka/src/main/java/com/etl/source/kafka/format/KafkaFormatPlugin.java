package com.etl.source.kafka.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Kafka 消息格式反序列化器 SPI 接口
 * 定义在 Kafka Source 模块内部，不污染 core 模块
 */
public interface KafkaFormatPlugin extends Serializable {

    /**
     * Format 标识符
     *
     * @return 格式名称，如 "json", "debezium-json", "ogg-json"
     */
    String identifier();

    /**
     * 创建反序列化器
     *
     * @param schema 业务数据的 schema（before/after 的数据结构，不包括 CDC 元数据）
     * @return KafkaRecordDeserializationSchema 实例
     */
    KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema);
}