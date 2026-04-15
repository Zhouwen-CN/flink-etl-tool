package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import com.google.auto.service.AutoService;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * Debezium CDC JSON 格式反序列化器插件
 * 解析 Debezium JSON 结构，设置 RowKind，提取 after/before 数据
 */
@AutoService(KafkaFormatPlugin.class)
public class DebeziumJsonFormatPlugin implements KafkaFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "debezium-json";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        return new DebeziumJsonDeserializationSchema(schema);
    }
}