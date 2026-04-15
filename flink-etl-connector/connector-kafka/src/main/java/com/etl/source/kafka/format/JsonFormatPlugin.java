package com.etl.source.kafka.format;

import com.etl.core.schema.EtlSchema;
import com.google.auto.service.AutoService;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * 标准 JSON 格式反序列化器插件
 * 将 Kafka 消息 JSON 直接解析为 Row，RowKind 默认为 INSERT
 */
@AutoService(KafkaFormatPlugin.class)
public class JsonFormatPlugin implements KafkaFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "json";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        return new JsonToRowDeserializationSchema(schema);
    }
}