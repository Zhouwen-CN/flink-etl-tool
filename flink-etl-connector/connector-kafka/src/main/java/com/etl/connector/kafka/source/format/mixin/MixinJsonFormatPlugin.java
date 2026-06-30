package com.etl.connector.kafka.source.format.mixin;

import com.etl.connector.kafka.source.format.KafkaFormatPlugin;
import com.etl.core.schema.EtlSchema;
import com.google.auto.service.AutoService;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * ogg、custom-ogg、debezium，混合模式
 */
@AutoService(KafkaFormatPlugin.class)
public class MixinJsonFormatPlugin implements KafkaFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "mixin-json";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        return new MixinJsonDeserializationSchema(schema);
    }
}