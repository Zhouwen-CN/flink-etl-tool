package com.etl.connector.kafka.source.format.cdc;

import com.etl.connector.kafka.source.format.KafkaFormatPlugin;
import com.etl.core.schema.EtlSchema;
import com.google.auto.service.AutoService;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * 兼容 debezium、ogg、yingfang
 */
@AutoService(KafkaFormatPlugin.class)
public class CdcJsonFormatPlugin implements KafkaFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "cdc-json";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        return new CdcJsonDeserializationSchema(schema);
    }
}