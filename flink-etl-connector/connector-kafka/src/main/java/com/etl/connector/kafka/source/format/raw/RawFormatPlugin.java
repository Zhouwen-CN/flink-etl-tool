package com.etl.connector.kafka.source.format.raw;

import com.etl.connector.kafka.source.format.KafkaFormatPlugin;
import com.etl.core.schema.EtlSchema;
import com.google.auto.service.AutoService;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

@AutoService(KafkaFormatPlugin.class)
public class RawFormatPlugin implements KafkaFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "raw";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        if (schema.getFieldCount() != 1) {
            throw new IllegalArgumentException(
                    "raw format requires exactly one STRING field, but got " + schema.getFieldCount() + " fields");
        }
        if (schema.getFieldType(0) != Types.STRING) {
            throw new IllegalArgumentException(
                    "raw format requires the field type to be STRING, but got " + schema.getFieldType(0));
        }
        return new RawDeserializationSchema(schema);
    }
}