package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RawDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final EtlSchema schema;

    public RawDeserializationSchema(EtlSchema schema) {
        this.schema = schema;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        Row row = Row.withPositions(schema.getFieldCount());
        if (record.value() != null) {
            row.setField(0, new String(record.value(), StandardCharsets.UTF_8));
        }
        out.collect(row);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}