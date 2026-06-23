package com.etl.connector.kafka.source.format;

import com.etl.connector.kafka.source.format.raw.RawDeserializationSchema;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RawDeserializationSchemaTest {

    @Test
    void testDeserializeNormalMessage() throws Exception {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawDeserializationSchema deserializer = new RawDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, null, "hello world".getBytes(StandardCharsets.UTF_8)
        );

        List<Row> collectedRows = new ArrayList<>();
        Collector<Row> collector = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                collectedRows.add(record);
            }
            @Override
            public void close() {}
        };

        deserializer.deserialize(record, collector);

        assertEquals(1, collectedRows.size());
        Row row = collectedRows.get(0);
        assertEquals("hello world", row.getField(0));
    }

    @Test
    void testDeserializeNullValue() throws Exception {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawDeserializationSchema deserializer = new RawDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, null, null
        );

        List<Row> collectedRows = new ArrayList<>();
        Collector<Row> collector = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                collectedRows.add(record);
            }
            @Override
            public void close() {}
        };

        deserializer.deserialize(record, collector);

        assertEquals(1, collectedRows.size());
        Row row = collectedRows.get(0);
        assertNull(row.getField(0));
    }

    @Test
    void testDeserializeEmptyBytes() throws Exception {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawDeserializationSchema deserializer = new RawDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, null, new byte[0]
        );

        List<Row> collectedRows = new ArrayList<>();
        Collector<Row> collector = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                collectedRows.add(record);
            }
            @Override
            public void close() {}
        };

        deserializer.deserialize(record, collector);

        assertEquals(1, collectedRows.size());
        Row row = collectedRows.get(0);
        assertEquals("", row.getField(0));
    }

    @Test
    void testGetProducedType() {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawDeserializationSchema deserializer = new RawDeserializationSchema(schema);

        TypeInformation<Row> producedType = deserializer.getProducedType();
        assertEquals(Types.ROW_NAMED(fieldNames, fieldTypes), producedType);
    }
}