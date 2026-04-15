package com.etl.source.kafka.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumJsonDeserializationSchemaTest {

    @Test
    void testInsertOperation() throws Exception {
        // 准备 schema
        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        // 准备 Debezium INSERT 数据
        String debeziumJson = "{\"op\":\"c\",\"ts_ms\":1234567890,\"after\":{\"id\":1,\"name\":\"Alice\"}}";

        DebeziumJsonDeserializationSchema deserializer =
            new DebeziumJsonDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            "test-topic", 0, 0, null, debeziumJson.getBytes(StandardCharsets.UTF_8)
        );

        // Mock Collector
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

        // 验证结果
        assertEquals(1, collectedRows.size());
        Row row = collectedRows.get(0);
        assertEquals(RowKind.INSERT, row.getKind());
        assertEquals(1L, row.getField(0));  // id 字段
        assertEquals("Alice", row.getField(1));  // name 字段
    }

    @Test
    void testUpdateOperation() throws Exception {
        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        String debeziumJson = "{\"op\":\"u\",\"before\":{\"id\":1,\"name\":\"Old\"},\"after\":{\"id\":1,\"name\":\"New\"}}";

        DebeziumJsonDeserializationSchema deserializer =
            new DebeziumJsonDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            "test-topic", 0, 0, null, debeziumJson.getBytes(StandardCharsets.UTF_8)
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
        assertEquals(RowKind.UPDATE_AFTER, row.getKind());
        assertEquals(1L, row.getField(0));  // id 字段
        assertEquals("New", row.getField(1));  // name 字段，使用 after 数据
    }

    @Test
    void testDeleteOperation() throws Exception {
        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        String debeziumJson = "{\"op\":\"d\",\"before\":{\"id\":1,\"name\":\"Old\"},\"after\":null}";

        DebeziumJsonDeserializationSchema deserializer =
            new DebeziumJsonDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            "test-topic", 0, 0, null, debeziumJson.getBytes(StandardCharsets.UTF_8)
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
        assertEquals(RowKind.DELETE, row.getKind());
        assertEquals(1L, row.getField(0));  // id 字段
        assertEquals("Old", row.getField(1));  // name 字段，DELETE 使用 before 数据
    }
}