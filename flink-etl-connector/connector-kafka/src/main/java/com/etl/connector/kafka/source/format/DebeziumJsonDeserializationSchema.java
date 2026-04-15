package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.utils.JsonUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.List;

/**
 * Debezium JSON 反序列化器
 * 解析 Debezium CDC JSON 结构，设置 RowKind，提取业务数据
 */
public class DebeziumJsonDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final EtlSchema schema;  // 业务数据 schema

    public DebeziumJsonDeserializationSchema(EtlSchema schema) {
        this.schema = schema;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            return;
        }

        // 解析 Debezium JSON
        JsonNode debeziumJson = JsonUtils.readTree(record.value());

        // 提取操作类型
        String op = debeziumJson.get("op").asText();
        RowKind rowKind = mapOpToRowKind(op);

        // 根据操作类型提取数据源
        JsonNode dataNode;
        if ("d".equals(op)) {
            dataNode = debeziumJson.get("before");
        } else {
            dataNode = debeziumJson.get("after");
        }

        if (dataNode == null || dataNode.isNull()) {
            // before/after 可能为 null（某些场景）
            return;
        }

        // 转换为 Row
        List<Row> rows = JsonToRowConverter.convertJsonToRows(dataNode, schema);
        if (rows.isEmpty()) {
            return;
        }
        // cdc before/after 节点不会是array类型
        Row row = rows.get(0);

        row.setKind(rowKind);
        out.collect(row);
    }

    /**
     * Debezium op 字段映射到 Flink RowKind
     */
    private RowKind mapOpToRowKind(String op) {
        switch (op) {
            case "c":  // create
            case "r":  // read (initial snapshot)
                return RowKind.INSERT;
            case "u":  // update
                return RowKind.UPDATE_AFTER;
            case "d":  // delete
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException(
                    String.format("未知的 Debezium op 类型: '%s'，支持的操作: c, r, u, d", op)
                );
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        // 返回业务数据 Row 的类型信息
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}