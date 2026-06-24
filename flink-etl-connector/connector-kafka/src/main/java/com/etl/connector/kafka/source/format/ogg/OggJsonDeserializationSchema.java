package com.etl.connector.kafka.source.format.ogg;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.util.JsonUtil;
import com.etl.core.util.MetadataUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Collections;

@Slf4j
public class OggJsonDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private final EtlSchema schema;

    public OggJsonDeserializationSchema(EtlSchema schema) {
        this.schema = MetadataUtil.addSourceToSchema(schema);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            return;
        }

        // 解析 Debezium JSON
        JsonNode oggJsonNode = JsonUtil.readTree(record.value());

        // 提取操作类型
        JsonNode opTypeNode = oggJsonNode.get("op_type");
        if (opTypeNode == null || opTypeNode.isNull()) {
            log.warn("OGG 消息缺少 op_type 字段，跳过该记录: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        String opType = opTypeNode.asText();
        RowKind rowKind = mapOpToRowKind(opType);
        if (rowKind == null) {
            log.warn("不支持的 op_type 类型: {}", opType);
            return;
        }

        JsonNode dataNode;
        if (rowKind == RowKind.DELETE) {
            dataNode = oggJsonNode.get("before");
        } else {
            dataNode = oggJsonNode.get("after");
        }

        if (dataNode == null || dataNode.isNull()) {
            log.warn("OGG 消息缺少 before/after: {}", oggJsonNode);
            return;
        }

        // 添加source到row
        JsonNode table = oggJsonNode.get("table");
        String source = table == null ? null : table.asText();

        Row row = JsonToRowConverter.convertJsonToRow(
                dataNode,
                schema,
                Collections.singletonMap(MetadataUtil.SOURCE, source)
        );
        row.setKind(rowKind);
        out.collect(row);
    }

    /**
     * Debezium op 字段映射到 Flink RowKind
     */
    private RowKind mapOpToRowKind(String opType) {
        switch (opType) {
            case "I":  // insert
                return RowKind.INSERT;
            case "U":  // update
                return RowKind.UPDATE_AFTER;
            case "D":  // delete
                return RowKind.DELETE;
            default: // "T" truncate
                return null;
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}
