package com.etl.connector.kafka.source.format.debezium;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.convert.JsonToRowConverter;
import com.etl.core.schema.metadata.Metadata;
import com.etl.core.schema.metadata.MetadataManager;
import com.etl.core.util.CdcJsonUtil;
import com.etl.core.util.JsonUtil;
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

/**
 * Debezium JSON 反序列化器
 * 解析 Debezium CDC JSON 结构，设置 RowKind，提取业务数据
 */
@Slf4j
public class DebeziumJsonDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final EtlSchema schema;  // 业务数据 schema

    public DebeziumJsonDeserializationSchema(EtlSchema schema) {
        this.schema = MetadataManager.addMetadata(schema);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            return;
        }

        JsonNode debeziumJsonNode = JsonUtil.readTree(record.value());

        // 解析操作类型
        JsonNode opNode = debeziumJsonNode.get("op");
        if (opNode == null || opNode.isNull()) {
            log.warn("Debezium Json 缺少 op 字段: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }
        String op = opNode.asText();
        RowKind rowKind = CdcJsonUtil.parseDebeziumOp(op);
        if (rowKind == null) {
            log.warn("Debezium Json 不支持的 op 类型: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        // 获取 data
        JsonNode dataNode;
        if (rowKind == RowKind.DELETE) {
            dataNode = debeziumJsonNode.get("before");
        } else {
            dataNode = debeziumJsonNode.get("after");
        }
        if (dataNode == null || dataNode.isNull()) {
            // before/after 可能为 null（某些场景）
            log.warn("Debezium Json 缺少 before/after: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        // 添加source到row
        String table = CdcJsonUtil.getDebeziumSource(debeziumJsonNode);
        Row row = JsonToRowConverter.convertJsonToRow(
                dataNode,
                schema,
                Metadata.builder().topic(record.topic()).table(table).build()
        );
        row.setKind(rowKind);
        out.collect(row);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}