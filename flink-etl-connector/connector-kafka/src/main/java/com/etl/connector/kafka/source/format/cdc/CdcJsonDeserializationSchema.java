package com.etl.connector.kafka.source.format.cdc;

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

@Slf4j
public class CdcJsonDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private final EtlSchema schema;

    public CdcJsonDeserializationSchema(EtlSchema schema) {
        this.schema = MetadataManager.addMetadata(schema);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            return;
        }

        JsonNode mixinJsonNode = JsonUtil.readTree(record.value());

        RowKind rowKind = null;
        String source = null;

        // 解析操作类型
        // ogg
        if (mixinJsonNode.has("op_type")) {
            rowKind = CdcJsonUtil.parseOggOp(mixinJsonNode.get("op_type").asText());
            source = CdcJsonUtil.getOggSource(mixinJsonNode);
            // custom ogg
        } else if (mixinJsonNode.has("optype")) {
            rowKind = CdcJsonUtil.parseCustomOggOp(mixinJsonNode.get("optype").asText());
            source = CdcJsonUtil.getCustomOggSource(mixinJsonNode);
            // debezium
        } else if (mixinJsonNode.has("op")) {
            rowKind = CdcJsonUtil.parseDebeziumOp(mixinJsonNode.get("op").asText());
            source = CdcJsonUtil.getDebeziumSource(mixinJsonNode);
        }

        if (rowKind == null) {
            log.warn("CDC Json 操作类型解析失败: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        // 获取 data
        JsonNode dataNode;
        if (rowKind == RowKind.DELETE) {
            dataNode = mixinJsonNode.get("before");
        } else {
            dataNode = mixinJsonNode.get("after");
        }

        if (dataNode == null || dataNode.isNull()) {
            log.warn("CDC Json 消息缺少 before/after: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        // 添加 source 到 row
        Row row = JsonToRowConverter.convertJsonToRow(
                dataNode,
                schema,
                Metadata.builder().topic(record.topic()).source(source).build()
        );
        row.setKind(rowKind);
        out.collect(row);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}
