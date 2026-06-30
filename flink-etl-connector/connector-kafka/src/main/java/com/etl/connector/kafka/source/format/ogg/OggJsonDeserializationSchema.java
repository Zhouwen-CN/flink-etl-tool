package com.etl.connector.kafka.source.format.ogg;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.util.CdcJsonUtil;
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

        JsonNode oggJsonNode = JsonUtil.readTree(record.value());

        // 解析操作类型
        JsonNode opTypeNode = oggJsonNode.get("op_type");
        if (opTypeNode == null || opTypeNode.isNull()) {
            log.warn("OGG Json 缺少 op_type 字段: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }
        String opType = opTypeNode.asText();
        RowKind rowKind = CdcJsonUtil.parseOggOp(opType);
        if (rowKind == null) {
            log.warn("OGG Json 不支持的 op_type 类型: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        // 获取 data
        JsonNode dataNode;
        if (rowKind == RowKind.DELETE) {
            dataNode = oggJsonNode.get("before");
        } else {
            dataNode = oggJsonNode.get("after");
        }

        if (dataNode == null || dataNode.isNull()) {
            log.warn("OGG Json 缺少 before/after: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        // 添加source到row
        String source = CdcJsonUtil.getOggSource(oggJsonNode);
        Row row = JsonToRowConverter.convertJsonToRow(
                dataNode,
                schema,
                Collections.singletonMap(MetadataUtil.SOURCE, source)
        );
        row.setKind(rowKind);
        out.collect(row);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}
