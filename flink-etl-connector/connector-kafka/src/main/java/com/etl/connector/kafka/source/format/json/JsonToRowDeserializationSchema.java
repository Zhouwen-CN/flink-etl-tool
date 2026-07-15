package com.etl.connector.kafka.source.format.json;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.convert.JsonToRowConverter;
import com.etl.core.util.JsonUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.List;

/**
 * JSON 到 Row 的反序列化器
 * 将 Kafka 消息的 value 解析为 JsonNode，然后转换为 Flink Row
 */
public class JsonToRowDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final EtlSchema schema;

    /**
     * 构造函数
     *
     * @param schema Schema 定义
     */
    public JsonToRowDeserializationSchema(EtlSchema schema) {
        this.schema = schema;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            return;
        }

        // 解析 JSON
        JsonNode jsonNode = JsonUtil.readTree(record.value());

        // 使用 JsonToRowConverter.convertJsonToRows 方法，支持 JSONObject 和 JSONArray
        List<Row> rows = JsonToRowConverter.convertJsonToRows(jsonNode, schema);

        for (Row row : rows) {
            out.collect(row);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        // 返回业务数据 Row 的类型信息
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}