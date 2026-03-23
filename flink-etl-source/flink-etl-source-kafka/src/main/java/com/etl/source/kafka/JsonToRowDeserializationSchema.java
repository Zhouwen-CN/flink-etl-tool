package com.etl.source.kafka;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.TypeConverter;
import com.etl.core.utils.JsonUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * JSON 到 Row 的反序列化器
 * 将 Kafka 消息的 value 解析为 JsonNode，然后转换为 Flink Row
 */
public class JsonToRowDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    /** 隐藏字段名，用于存储消息来源 Topic */
    public static final String TOPIC_FIELD = "__topic__";

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
        JsonNode jsonNode = JsonUtils.readTree(record.value());

        // 使用 TypeConverter.convertJsonToRows 方法，支持 JSONObject 和 JSONArray
        List<Row> rows = TypeConverter.convertJsonToRows(jsonNode, schema);

        // 为每个 Row 添加 __topic__ 隐藏字段
        String topic = record.topic();
        for (Row row : rows) {
            Row rowWithTopic = appendTopicField(row, topic);
            out.collect(rowWithTopic);
        }
    }

    /**
     * 在 Row 末尾追加 __topic__ 字段
     *
     * @param row   原始 Row
     * @param topic Topic 名称
     * @return 追加了 __topic__ 字段的新 Row
     */
    private Row appendTopicField(Row row, String topic) {
        int fieldCount = row.getArity();
        Row newRow = Row.withPositions(fieldCount + 1);

        // 复制原有字段
        for (int i = 0; i < fieldCount; i++) {
            newRow.setField(i, row.getField(i));
        }

        // 追加 __topic__ 字段
        newRow.setField(fieldCount, topic);
        return newRow;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        // 在原有 schema 字段基础上，追加 __topic__ 字段
        String[] fieldNames = schema.getFieldNames();
        TypeInformation<?>[] fieldTypes = schema.getFieldTypes();

        String[] newFieldNames = Arrays.copyOf(fieldNames, fieldNames.length + 1);
        newFieldNames[fieldNames.length] = TOPIC_FIELD;

        TypeInformation<?>[] newFieldTypes = Arrays.copyOf(fieldTypes, fieldTypes.length + 1);
        newFieldTypes[fieldTypes.length] = Types.STRING;

        return Types.ROW_NAMED(newFieldNames, newFieldTypes);
    }
}