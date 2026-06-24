package com.etl.connector.cdc.mysql;

import com.etl.connector.cdc.mysql.config.MySqlCdcConfig;
import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.util.DebeziumJsonUtil;
import com.etl.core.util.JsonUtil;
import com.etl.core.util.MetadataUtil;
import com.etl.core.util.SqlUtil;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL CDC 序列化器
 * 将 Debezium JSON 转换为带 RowKind 的 Row
 * 动态从数据库获取表 Schema
 */
public class MySqlCdcDeserializer implements DebeziumDeserializationSchema<Row> {

    private final EtlSchema etlSchema;
    private transient JsonConverter jsonConverter;

    public MySqlCdcDeserializer(MySqlCdcConfig cdcConfig) {
        String username = cdcConfig.getUsername();
        String password = cdcConfig.getPassword();
        String table = cdcConfig.getTable();

        RowTypeInfo rowTypeInfo = (RowTypeInfo) SqlUtil.inferRowType(
                table,
                null,
                cdcConfig.getUrl(),
                username,
                password
        );

        etlSchema = MetadataUtil.addSourceToSchema(rowTypeInfo);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Row> out) throws Exception {
        // 延迟初始化，因为jsonConverter不能序列化
        if (jsonConverter == null) {
            initializeJsonConverter();
        }

        // 从 SourceRecord 中提取 JSON 字节流
        byte[] valueBytes = jsonConverter.fromConnectData(
                record.topic(),
                record.valueSchema(),
                record.value()
        );

        // 解析 JSON 字符串
        String jsonString = new String(valueBytes, StandardCharsets.UTF_8);
        JsonNode debeziumJsonNode = JsonUtil.readTree(jsonString);

        // 验证必需字段 'op'
        if (!debeziumJsonNode.has("op")) {
            throw new IOException("Debezium JSON 缺少必需字段 'op'");
        }

        // 解析 Debezium op 字段
        String op = debeziumJsonNode.get("op").asText();
        RowKind rowKind = parseRowKind(op);

        // 提取业务数据（after/before 字段）并进行验证
        JsonNode dataNode;
        if (op.equals("d")) {
            // DELETE 操作使用 before 字段
            if (!debeziumJsonNode.has("before") || debeziumJsonNode.get("before").isNull()) {
                throw new IOException("DELETE 操作缺少 'before' 字段");
            }
            dataNode = debeziumJsonNode.get("before");
        } else {
            // INSERT/UPDATE 操作使用 after 字段
            if (!debeziumJsonNode.has("after") || debeziumJsonNode.get("after").isNull()) {
                throw new IOException("INSERT/UPDATE 操作缺少 'after' 字段");
            }
            dataNode = debeziumJsonNode.get("after");
        }

        // 构建 Row（带 RowKind）
        String source = DebeziumJsonUtil.getSourceFromJsonNode(debeziumJsonNode);
        Row row = JsonToRowConverter.convertJsonToRow(
                dataNode,
                etlSchema,
                Collections.singletonMap(MetadataUtil.SOURCE, source)
        );

        row.setKind(rowKind);

        // 发送到下游
        out.collect(row);
    }

    private RowKind parseRowKind(String op) {
        switch (op) {
            case "c":  // create
            case "r":  // read（快照读取）
                return RowKind.INSERT;
            case "u":  // update
                return RowKind.UPDATE_AFTER;
            case "d":  // delete
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException("不支持的 op 类型: " + op);
        }
    }

    /**
     * 初始化 JsonConverter
     * 用于将 Kafka Connect Struct 转换为 JSON
     */
    private void initializeJsonConverter() {
        jsonConverter = new JsonConverter();
        Map<String, Object> configs = new HashMap<>(2);
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);  // 不包含 schema
        jsonConverter.configure(configs);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(etlSchema.getFieldNames(), etlSchema.getFieldTypes());
    }
}