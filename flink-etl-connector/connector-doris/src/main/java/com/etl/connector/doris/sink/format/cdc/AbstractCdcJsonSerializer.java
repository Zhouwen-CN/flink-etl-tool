package com.etl.connector.doris.sink.format.cdc;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.etl.core.util.JsonUtil;
import lombok.val;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

import java.io.IOException;

public abstract class AbstractCdcJsonSerializer implements DorisRecordSerializer<Row> {

    private final JsonDebeziumSchemaSerializer serializer;

    public AbstractCdcJsonSerializer(DorisSinkConfig config){

        // Doris 连接配置
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(config.getFenodes())
                .setTableIdentifier(config.getTable())
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .build();

        serializer = JsonDebeziumSchemaSerializer.builder()
                .setDorisOptions(dorisOptions)
                .setTableMapping(config.getTableMapping())
                .build();
    }

    @Override
    public DorisRecord serialize(Row record) throws IOException {
        // row 必须只有一个字段
        if (record.getArity() != 1) {
            throw new IOException("Invalid record arity");
        }

        // 这个字段必须是string类型
        Object field = record.getField(0);
        if (!(field instanceof String)) {
            throw new  IOException("Invalid record field type");
        }

        // row必须是jsonObject
        val json = (String) field;
        JsonNode jsonNode;
        try {
            jsonNode = JsonUtil.readTree(json);
        }catch (Exception e){
            throw new IOException("Parse error");
        }
        if (!jsonNode.isObject()) {
            throw new IOException("Invalid record field type");
        }

        // 转换成 DebeziumJson 格式
        ObjectNode objectNode = toDebeziumJson((ObjectNode) jsonNode);
        // return null 的数据会被过滤
        if (objectNode == null) {
            return null;
        }

        return serializer.serialize(objectNode.toString());
    }

    protected abstract ObjectNode toDebeziumJson(ObjectNode objectNode);
}
