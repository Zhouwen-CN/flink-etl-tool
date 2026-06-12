package com.etl.connector.doris.sink.format;

import com.etl.core.schema.RowToJsonConverter;
import com.etl.core.utils.JsonUtils;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Row 到 Doris JSON 字节的序列化器
 * 复用 RowToJsonConverter，输出每行一个 JSON 对象（配合 read_json_by_line=true）
 */
public class RowToDorisJsonSerializer implements DorisRecordSerializer<Row> {

    private static final long serialVersionUID = 1L;
    private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";


    @Override
    public DorisRecord serialize(Row row) throws IOException {
        int sign = row.getKind() == RowKind.DELETE ? 1 : 0;
        JsonNode node = RowToJsonConverter.convertRowToJsonNode(row);
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.put(DORIS_DELETE_SIGN, sign);
        }
        String json = JsonUtils.writeValueAsString(node);
        return DorisRecord.of(json.getBytes(StandardCharsets.UTF_8));
    }
}
