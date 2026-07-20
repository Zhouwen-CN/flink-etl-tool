/*
package com.etl.connector.doris.sink.v1;

import com.etl.core.schema.convert.RowToJsonConverter;
import com.etl.core.schema.metadata.MetadataManager;
import com.etl.core.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.flink.sink.writer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.LoadConstants;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

*/
/**
 * Row 到 Doris JSON 字节的序列化器
 * 适用于 flink-doris-connector-1.15:1.4.0
 *//*

@Slf4j
public class RowToJsonSerializer implements DorisRecordSerializer<Row> {

    private static final long serialVersionUID = 1L;


    @Override
    public byte[] serialize(Row row) throws IOException {
        row = MetadataManager.removeMetadata(row);

        // 转 json
        int sign = row.getKind() == RowKind.DELETE ? 1 : 0;
        JsonNode node = RowToJsonConverter.convertRowToJsonNode(row);
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.put(LoadConstants.DORIS_DELETE_SIGN, sign);
        }
        String json = JsonUtil.writeValueAsString(node);
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
*/
