package com.etl.connector.doris.sink.format;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.etl.core.schema.RowToJsonConverter;
import com.etl.core.utils.JsonUtils;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Row 到 Doris JSON 字节的序列化器
 * 复用 RowToJsonConverter，输出每行一个 JSON 对象（配合 read_json_by_line=true）
 */
public class RowToDorisJsonSerializer implements DorisRecordSerializer<Row> {

    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;

    public RowToDorisJsonSerializer(DorisSinkConfig config) {
        String[] parts = config.getTableIdentifier().split("\\.", 2);
        this.database = parts[0];
        this.table = parts[1];
    }

    @Override
    public DorisRecord serialize(Row row) throws IOException {
        JsonNode node = RowToJsonConverter.convertRowToJsonNode(row);
        String json = JsonUtils.writeValueAsString(node);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return DorisRecord.of(database, table, bytes);
    }
}
