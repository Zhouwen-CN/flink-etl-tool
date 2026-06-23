package com.etl.connector.doris.sink.format.json;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.etl.core.schema.RowToJsonConverter;
import com.etl.core.util.JsonUtil;
import com.etl.core.util.MetadataUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.doris.flink.sink.writer.LoadConstants;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Row 到 Doris JSON 字节的序列化器
 */
@Slf4j
public class RowToJsonSerializer implements DorisRecordSerializer<Row> {

    private static final long serialVersionUID = 1L;
    private final Map<String, String> tableMapping;

    public RowToJsonSerializer(DorisSinkConfig config) {
        tableMapping = config.getTableMapping();
    }


    @Override
    public DorisRecord serialize(Row row) throws IOException {
        Pair<Row, String> pair = MetadataUtil.removeSource(row);
        String source = pair.getValue();

        String tableIdentifier = tableMapping.get(source);

        if (tableIdentifier == null) {
            log.warn("未找到表映射，请正确配置 tableMapping: {}", source);
            return null;
        }

        row = pair.getKey();

        int sign = row.getKind() == RowKind.DELETE ? 1 : 0;
        JsonNode node = RowToJsonConverter.convertRowToJsonNode(row);
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.put(LoadConstants.DORIS_DELETE_SIGN, sign);
        }
        String json = JsonUtil.writeValueAsString(node);

        return DorisRecord.of(tableIdentifier, json.getBytes(StandardCharsets.UTF_8));
    }
}
