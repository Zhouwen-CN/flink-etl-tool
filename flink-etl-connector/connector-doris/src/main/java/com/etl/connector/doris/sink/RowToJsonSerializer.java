package com.etl.connector.doris.sink;

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
    private final boolean isMappingMode;

    public RowToJsonSerializer(Map<String, String> tableMapping) {
        this.tableMapping = tableMapping;
        // 配置已经校验，如果mapping不为空，说明是mapping模式
        this.isMappingMode = !tableMapping.isEmpty();
    }


    @Override
    public DorisRecord serialize(Row row) throws IOException {
        String source = null;
        if (isMappingMode) {
            // 删除 source，并获取
            Pair<Row, String> pair = MetadataUtil.removeSource(row);
            row = pair.getKey();
            source = pair.getValue();
        }

        // 转 json
        int sign = row.getKind() == RowKind.DELETE ? 1 : 0;
        JsonNode node = RowToJsonConverter.convertRowToJsonNode(row);
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.put(LoadConstants.DORIS_DELETE_SIGN, sign);
        }
        String json = JsonUtil.writeValueAsString(node);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        // 序列化
        if (isMappingMode) {
            String tableIdentifier = tableMapping.get(source);

            // 如果未找到表映射，直接返回null，数据会被过滤
            if (tableIdentifier == null) {
                log.warn("未找到表映射，请正确配置 tableMapping: {}", source);
                return null;
            }
            return DorisRecord.of(tableIdentifier, bytes);
        } else {
            return DorisRecord.of(bytes);
        }
    }
}
