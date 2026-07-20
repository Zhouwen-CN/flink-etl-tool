package com.etl.connector.doris.sink.v2;

/**
 * Row 到 Doris JSON 字节的序列化器
 * 适用于 flink-doris-connector-1.15:1.5.2，建议使用这个
 */
/*@Slf4j
public class RowToJsonSerializer implements DorisRecordSerializer<Row> {

    private static final long serialVersionUID = 1L;


    @Override
    public DorisRecord serialize(Row row) throws IOException {
        row = MetadataManager.removeMetadata(row);

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
        return DorisRecord.of(bytes);
    }
}*/
