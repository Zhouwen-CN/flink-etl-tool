package com.etl.connector.doris.sink.format.cdc;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.etl.core.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class OggJsonSerializerImpl extends AbstractCdcJsonSerializer {
    private static final Map<String,String> OP_MAP = new HashMap<>();

    static {
        OP_MAP.put("I","c");
        OP_MAP.put("U","u");
        OP_MAP.put("D","d");
    }

    public OggJsonSerializerImpl(DorisSinkConfig config) {
        super(config);
    }

    @Override
    protected ObjectNode toDebeziumJson(ObjectNode objectNode) {
        JsonNode opType = objectNode.remove("op_type");

        // op type 检查
        if (opType == null) {
            log.warn("op_type is null: {}", objectNode);
            return null;
        }
        String op = OP_MAP.get(opType.asText());
        if (op == null) {
            log.warn("op_type not in {}: {}", OP_MAP.keySet() , objectNode);
            return null;
        }

        objectNode.put("op", op);
        JsonNode table = objectNode.remove("table");
        if (table == null) {
            log.warn("table is null: {}", objectNode);
            return null;
        }

        ObjectNode source = JsonUtils.getMapper().createObjectNode();
        source.put("table", table.asText());
        objectNode.set("source", source);

        return objectNode;
    }
}
