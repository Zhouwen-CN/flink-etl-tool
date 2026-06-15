package com.etl.connector.doris.sink.format.cdc;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashSet;
import java.util.Set;

public class DebeziumJsonSerializerImpl extends AbstractCdcJsonSerializer {

    private static final Set<String> OP =  new HashSet<>();

    static {
        OP.add("c");
        OP.add("r");
        OP.add("u");
        OP.add("d");
    }

    public DebeziumJsonSerializerImpl(DorisSinkConfig config) {
        super(config);
    }

    @Override
    protected ObjectNode toDebeziumJson(ObjectNode objectNode) {
        val jsonNode = objectNode.get("op");

        if (jsonNode == null || !OP.contains(jsonNode.asText())) {
            return null;
        }
        return objectNode;
    }
}
