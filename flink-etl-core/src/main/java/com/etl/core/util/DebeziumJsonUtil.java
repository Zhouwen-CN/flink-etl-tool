package com.etl.core.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;
import java.util.StringJoiner;

public final class DebeziumJsonUtil {

    private DebeziumJsonUtil() {
    }

    public static String getSourceFromJsonNode(JsonNode jsonNode) {
        StringJoiner identifier = new StringJoiner(".");
        JsonNode source = jsonNode.get("source");
        if (source == null) {
            return null;
        }

        Optional.ofNullable(source.get("db")).ifPresent(item -> identifier.add(item.asText()));
        Optional.ofNullable(source.get("schema")).ifPresent(item -> identifier.add(item.asText()));
        Optional.ofNullable(source.get("table")).ifPresent(item -> identifier.add(item.asText()));

        String result = identifier.toString();
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        return result;
    }
}
