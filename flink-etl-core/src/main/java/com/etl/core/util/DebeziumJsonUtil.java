package com.etl.core.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.RowKind;

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

    /**
     * Debezium op 字段映射到 Flink RowKind
     */
    public static RowKind mapOpToRowKind(String op) {
        switch (op) {
            case "c":  // create
            case "r":  // read (initial snapshot)
                return RowKind.INSERT;
            case "u":  // update
                return RowKind.UPDATE_AFTER;
            case "d":  // delete
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException(
                        String.format("未知的 Debezium op 类型: '%s'，支持的操作: c, r, u, d", op)
                );
        }
    }
}
