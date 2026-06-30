package com.etl.core.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.RowKind;

import java.util.Optional;
import java.util.StringJoiner;

public final class CdcJsonUtil {
    private CdcJsonUtil() {
    }

    /**
     * 解析 ogg op_type
     */
    public static RowKind parseOggOp(String opType) {
        switch (opType) {
            case "I":  // insert
                return RowKind.INSERT;
            case "U":  // update
                return RowKind.UPDATE_AFTER;
            case "D":  // delete
                return RowKind.DELETE;
            default: // "T" truncate
                return null;
        }
    }

    /**
     * 获取 ogg source
     */
    public static String getOggSource(JsonNode jsonNode) {
        if (jsonNode.has("table")) {
            return jsonNode.get("table").asText();
        }
        return null;
    }


    /**
     * 解析 自定义 ogg optype
     */
    public static RowKind parseCustomOggOp(String opType) {
        switch (opType) {
            case "INSERT":
                return RowKind.INSERT;
            case "DELETE":
                return RowKind.DELETE;
            case "UPDATE":
                return RowKind.UPDATE_AFTER;
            default:
                return null;
        }
    }

    /**
     * 获取 自定义 ogg source
     */
    public static String getCustomOggSource(JsonNode jsonNode) {
        StringJoiner identifier = new StringJoiner(".");

        Optional.ofNullable(jsonNode.get("owner")).ifPresent(item -> identifier.add(item.asText()));
        Optional.ofNullable(jsonNode.get("name")).ifPresent(item -> identifier.add(item.asText()));

        String result = identifier.toString();
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        return result;
    }

    /**
     * 解析 debezium op
     */
    public static RowKind parseDebeziumOp(String op) {
        switch (op) {
            case "c":  // create
            case "r":  // read (initial snapshot)
                return RowKind.INSERT;
            case "u":  // update
                return RowKind.UPDATE_AFTER;
            case "d":  // delete
                return RowKind.DELETE;
            default:
                return null;
        }
    }

    /**
     * 获取 debezium source
     */
    public static String getDebeziumSource(JsonNode jsonNode) {
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
