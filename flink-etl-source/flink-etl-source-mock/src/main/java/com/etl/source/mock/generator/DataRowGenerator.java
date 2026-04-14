package com.etl.source.mock.generator;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.source.mock.config.MockSourceConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;

/**
 * 从配置生成固定数据的 Row 列表
 */
public class DataRowGenerator {

    /**
     * 解析 RowKind 字符串（大小写不敏感）
     *
     * @param kind RowKind 字符串
     * @return 对应的 RowKind 枚举
     */
    public static RowKind parseRowKind(String kind) {
        if (kind == null || kind.trim().isEmpty()) {
            return RowKind.INSERT;
        }
        try {
            return RowKind.valueOf(kind.toUpperCase());
        } catch (IllegalArgumentException e) {
            return RowKind.INSERT;
        }
    }

    /**
     * 从配置生成 Row 列表
     *
     * @param rowsData Row 数据配置列表
     * @param schema   Schema 定义
     * @return 生成的 Row 列表
     */
    public static List<Row> generateRows(List<MockSourceConfig.RowData> rowsData, EtlSchema schema) {
        if (rowsData == null || rowsData.isEmpty()) {
            return new ArrayList<>();
        }

        List<Row> rows = new ArrayList<>();
        for (MockSourceConfig.RowData rowData : rowsData) {
            Row row = generateRow(rowData, schema);
            rows.add(row);
        }
        return rows;
    }

    /**
     * 从单个配置生成 Row
     *
     * @param rowData Row 数据配置
     * @param schema  Schema 定义
     * @return 生成的 Row
     */
    private static Row generateRow(MockSourceConfig.RowData rowData, EtlSchema schema) {
        JsonNode data = rowData.getData();

        // 使用 JsonToRowConverter 转换
        Row row = JsonToRowConverter.convertJsonToRow(data, schema);

        // 设置 RowKind
        RowKind kind = parseRowKind(rowData.getKind());
        if (kind != RowKind.INSERT) {
            row.setKind(kind);
        }

        return row;
    }
}
