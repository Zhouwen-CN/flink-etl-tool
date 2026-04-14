package com.etl.source.mock.generator;

import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.source.mock.config.MockSourceConfig;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 从配置生成固定数据的 Row 列表
 */
public class DataRowGenerator {

    /**
     * 解析 RowKind 字符串（大小写不敏感）
     *
     * @param kind RowKind 字符串
     * @return 对应的 RowKind 枚举
     * @throws SchemaConfigException 如果 kind 无效
     */
    public static RowKind parseRowKind(String kind) {
        if (kind == null || kind.trim().isEmpty()) {
            throw new SchemaConfigException("RowKind 不能为空");
        }

        try {
            return RowKind.valueOf(kind.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new SchemaConfigException("无效的 RowKind: " + kind +
                "，有效值: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE");
        }
    }

    /**
     * 从配置生成 Row 列表
     *
     * @param rowsData Row 数据配置列表
     * @param schema Schema 定义
     * @return 生成的 Row 列表
     * @throws SchemaConfigException 如果配置校验失败
     */
    public static List<Row> generateRows(List<MockSourceConfig.RowData> rowsData, EtlSchema schema) {
        if (rowsData == null || rowsData.isEmpty()) {
            throw new SchemaConfigException("rows 配置不能为空");
        }
        if (schema == null) {
            throw new SchemaConfigException("schema 不能为空");
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
     * @param schema Schema 定义
     * @return 生成的 Row
     * @throws SchemaConfigException 如果字段缺失或类型不匹配
     */
    private static Row generateRow(MockSourceConfig.RowData rowData, EtlSchema schema) {
        RowKind kind = parseRowKind(rowData.getKind());
        int arity = schema.getFieldCount();
        Row row = Row.withPositions(kind, arity);

        Map<String, Object> data = rowData.getData();
        if (data == null) {
            throw new SchemaConfigException("Row data 不能为 null");
        }

        // 遍历 schema 中的所有字段，确保数据完整
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String fieldName = schema.getFieldName(i);

            if (!data.containsKey(fieldName)) {
                throw new SchemaConfigException("缺失字段: " + fieldName);
            }

            Object value = data.get(fieldName);
            row.setField(i, value);
        }

        return row;
    }
}