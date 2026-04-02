package com.etl.core.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * 类型转换器
 * 将原始值转换为目标类型（基于 Flink TypeInformation）
 *
 * @deprecated 已拆分为三个专门的转换器类，建议使用：
 * <ul>
 *   <li>{@link SqlTypeConverter} - SQL 类型转换</li>
 *   <li>{@link JsonToRowConverter} - JSON 转 Row</li>
 *   <li>{@link RowToJsonConverter} - Row 转 JSON</li>
 * </ul>
 */
@Deprecated
public class TypeConverter {

    private TypeConverter() {
        // 私有构造函数，防止实例化
    }

    /**
     * 根据 JDBC java.sql.Types 转换为 Flink TypeInformation
     *
     * @param sqlType JDBC SQL 类型常量（来自 java.sql.Types）
     * @return 对应的 Flink TypeInformation
     * @deprecated 使用 {@link SqlTypeConverter#fromSqlType(int)} 代替
     */
    @Deprecated
    public static TypeInformation<?> fromSqlType(int sqlType) {
        return SqlTypeConverter.fromSqlType(sqlType);
    }

    /**
     * 将原始值转换为目标类型
     *
     * @param value 原始值（通常是 String）
     * @param fieldName 字段名（用于错误信息）
     * @param targetType 目标类型（Flink TypeInformation）
     * @return 转换后的值
     * @deprecated 使用 {@link SqlTypeConverter#convertFromValue(Object, String, TypeInformation)} 代替
     */
    @Deprecated
    public static Object convertFromValue(Object value, String fieldName, TypeInformation<?> targetType) {
        return SqlTypeConverter.convertFromValue(value, fieldName, targetType);
    }

    /**
     * 将 JsonNode 转换为 Row 列表
     *
     * @param node   JsonNode 节点
     * @param schema Schema 定义
     * @return Row 列表
     * @deprecated 使用 {@link JsonToRowConverter#convertJsonToRows(JsonNode, EtlSchema)} 代替
     */
    @Deprecated
    public static List<Row> convertJsonToRows(JsonNode node, EtlSchema schema) {
        return JsonToRowConverter.convertJsonToRows(node, schema);
    }

    /**
     * 将 JsonNode 转换为 Flink Row（基于 RowTypeInfo）
     *
     * @param node JsonNode 节点
     * @param rowTypeInfo Row 类型信息
     * @return Row 对象
     * @deprecated 使用 {@link JsonToRowConverter#convertJsonToRow(JsonNode, TypeInformation)} 代替
     */
    @Deprecated
    public static Row convertJsonToRow(JsonNode node, TypeInformation<?> rowTypeInfo) {
        return JsonToRowConverter.convertJsonToRow(node, rowTypeInfo);
    }

    /**
     * 将 Flink Row 转换为 Jackson JsonNode
     *
     * @param row Flink Row 对象
     * @return JsonNode 对象
     * @deprecated 使用 {@link RowToJsonConverter#convertRowToJsonNode(Row)} 代替
     */
    @Deprecated
    public static JsonNode convertRowToJsonNode(Row row) {
        return RowToJsonConverter.convertRowToJsonNode(row);
    }
}