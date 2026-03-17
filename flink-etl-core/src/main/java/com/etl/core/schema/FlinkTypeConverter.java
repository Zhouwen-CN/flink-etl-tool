package com.etl.core.schema;

import org.apache.flink.table.types.logical.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Flink 类型转换器
 * 将 EtlSchema 转换为 Flink RowType
 */
public class FlinkTypeConverter {

    /**
     * 将 EtlSchema 转换为 Flink RowType
     */
    public static RowType toRowType(EtlSchema schema) {
        List<RowType.RowField> fields = schema.getFields().stream()
            .map(f -> new RowType.RowField(f.getName(), toLogicalType(f.getType())))
            .collect(Collectors.toList());
        return new RowType(fields);
    }

    /**
     * 将 EtlFieldType 转换为 Flink LogicalType
     */
    private static LogicalType toLogicalType(EtlFieldType type) {
        switch (type) {
            case STRING:
                return new VarCharType(VarCharType.MAX_LENGTH);
            case BOOLEAN:
                return new BooleanType();
            case INT:
                return new IntType();
            case LONG:
                return new BigIntType();
            case DOUBLE:
                return new DoubleType();
            case DECIMAL:
                return new DecimalType(38, 18); // 默认精度
            case TIMESTAMP:
                return new TimestampType(9); // 纳秒精度
            case BYTES:
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            default:
                throw new IllegalArgumentException("不支持的类型: " + type);
        }
    }
}