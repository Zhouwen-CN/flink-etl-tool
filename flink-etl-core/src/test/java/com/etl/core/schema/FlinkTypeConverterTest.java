package com.etl.core.schema;

import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class FlinkTypeConverterTest {

    @Test
    void toRowType_shouldReturnCorrectRowType() {
        EtlSchema schema = new EtlSchema(null, Arrays.asList(
            new EtlField("id", EtlFieldType.LONG),
            new EtlField("name", EtlFieldType.STRING),
            new EtlField("age", EtlFieldType.INT),
            new EtlField("price", EtlFieldType.DOUBLE),
            new EtlField("active", EtlFieldType.BOOLEAN),
            new EtlField("amount", EtlFieldType.DECIMAL),
            new EtlField("created_at", EtlFieldType.TIMESTAMP),
            new EtlField("data", EtlFieldType.BYTES)
        ));

        RowType rowType = FlinkTypeConverter.toRowType(schema);

        assertEquals(8, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));

        // 验证类型映射
        assertTrue(rowType.getTypeAt(0) instanceof BigIntType);
        assertTrue(rowType.getTypeAt(1) instanceof VarCharType);
        assertTrue(rowType.getTypeAt(2) instanceof IntType);
        assertTrue(rowType.getTypeAt(3) instanceof DoubleType);
        assertTrue(rowType.getTypeAt(4) instanceof BooleanType);
        assertTrue(rowType.getTypeAt(5) instanceof DecimalType);
        assertTrue(rowType.getTypeAt(6) instanceof TimestampType);
        assertTrue(rowType.getTypeAt(7) instanceof VarBinaryType);
    }

    @Test
    void toRowType_shouldHandleSingleField() {
        EtlSchema schema = new EtlSchema(null, Arrays.asList(
            new EtlField("id", EtlFieldType.INT)
        ));

        RowType rowType = FlinkTypeConverter.toRowType(schema);

        assertEquals(1, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertTrue(rowType.getTypeAt(0) instanceof IntType);
    }
}