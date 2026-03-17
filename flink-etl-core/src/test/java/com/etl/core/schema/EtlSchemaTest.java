package com.etl.core.schema;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class EtlSchemaTest {

    @Test
    void getField_byIndex_shouldReturnCorrectField() {
        EtlSchema schema = new EtlSchema(null, Arrays.asList(
            new EtlField("id", EtlFieldType.LONG),
            new EtlField("name", EtlFieldType.STRING)
        ));

        assertEquals("id", schema.getField(0).getName());
        assertEquals("name", schema.getField(1).getName());
    }

    @Test
    void getField_byName_shouldReturnCorrectField() {
        EtlSchema schema = new EtlSchema(null, Arrays.asList(
            new EtlField("id", EtlFieldType.LONG),
            new EtlField("name", EtlFieldType.STRING)
        ));

        assertEquals(EtlFieldType.LONG, schema.getField("id").getType());
        assertEquals(EtlFieldType.STRING, schema.getField("name").getType());
    }

    @Test
    void getField_byName_shouldReturnNull_whenNotFound() {
        EtlSchema schema = new EtlSchema(null, Arrays.asList(
            new EtlField("id", EtlFieldType.LONG)
        ));

        assertNull(schema.getField("nonexistent"));
    }

    @Test
    void getFieldNames_shouldReturnAllNames() {
        EtlSchema schema = new EtlSchema(null, Arrays.asList(
            new EtlField("id", EtlFieldType.LONG),
            new EtlField("name", EtlFieldType.STRING)
        ));

        List<String> names = schema.getFieldNames();
        assertEquals(2, names.size());
        assertEquals("id", names.get(0));
        assertEquals("name", names.get(1));
    }

    @Test
    void getTableName_shouldReturnCorrectValue() {
        EtlSchema schema = new EtlSchema();
        schema.setTableName("users");
        schema.setFields(Arrays.asList(
            new EtlField("id", EtlFieldType.LONG)
        ));

        assertEquals("users", schema.getTableName());
    }

    @Test
    void constructor_withTableName_shouldSetAllFields() {
        EtlSchema schema = new EtlSchema();
        schema.setTableName("orders");
        schema.setFields(Arrays.asList(
            new EtlField("order_id", EtlFieldType.LONG),
            new EtlField("amount", EtlFieldType.DOUBLE)
        ));

        assertEquals("orders", schema.getTableName());
        assertEquals(2, schema.getFields().size());
    }
}