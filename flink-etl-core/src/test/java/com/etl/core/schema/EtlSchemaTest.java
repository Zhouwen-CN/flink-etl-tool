package com.etl.core.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EtlSchemaTest {

    @Test
    void testCreateSchema() {
        String[] fieldNames = {"id", "name", "age"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING, Types.INT};

        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        assertEquals(3, schema.getFieldCount());
        assertArrayEquals(fieldNames, schema.getFieldNames());
        assertEquals(Types.LONG, schema.getFieldType(0));
        assertEquals(Types.STRING, schema.getFieldType(1));
        assertEquals(Types.INT, schema.getFieldType(2));
    }

    @Test
    void testGetFieldNamesAsList() {
        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};

        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        assertEquals(2, schema.getFieldNamesAsList().size());
        assertEquals("id", schema.getFieldNamesAsList().get(0));
        assertEquals("name", schema.getFieldNamesAsList().get(1));
    }

    @Test
    void testEmptySchema() {
        EtlSchema schema = new EtlSchema(new String[0], new TypeInformation<?>[0]);
        assertEquals(0, schema.getFieldCount());
    }

    @Test
    void testNullFieldNames() {
        assertThrows(IllegalArgumentException.class, () ->
            new EtlSchema(null, new TypeInformation<?>[0]));
    }

    @Test
    void testNullFieldTypes() {
        assertThrows(IllegalArgumentException.class, () ->
            new EtlSchema(new String[0], null));
    }

    @Test
    void testMismatchedArrayLengths() {
        assertThrows(IllegalArgumentException.class, () ->
            new EtlSchema(new String[]{"id"}, new TypeInformation<?>[0]));
    }

    @Test
    void testIndexOutOfBounds() {
        EtlSchema schema = new EtlSchema(new String[]{"id"}, new TypeInformation<?>[]{Types.INT});
        assertThrows(IndexOutOfBoundsException.class, () -> schema.getFieldName(1));
        assertThrows(IndexOutOfBoundsException.class, () -> schema.getFieldName(-1));
    }
}