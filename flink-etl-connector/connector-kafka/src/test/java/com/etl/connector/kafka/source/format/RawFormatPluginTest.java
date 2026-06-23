package com.etl.connector.kafka.source.format;

import com.etl.connector.kafka.source.format.raw.RawFormatPlugin;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RawFormatPluginTest {

    @Test
    void testIdentifier() {
        RawFormatPlugin plugin = new RawFormatPlugin();
        assertEquals("raw", plugin.identifier());
    }

    @Test
    void testCreateDeserializerWithValidSchema() {
        String[] fieldNames = {"message"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawFormatPlugin plugin = new RawFormatPlugin();
        assertNotNull(plugin.createDeserializer(schema));
    }

    @Test
    void testCreateDeserializerWithMultipleFieldsThrows() {
        String[] fieldNames = {"message", "extra"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.STRING, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawFormatPlugin plugin = new RawFormatPlugin();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> plugin.createDeserializer(schema));
        assertTrue(exception.getMessage().contains("raw format requires exactly one STRING field"));
    }

    @Test
    void testCreateDeserializerWithNonStringTypeThrows() {
        String[] fieldNames = {"message"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.LONG};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawFormatPlugin plugin = new RawFormatPlugin();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> plugin.createDeserializer(schema));
        assertTrue(exception.getMessage().contains("raw format requires the field type to be STRING"));
    }
}