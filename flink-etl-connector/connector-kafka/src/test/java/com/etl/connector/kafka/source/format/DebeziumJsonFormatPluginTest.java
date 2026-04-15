package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DebeziumJsonFormatPluginTest {

    @Test
    void testIdentifier() {
        DebeziumJsonFormatPlugin plugin = new DebeziumJsonFormatPlugin();
        assertEquals("debezium-json", plugin.identifier());
    }

    @Test
    void testCreateDeserializer() {
        String[] fieldNames = {"id"};
        TypeInformation<?>[] fieldTypes = {Types.LONG};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        DebeziumJsonFormatPlugin plugin = new DebeziumJsonFormatPlugin();
        assertNotNull(plugin.createDeserializer(schema));
    }
}