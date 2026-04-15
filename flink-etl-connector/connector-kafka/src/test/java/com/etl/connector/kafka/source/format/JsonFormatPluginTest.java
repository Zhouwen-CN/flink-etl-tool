package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JsonFormatPluginTest {

    @Test
    void testIdentifier() {
        JsonFormatPlugin plugin = new JsonFormatPlugin();
        assertEquals("json", plugin.identifier());
    }

    @Test
    void testCreateDeserializer() {
        String[] fieldNames = {"id", "name"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        JsonFormatPlugin plugin = new JsonFormatPlugin();
        assertNotNull(plugin.createDeserializer(schema));
    }
}