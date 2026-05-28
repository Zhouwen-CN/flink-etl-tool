package com.etl.connector.http.format;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.connector.http.source.format.JsonFormat;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * JsonFormat 单元测试
 */
class JsonFormatTest {

    private final JsonFormat format = new JsonFormat();

    private static EtlSchema schema() {
        return new EtlSchema(
                new String[]{"id", "name"},
                new TypeInformation<?>[]{Types.INT, Types.STRING});
    }

    @Test
    void testIdentifier() {
        assertEquals("json", format.identifier());
    }

    @Test
    void testParseArrayWithJsonPath() {
        String response = "{\"code\":0,\"data\":[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]}";
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("json")
                .jsonPath("$.data")
                .schema(schema())
                .build();

        List<Row> rows = format.parse(response, config);

        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getField(0));
        assertEquals("a", rows.get(0).getField(1));
        assertEquals(2, rows.get(1).getField(0));
        assertEquals("b", rows.get(1).getField(1));
    }

    @Test
    void testParseSingleObjectWithoutJsonPath() {
        String response = "{\"id\":42,\"name\":\"x\"}";
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("json")
                .schema(schema())
                .build();

        List<Row> rows = format.parse(response, config);

        assertEquals(1, rows.size());
        assertEquals(42, rows.get(0).getField(0));
        assertEquals("x", rows.get(0).getField(1));
    }

    @Test
    void testParseInvalidPathThrows() {
        String response = "{\"code\":0,\"data\":[]}";
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("json")
                .jsonPath("$.missing")
                .schema(schema())
                .build();

        assertThrows(IllegalArgumentException.class, () -> format.parse(response, config));
    }
}