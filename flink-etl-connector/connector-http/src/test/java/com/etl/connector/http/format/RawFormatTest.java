package com.etl.connector.http.format;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.connector.http.source.format.RawFormat;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RawFormat 单元测试
 */
class RawFormatTest {

    private final RawFormat format = new RawFormat();

    @Test
    void testIdentifier() {
        assertEquals("raw", format.identifier());
    }

    @Test
    void testParse() {
        String response = "<html><body>Hello</body></html>";
        EtlSchema schema = new EtlSchema(new String[]{"body"}, new TypeInformation<?>[]{Types.STRING});
        HttpSourceConfig config = HttpSourceConfig.builder().format("raw").schema(schema).build();

        List<Row> rows = format.parse(response, config);

        assertEquals(1, rows.size());
        assertEquals(response, rows.get(0).getField(0));
    }

    @Test
    void testParseEmptyResponse() {
        EtlSchema schema = new EtlSchema(new String[]{"body"}, new TypeInformation<?>[]{Types.STRING});
        HttpSourceConfig config = HttpSourceConfig.builder().format("raw").schema(schema).build();

        List<Row> rows = format.parse("", config);

        assertEquals(1, rows.size());
        assertEquals("", rows.get(0).getField(0));
    }
}