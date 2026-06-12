package com.etl.connector.doris.sink.format;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RowToDorisJsonSerializerTest {

    private DorisSinkConfig config() {
        return DorisSinkConfig.builder()
                .fenodes("127.0.0.1:8030")
                .tableIdentifier("test_db.test_tbl")
                .username("root")
                .password("")
                .format("json")
                .build();
    }

    @Test
    void serialize_namedRow_producesJsonWithDbAndTable() throws Exception {
        RowToDorisJsonSerializer ser = new RowToDorisJsonSerializer(config());

        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("name", "alice");

        DorisRecord record = ser.serialize(row);

        assertEquals("test_db", record.getDatabase());
        assertEquals("test_tbl", record.getTable());

        String json = new String(record.getRow(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"id\""), "json 应含字段 id: " + json);
        assertTrue(json.contains("\"name\""), "json 应含字段 name: " + json);
        assertTrue(json.contains("alice"), "json 应含值 alice: " + json);
    }
}
