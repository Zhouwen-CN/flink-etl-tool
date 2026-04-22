package com.etl.connector.cdc.mysql;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MySqlCdcDeserializerTest {

    private Connection h2Connection;
    private final String H2_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private final String USERNAME = "sa";
    private final String PASSWORD = "";

    @BeforeEach
    void setUp() throws Exception {
        // 创建 H2 内存数据库并建表
        h2Connection = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
        Statement stmt = h2Connection.createStatement();

        stmt.execute("CREATE TABLE users (" +
            "id BIGINT PRIMARY KEY, " +
            "name VARCHAR(255), " +
            "age INT, " +
            "salary DOUBLE, " +
            "created_at TIMESTAMP" +
            ")");

        stmt.close();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (h2Connection != null) {
            Statement stmt = h2Connection.createStatement();
            stmt.execute("DROP TABLE IF EXISTS users");
            stmt.close();
            h2Connection.close();
        }
    }

    /**
     * 创建简单的 Collector 实现，用于收集 Row 结果
     */
    private static class TestCollector implements Collector<Row> {
        private final List<Row> collectedRows = new ArrayList<>();

        @Override
        public void collect(Row record) {
            collectedRows.add(record);
        }

        @Override
        public void close() {
        }

        public List<Row> getCollectedRows() {
            return collectedRows;
        }

        public Row getFirstRow() {
            return collectedRows.isEmpty() ? null : collectedRows.get(0);
        }
    }

    /**
     * 创建 SourceRecord（包含 Debezium JSON value）
     */
    private SourceRecord createSourceRecord(String json) {
        return new SourceRecord(
            null,  // sourcePartition
            null,  // sourceOffset
            "test-topic",  // topic
            null,  // keySchema
            null,  // key
            null,  // valueSchema
            json.getBytes()  // value（Debezium JSON bytes）
        );
    }

    @Test
    void testSchemaExtractionFromDatabase() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        // 创建简单的 INSERT JSON 触发 Schema 获取（延迟初始化）
        String json = "{\"before\":null,\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"},\"op\":\"c\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        deserializer.deserialize(record, collector);

        // 验证 Schema 是否正确获取
        assertNotNull(deserializer.getRowType());
        assertEquals(5, deserializer.getRowType().getFieldCount());

        // 验证字段名称（H2 默认返回大写字段名）
        List<String> fieldNames = deserializer.getRowType().getFieldNames();
        assertTrue(fieldNames.contains("ID"));
        assertTrue(fieldNames.contains("NAME"));
        assertTrue(fieldNames.contains("AGE"));
        assertTrue(fieldNames.contains("SALARY"));
        assertTrue(fieldNames.contains("CREATED_AT"));
    }

    @Test
    void testDeserializeInsert() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        String json = "{\"before\":null,\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"},\"op\":\"c\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        deserializer.deserialize(record, collector);

        Row row = collector.getFirstRow();
        assertNotNull(row);

        assertEquals(RowKind.INSERT, row.getKind());
        assertEquals(1L, row.getField(0));  // ID
        assertEquals("Alice", row.getField(1));  // NAME
        assertEquals(30, row.getField(2));  // AGE
        assertEquals(5000.5, row.getField(3));  // SALARY
    }

    @Test
    void testDeserializeUpdate() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        String json = "{\"before\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}," +
            "\"after\":{\"ID\":1,\"NAME\":\"Bob\",\"AGE\":35,\"SALARY\":6000.0,\"CREATED_AT\":\"2023-01-01 10:00:00\"}," +
            "\"op\":\"u\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        deserializer.deserialize(record, collector);

        Row row = collector.getFirstRow();
        assertNotNull(row);

        assertEquals(RowKind.UPDATE_AFTER, row.getKind());
        assertEquals(1L, row.getField(0));
        assertEquals("Bob", row.getField(1));
        assertEquals(35, row.getField(2));
        assertEquals(6000.0, row.getField(3));
    }

    @Test
    void testDeserializeDelete() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        String json = "{\"before\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}," +
            "\"after\":null," +
            "\"op\":\"d\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        deserializer.deserialize(record, collector);

        Row row = collector.getFirstRow();
        assertNotNull(row);

        assertEquals(RowKind.DELETE, row.getKind());
        assertEquals(1L, row.getField(0));
        assertEquals("Alice", row.getField(1));
        assertEquals(30, row.getField(2));
        assertEquals(5000.5, row.getField(3));
    }

    @Test
    void testDeserializeRead() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        String json = "{\"before\":null,\"after\":{\"ID\":2,\"NAME\":\"Charlie\",\"AGE\":25,\"SALARY\":4000.0,\"CREATED_AT\":\"2023-01-02 10:00:00\"},\"op\":\"r\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        deserializer.deserialize(record, collector);

        Row row = collector.getFirstRow();
        assertNotNull(row);

        assertEquals(RowKind.INSERT, row.getKind());  // READ 映射到 INSERT
        assertEquals(2L, row.getField(0));
        assertEquals("Charlie", row.getField(1));
    }

    @Test
    void testDeserializeWithNullValue() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        String json = "{\"before\":null,\"after\":{\"ID\":3,\"NAME\":null,\"AGE\":null,\"SALARY\":null,\"CREATED_AT\":null},\"op\":\"c\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        deserializer.deserialize(record, collector);

        Row row = collector.getFirstRow();
        assertNotNull(row);

        assertEquals(RowKind.INSERT, row.getKind());
        assertEquals(3L, row.getField(0));
        assertNull(row.getField(1));
        assertNull(row.getField(2));
        assertNull(row.getField(3));
        assertNull(row.getField(4));
    }

    @Test
    void testUnsupportedOperationType() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        // 先触发 Schema 初始化
        String validJson = "{\"before\":null,\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"},\"op\":\"c\"}";
        TestCollector initCollector = new TestCollector();
        SourceRecord initRecord = createSourceRecord(validJson);
        deserializer.deserialize(initRecord, initCollector);

        // 不支持的 op 类型
        String invalidJson = "{\"before\":null,\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"},\"op\":\"x\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(invalidJson);

        assertThrows(IllegalArgumentException.class, () -> {
            deserializer.deserialize(record, collector);
        });
    }

    @Test
    void testMissingOpField() {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        String json = "{\"before\":null,\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        assertThrows(Exception.class, () -> {
            deserializer.deserialize(record, collector);
        });
    }

    @Test
    void testMissingBeforeFieldForDelete() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        // 先触发 Schema 初始化
        String validJson = "{\"before\":null,\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"},\"op\":\"c\"}";
        TestCollector initCollector = new TestCollector();
        SourceRecord initRecord = createSourceRecord(validJson);
        deserializer.deserialize(initRecord, initCollector);

        // DELETE 缺少 before 字段
        String invalidJson = "{\"before\":null,\"after\":null,\"op\":\"d\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(invalidJson);

        assertThrows(Exception.class, () -> {
            deserializer.deserialize(record, collector);
        });
    }

    @Test
    void testMissingAfterFieldForInsert() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );

        // 先触发 Schema 初始化
        String validJson = "{\"before\":null,\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"},\"op\":\"c\"}";
        TestCollector initCollector = new TestCollector();
        SourceRecord initRecord = createSourceRecord(validJson);
        deserializer.deserialize(initRecord, initCollector);

        // INSERT 缺少 after 字段
        String invalidJson = "{\"before\":null,\"after\":null,\"op\":\"c\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(invalidJson);

        assertThrows(Exception.class, () -> {
            deserializer.deserialize(record, collector);
        });
    }

    @Test
    void testTableNotExist() {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "non_existent_table"
        );

        String json = "{\"before\":null,\"after\":{\"id\":1},\"op\":\"c\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        assertThrows(Exception.class, () -> {
            deserializer.deserialize(record, collector);
        });
    }
}