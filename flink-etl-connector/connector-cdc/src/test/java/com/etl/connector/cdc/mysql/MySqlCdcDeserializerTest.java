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
     * 创建 MySqlCdcConfig（适配新的构造方式）
     */
    private MySqlCdcConfig createCdcConfig(String table) {
        return MySqlCdcConfig.builder()
            .url(H2_URL)
            .hostname("mem:testdb")
            .port(-1)
            .database("testdb")
            .username(USERNAME)
            .password(PASSWORD)
            .table(table)
            .startupMode(StartupMode.LATEST)
            .build();
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
    void testDeserializeInsert() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

        // H2 默认将字段名转为大写，Debezium JSON 也应使用大写字段名
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
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

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
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

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
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

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
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

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
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

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
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

        String json = "{\"before\":null,\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        assertThrows(Exception.class, () -> {
            deserializer.deserialize(record, collector);
        });
    }

    @Test
    void testMissingBeforeFieldForDelete() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

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
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

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
        // 新实现：表不存在时，构造函数直接抛出异常（构造时 Schema 推断）
        assertThrows(RuntimeException.class, () -> {
            MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("non_existent_table"));
        });
    }

    /**
     * 测试字段名大小写匹配
     * 验证 Schema 推断结果与 Debezium JSON 字段名是否一致
     */
    @Test
    void testFieldNameCaseSensitivity() throws Exception {
        // 创建表使用小写字段名（H2 会转为大写）
        Statement stmt = h2Connection.createStatement();
        stmt.execute("DROP TABLE IF EXISTS users");
        stmt.execute("CREATE TABLE users (" +
            "userId BIGINT PRIMARY KEY, " +  // 混合大小写
            "UserName VARCHAR(255), " +       // 混合大小写
            "AGE INT, " +                     // 全大写
            "salary DOUBLE, " +               // 全小写
            "created_at TIMESTAMP" +          // 全小写
            ")");
        stmt.close();

        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

        // H2 将字段名转为大写，Debezium JSON 也应使用大写字段名
        String json = "{\"before\":null,\"after\":{\"USERID\":1,\"USERNAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"},\"op\":\"c\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        deserializer.deserialize(record, collector);

        Row row = collector.getFirstRow();
        assertNotNull(row);

        // 验证字段值正确解析（说明字段名大小写匹配）
        assertEquals(RowKind.INSERT, row.getKind());
        assertEquals(1L, row.getField(0));  // USERID
        assertEquals("Alice", row.getField(1));  // USERNAME
        assertEquals(30, row.getField(2));  // AGE
        assertEquals(5000.5, row.getField(3));  // SALARY
    }

    /**
     * 测试字段名大小写不匹配场景
     * 如果 Debezium JSON 字段名与 Schema 推断结果不一致，应该抛出异常或返回 null
     */
    @Test
    void testFieldNameCaseMismatch() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(createCdcConfig("users"));

        // H2 Schema 推断得到大写字段名（ID, NAME, AGE, SALARY, CREATED_AT）
        // 但 JSON 使用小写字段名（id, name, age, salary, created_at）
        String json = "{\"before\":null,\"after\":{\"id\":1,\"name\":\"Alice\",\"age\":30,\"salary\":5000.5,\"created_at\":\"2023-01-01 10:00:00\"},\"op\":\"c\"}";

        TestCollector collector = new TestCollector();
        SourceRecord record = createSourceRecord(json);

        deserializer.deserialize(record, collector);

        Row row = collector.getFirstRow();
        assertNotNull(row);

        // 字段名大小写不匹配时，JsonNode.get(fieldName) 返回 null
        // 所有字段值应该为 null
        assertNull(row.getField(0));  // id != ID
        assertNull(row.getField(1));  // name != NAME
        assertNull(row.getField(2));  // age != AGE
        assertNull(row.getField(3));  // salary != SALARY
        assertNull(row.getField(4));  // created_at != CREATED_AT
    }
}