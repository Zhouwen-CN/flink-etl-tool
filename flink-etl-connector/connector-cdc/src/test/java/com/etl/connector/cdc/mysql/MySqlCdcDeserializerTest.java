package com.etl.connector.cdc.mysql;

import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MySqlCdcDeserializer 测试类
 * 使用 H2 内存数据库验证动态 Schema 获取和 Debezium JSON 解析
 */
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
            // 删除表以便下次测试
            Statement stmt = h2Connection.createStatement();
            stmt.execute("DROP TABLE IF EXISTS users");
            stmt.close();
            h2Connection.close();
        }
    }

    @Test
    void testSchemaExtractionFromDatabase() throws Exception {
        // 创建序列化器（传入 H2 连接参数）
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb",
            -1,  // H2 内存数据库端口为 -1
            "testdb",
            USERNAME,
            PASSWORD,
            "users"
        );

        // 模拟 open() 方法调用（手动触发 Schema 获取）
        deserializer.open(null);

        // 验证 Schema 是否正确获取
        RowType rowType = deserializer.getRowType();

        assertNotNull(rowType);
        assertEquals(5, rowType.getFieldCount());

        // 验证字段名称（H2 默认返回大写字段名）
        List<String> fieldNames = rowType.getFieldNames();
        assertTrue(fieldNames.contains("ID"));
        assertTrue(fieldNames.contains("NAME"));
        assertTrue(fieldNames.contains("AGE"));
        assertTrue(fieldNames.contains("SALARY"));
        assertTrue(fieldNames.contains("CREATED_AT"));
    }

    @Test
    void testDeserializeInsert() throws Exception {
        // 创建序列化器并初始化 Schema
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );
        deserializer.open(null);

        // Debezium INSERT JSON（对应 users 表）
        // 注意：H2 字段名默认为大写
        String json = "{" +
            "\"before\":null," +
            "\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}," +
            "\"op\":\"c\"" +
            "}";

        org.apache.flink.types.Row row = deserializer.deserialize(json.getBytes());

        assertEquals(org.apache.flink.types.RowKind.INSERT, row.getKind());
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
        deserializer.open(null);

        // H2 字段名默认为大写
        String json = "{" +
            "\"before\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}," +
            "\"after\":{\"ID\":1,\"NAME\":\"Bob\",\"AGE\":35,\"SALARY\":6000.0,\"CREATED_AT\":\"2023-01-01 10:00:00\"}," +
            "\"op\":\"u\"" +
            "}";

        org.apache.flink.types.Row row = deserializer.deserialize(json.getBytes());

        assertEquals(org.apache.flink.types.RowKind.UPDATE_AFTER, row.getKind());
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
        deserializer.open(null);

        // H2 字段名默认为大写
        String json = "{" +
            "\"before\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}," +
            "\"after\":null," +
            "\"op\":\"d\"" +
            "}";

        org.apache.flink.types.Row row = deserializer.deserialize(json.getBytes());

        assertEquals(org.apache.flink.types.RowKind.DELETE, row.getKind());
        assertEquals(1L, row.getField(0));
        assertEquals("Alice", row.getField(1));
        assertEquals(30, row.getField(2));
        assertEquals(5000.5, row.getField(3));
    }

    @Test
    void testDeserializeRead() throws Exception {
        // 测试 op='r'（快照读取）的处理
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );
        deserializer.open(null);

        String json = "{" +
            "\"before\":null," +
            "\"after\":{\"ID\":2,\"NAME\":\"Charlie\",\"AGE\":25,\"SALARY\":4000.0,\"CREATED_AT\":\"2023-01-02 10:00:00\"}," +
            "\"op\":\"r\"" +
            "}";

        org.apache.flink.types.Row row = deserializer.deserialize(json.getBytes());

        assertEquals(org.apache.flink.types.RowKind.INSERT, row.getKind());
        assertEquals(2L, row.getField(0));
        assertEquals("Charlie", row.getField(1));
    }

    @Test
    void testDeserializeWithNullValue() throws Exception {
        // 测试字段值为 null 的处理
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );
        deserializer.open(null);

        String json = "{" +
            "\"before\":null," +
            "\"after\":{\"ID\":3,\"NAME\":null,\"AGE\":null,\"SALARY\":null,\"CREATED_AT\":null}," +
            "\"op\":\"c\"" +
            "}";

        org.apache.flink.types.Row row = deserializer.deserialize(json.getBytes());

        assertEquals(org.apache.flink.types.RowKind.INSERT, row.getKind());
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
        deserializer.open(null);

        String json = "{" +
            "\"before\":null," +
            "\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}," +
            "\"op\":\"x\"" +  // 不支持的 op 类型
            "}";

        assertThrows(IllegalArgumentException.class, () -> {
            deserializer.deserialize(json.getBytes());
        });
    }

    @Test
    void testMissingOpField() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );
        deserializer.open(null);

        // 缺少 'op' 字段
        String json = "{" +
            "\"before\":null," +
            "\"after\":{\"ID\":1,\"NAME\":\"Alice\",\"AGE\":30,\"SALARY\":5000.5,\"CREATED_AT\":\"2023-01-01 10:00:00\"}" +
            "}";

        assertThrows(java.io.IOException.class, () -> {
            deserializer.deserialize(json.getBytes());
        });
    }

    @Test
    void testMissingBeforeFieldForDelete() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );
        deserializer.open(null);

        // DELETE 操作缺少 'before' 字段
        String json = "{" +
            "\"before\":null," +
            "\"after\":null," +
            "\"op\":\"d\"" +
            "}";

        assertThrows(java.io.IOException.class, () -> {
            deserializer.deserialize(json.getBytes());
        });
    }

    @Test
    void testMissingAfterFieldForInsert() throws Exception {
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
        );
        deserializer.open(null);

        // INSERT 操作缺少 'after' 字段
        String json = "{" +
            "\"before\":null," +
            "\"after\":null," +
            "\"op\":\"c\"" +
            "}";

        assertThrows(java.io.IOException.class, () -> {
            deserializer.deserialize(json.getBytes());
        });
    }

    @Test
    void testTableNotExist() {
        // 测试表不存在的情况
        MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
            "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "non_existent_table"
        );

        assertThrows(IllegalStateException.class, () -> {
            deserializer.open(null);
        });
    }
}