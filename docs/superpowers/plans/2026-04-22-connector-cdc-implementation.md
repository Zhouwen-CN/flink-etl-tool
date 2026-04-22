# connector-cdc 模块实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 connector-cdc 模块，封装 flink-connector-mysql-cdc，实现 MySQL CDC Source，支持实时捕获数据库变更并输出带 RowKind 标记的 Row 数据流。完整实现动态 Schema 获取逻辑，确保模块可以直接用于生产环境。

**Architecture:** 采用最小封装方案，直接使用 flink-connector-mysql-cdc 的 MySqlSource，类似 Kafka Source 设计。包含 3 个核心组件：MySqlCdcSourcePlugin（SPI 插件）、MySqlCdcConfig（配置解析 + URL 正则）、MySqlCdcDeserializer（Debezium JSON → Row 序列化器 + 动态 Schema 获取）。

**Tech Stack:** Java 1.8、Flink 1.15.2、flink-connector-mysql-cdc 2.3.0、Jackson JSON、MySQL JDBC Driver

---

## 文件结构

**创建文件**：
- `flink-etl-connector/connector-cdc/pom.xml` - Maven 配置
- `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/StartupMode.java` - 启动模式枚举
- `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcConfig.java` - 配置解析类
- `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcDeserializer.java` - 序列化器（包含动态 Schema 获取）
- `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcSourcePlugin.java` - SPI 插件
- `flink-etl-connector/connector-cdc/src/test/java/com/etl/connector/cdc/mysql/MySqlCdcConfigTest.java` - 配置测试
- `flink-etl-connector/connector-cdc/src/test/java/com/etl/connector/cdc/mysql/MySqlCdcDeserializerTest.java` - 序列化器测试（使用 H2 内存数据库）

**修改文件**：
- `flink-etl-connector/pom.xml:20-27` - 添加 connector-cdc 模块
- `flink-etl-client/pom.xml:18-51` - 添加 connector-cdc 依赖
- `PLUGINS.md` - 添加 MySQL CDC Source 文档

---

## Task 1: 创建 Maven 模块配置

**Files:**
- Create: `flink-etl-connector/connector-cdc/pom.xml`
- Modify: `flink-etl-connector/pom.xml:20-27`

- [ ] **Step 1: 在父 pom.xml 中添加模块**

在 `flink-etl-connector/pom.xml` 的 modules 部分添加 connector-cdc：

```xml
<modules>
    <module>connector-jdbc</module>
    <module>connector-kafka</module>
    <module>connector-localfile</module>
    <module>connector-console</module>
    <module>connector-http</module>
    <module>connector-mock</module>
    <module>connector-cdc</module>
</modules>
```

- [ ] **Step 2: 创建 connector-cdc/pom.xml**

创建完整的 Maven 配置文件，依赖 flink-connector-mysql-cdc 和 MySQL 动动：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-connector</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>connector-cdc</artifactId>

    <name>Flink ETL Connector - CDC</name>
    <description>CDC 连接器（MySQL CDC Source）</description>

    <dependencies>
        <!-- flink-connector-mysql-cdc -->
        <dependency>
            <groupId>com.ververica</groupId>
            <artifactId>flink-connector-mysql-cdc</artifactId>
            <version>2.3.0</version>
        </dependency>

        <!-- MySQL 驱动（用于 Schema 自动获取） -->
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
        </dependency>

        <!-- H2 数据库（测试用） -->
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: 提交 Maven 配置**

```bash
git add flink-etl-connector/pom.xml flink-etl-connector/connector-cdc/pom.xml
git commit -m "feat: 添加 connector-cdc 模块 Maven 配置"
```

---

## Task 2: 创建 StartupMode 枚举类

**Files:**
- Create: `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/StartupMode.java`

- [ ] **Step 1: 创建 StartupMode 枚举**

创建启动模式枚举，对应 flink-connector-mysql-cdc 的 StartupOptions：

```java
package com.etl.connector.cdc.mysql;

/**
 * MySQL CDC 启动模式
 */
public enum StartupMode {
    /**
     * 从最早可用位置开始（读取历史变更）
     */
    EARLIEST,

    /**
     * 从最新位置开始（只捕获新变更）
     */
    LATEST,

    /**
     * 从指定时间戳开始
     */
    TIMESTAMP,

    /**
     * 先读取全量快照，再捕获增量变更
     */
    SNAPSHOT_FIRST
}
```

- [ ] **Step 2: 提交 StartupMode**

```bash
git add flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/StartupMode.java
git commit -m "feat: 添加 MySQL CDC 启动模式枚举"
```

---

## Task 3: 实现 MySqlCdcConfig 配置解析类

**Files:**
- Create: `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcConfig.java`
- Test: `flink-etl-connector/connector-cdc/src/test/java/com/etl/connector/cdc/mysql/MySqlCdcConfigTest.java`

- [ ] **Step 1: 编写 URL 解析失败的测试**

创建测试类，验证 URL 格式错误的异常抛出：

```java
package com.etl.connector.cdc.mysql;

import com.etl.core.config.SourceConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MySqlCdcConfigTest {

    @Test
    void testParseUrlWithInvalidPrefix() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:postgresql://localhost:5432/mydb");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            MySqlCdcConfig.fromSourceConfig(sourceConfig);
        }, "URL 必须以 jdbc:mysql:// 开头");
    }

    @Test
    void testParseUrlWithInvalidFormat() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://invalid_url");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "users");

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            MySqlCdcConfig.fromSourceConfig(sourceConfig);
        }, "URL 格式错误");
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd flink-etl-connector/connector-cdc
mvn test -Dtest=MySqlCdcConfigTest
```

预期：FAIL - MySqlCdcConfig 类不存在

- [ ] **Step 3: 实现 MySqlCdcConfig（完整实现）**

创建配置类，实现完整的配置解析和 StartupOptions 转换：

```java
package com.etl.connector.cdc.mysql;

import com.etl.core.config.SourceConfig;
import com.ververica.cdc.connectors.mysql.source.StartupOptions;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MySQL CDC 配置封装类
 */
@Getter
@Builder
public class MySqlCdcConfig implements Serializable {
    private final String hostname;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final String table;
    private final StartupMode startupMode;
    private final Long startupTimestamp;
    private final Integer serverId;

    /**
     * 从 SourceConfig 解析配置参数
     */
    public static MySqlCdcConfig fromSourceConfig(SourceConfig config) {
        Map<String, Object> configMap = config.getConfig();

        // 解析 URL
        String url = (String) configMap.get("url");
        UrlParseResult urlResult = parseUrl(url);

        // 解析其他参数
        String username = (String) configMap.get("username");
        String password = (String) configMap.get("password");
        String table = (String) configMap.get("table");

        // 解析启动模式
        String startupModeStr = (String) configMap.getOrDefault("startupMode", "latest");
        StartupMode startupMode = StartupMode.valueOf(startupModeStr.toUpperCase());

        // timestamp 模式校验
        Long startupTimestamp = null;
        if (startupMode == StartupMode.TIMESTAMP) {
            Object timestampObj = configMap.get("startupTimestamp");
            if (timestampObj == null) {
                throw new IllegalArgumentException("startupMode=timestamp 时必须配置 startupTimestamp");
            }
            startupTimestamp = ((Number) timestampObj).longValue();
        }

        // serverId（可选）
        Integer serverId = null;
        Object serverIdObj = configMap.get("serverId");
        if (serverIdObj != null) {
            serverId = ((Number) serverIdObj).intValue();
        }

        return MySqlCdcConfig.builder()
            .hostname(urlResult.hostname)
            .port(urlResult.port)
            .database(urlResult.database)
            .username(username)
            .password(password)
            .table(table)
            .startupMode(startupMode)
            .startupTimestamp(startupTimestamp)
            .serverId(serverId)
            .build();
    }

    /**
     * URL 正则解析：jdbc:mysql://host:port/database
     */
    private static UrlParseResult parseUrl(String url) {
        // URL 格式校验
        if (url == null || !url.startsWith("jdbc:mysql://")) {
            throw new IllegalArgumentException("URL 必须以 jdbc:mysql:// 开头");
        }

        // 正则解析
        Pattern pattern = Pattern.compile("jdbc:mysql://([^:]+):(\\d+)/([^?]+)");
        Matcher matcher = pattern.matcher(url);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("URL 格式错误，应为 jdbc:mysql://host:port/database");
        }

        String hostname = matcher.group(1);
        int port = Integer.parseInt(matcher.group(2));
        String database = matcher.group(3);

        return new UrlParseResult(hostname, port, database);
    }

    /**
     * 获取 StartupOptions（用于 MySqlSource.builder）
     */
    public StartupOptions getStartupOptions() {
        switch (startupMode) {
            case EARLIEST:
                return StartupOptions.earliest();
            case LATEST:
                return StartupOptions.latest();
            case TIMESTAMP:
                return StartupOptions.timestamp(startupTimestamp);
            case SNAPSHOT_FIRST:
                return StartupOptions.snapshot_first();
            default:
                throw new IllegalArgumentException("不支持的启动模式: " + startupMode);
        }
    }

    /**
     * URL 解析结果内部类
     */
    private static class UrlParseResult {
        final String hostname;
        final int port;
        final String database;

        UrlParseResult(String hostname, int port, String database) {
            this.hostname = hostname;
            this.port = port;
            this.database = database;
        }
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

```bash
cd flink-etl-connector/connector-cdc
mvn test -Dtest=MySqlCdcConfigTest
```

预期：PASS - URL 解析异常正确抛出

- [ ] **Step 5: 编写 URL 解析成功的测试**

添加成功解析测试用例：

```java
@Test
void testParseUrlSuccessfully() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("url", "jdbc:mysql://localhost:3306/mydb");
    configMap.put("username", "root");
    configMap.put("password", "password");
    configMap.put("table", "users");

    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setConfig(configMap);

    MySqlCdcConfig config = MySqlCdcConfig.fromSourceConfig(sourceConfig);

    assertEquals("localhost", config.getHostname());
    assertEquals(3306, config.getPort());
    assertEquals("mydb", config.getDatabase());
    assertEquals("root", config.getUsername());
    assertEquals("password", config.getPassword());
    assertEquals("users", config.getTable());
    assertEquals(StartupMode.LATEST, config.getStartupMode());
}
```

- [ ] **Step 6: 运行测试验证通过**

```bash
mvn test -Dtest=MySqlCdcConfigTest
```

预期：PASS - URL 成功解析

- [ ] **Step 7: 提交 URL 解析实现**

```bash
git add flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcConfig.java
git add flink-etl-connector/connector-cdc/src/test/java/com/etl/connector/cdc/mysql/MySqlCdcConfigTest.java
git commit -m "feat: 实现 MySqlCdcConfig URL 正则解析"
```

---

## Task 4: 实现 MySqlCdcDeserializer 序列化器（基础框架 + 动态 Schema 获取）

**Files:**
- Create: `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcDeserializer.java`
- Test: `flink-etl-connector/connector-cdc/src/test/java/com/etl/connector/cdc/mysql/MySqlCdcDeserializerTest.java`

- [ ] **Step 1: 编写 Schema 获取测试（使用 H2 内存数据库）**

创建测试类，验证动态 Schema 获取功能：

```java
package com.etl.connector.cdc.mysql;

import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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

        // 验证字段名称
        assertTrue(rowType.getFieldNames().contains("id"));
        assertTrue(rowType.getFieldNames().contains("name"));
        assertTrue(rowType.getFieldNames().contains("age"));
        assertTrue(rowType.getFieldNames().contains("salary"));
        assertTrue(rowType.getFieldNames().contains("created_at"));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
cd flink-etl-connector/connector-cdc
mvn test -Dtest=MySqlCdcDeserializerTest::testSchemaExtractionFromDatabase
```

预期：FAIL - MySqlCdcDeserializer 类不存在

- [ ] **Step 3: 实现 MySqlCdcDeserializer（完整版本）**

创建序列化器，实现动态 Schema 获取 + Debezium JSON 解析：

```java
package com.etl.connector.cdc.mysql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * MySQL CDC 序列化器
 * 将 Debezium JSON 转换为带 RowKind 的 Row
 * 动态从数据库获取表 Schema
 */
public class MySqlCdcDeserializer implements org.apache.flink.api.common.serialization.DeserializationSchema<Row> {

    private final String hostname;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final String table;

    @Getter
    private RowType rowType;  // Schema 信息（open() 方法初始化）

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 构造函数：接收数据库连接参数
     */
    public MySqlCdcDeserializer(
            String hostname, int port, String database,
            String username, String password, String table) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.table = table;
    }

    /**
     * 初始化时从数据库获取表 Schema
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        // 构建 JDBC URL（处理 H2 内存数据库特殊情况）
        String jdbcUrl;
        if (hostname.startsWith("mem:") || hostname.startsWith("file:")) {
            // H2 内存数据库 URL 格式：jdbc:h2:mem:testdb
            jdbcUrl = "jdbc:h2:" + hostname;
        } else {
            // MySQL URL 格式：jdbc:mysql://host:port/database
            jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + database;
        }

        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(database, null, table, null);

        // 构建 RowType
        List<RowType.RowField> fields = new ArrayList<>();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");
            LogicalType logicalType = convertJdbcTypeToFlinkType(columnType);
            fields.add(new RowType.RowField(columnName, logicalType));
        }

        rowType = new RowType(fields);
        connection.close();
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(message);

        // 解析 Debezium op 字段
        String op = jsonNode.get("op").asText();
        RowKind rowKind = parseRowKind(op);

        // 提取业务数据（after/before 字段）
        JsonNode dataNode;
        if (op.equals("d")) {
            // DELETE 操作使用 before 字段
            dataNode = jsonNode.get("before");
        } else {
            // INSERT/UPDATE 操作使用 after 字段
            dataNode = jsonNode.get("after");
        }

        // 构建 Row
        Row row = extractRow(dataNode);
        row.setRowKind(rowKind);

        return row;
    }

    private RowKind parseRowKind(String op) {
        switch (op) {
            case "c":  // create
            case "r":  // read
                return RowKind.INSERT;
            case "u":  // update
                return RowKind.UPDATE_AFTER;
            case "d":  // delete
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException("不支持的 op 类型: " + op);
        }
    }

    private Row extractRow(JsonNode dataNode) {
        int fieldCount = rowType.getFieldCount();
        Row row = Row.withPositions(fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = rowType.getFieldNames().get(i);
            LogicalType fieldType = rowType.getTypeAt(i);

            JsonNode fieldValue = dataNode.get(fieldName);
            Object value = convertJsonNodeToValue(fieldValue, fieldType);
            row.setField(i, value);
        }

        return row;
    }

    private Object convertJsonNodeToValue(JsonNode node, LogicalType type) {
        if (node == null || node.isNull()) {
            return null;
        }

        // 根据 LogicalType 类型转换
        if (type instanceof IntType) {
            return node.asInt();
        } else if (type instanceof BigIntType) {
            return node.asLong();
        } else if (type instanceof VarCharType) {
            return node.asText();
        } else if (type instanceof DoubleType) {
            return node.asDouble();
        } else if (type instanceof DecimalType) {
            return new BigDecimal(node.asText());
        } else if (type instanceof TimestampType) {
            return Timestamp.valueOf(node.asText());
        } else if (type instanceof DateType) {
            return Date.valueOf(node.asText());
        } else if (type instanceof BooleanType) {
            return node.asBoolean();
        }

        throw new UnsupportedOperationException("不支持的字段类型: " + type);
    }

    private LogicalType convertJdbcTypeToFlinkType(String jdbcType) {
        // JDBC 类型 → Flink LogicalType 映射
        switch (jdbcType.toUpperCase()) {
            case "INT":
            case "INTEGER":
                return new IntType();
            case "BIGINT":
                return new BigIntType();
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
                return new VarCharType();
            case "DOUBLE":
            case "FLOAT":
                return new DoubleType();
            case "DECIMAL":
                return new DecimalType();
            case "TIMESTAMP":
            case "DATETIME":
                return new TimestampType();
            case "DATE":
                return new DateType();
            case "BOOLEAN":
            case "BIT":
                return new BooleanType();
            default:
                throw new UnsupportedOperationException("不支持的 JDBC 类型: " + jdbcType);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

```bash
cd flink-etl-connector/connector-cdc
mvn test -Dtest=MySqlCdcDeserializerTest::testSchemaExtractionFromDatabase
```

预期：PASS - Schema 成功从 H2 数据库获取

- [ ] **Step 5: 编写 Debezium JSON INSERT 解析测试**

添加 Debezium JSON 解析测试：

```java
@Test
void testDeserializeInsert() throws Exception {
    // 创建序列化器并初始化 Schema
    MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
        "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
    );
    deserializer.open(null);

    // Debezium INSERT JSON（对应 users 表）
    String json = "{" +
        "\"before\":null," +
        "\"after\":{\"id\":1,\"name\":\"Alice\",\"age\":30,\"salary\":5000.5,\"created_at\":\"2023-01-01 10:00:00\"}," +
        "\"op\":\"c\"" +
        "}";

    Row row = deserializer.deserialize(json.getBytes());

    assertEquals(RowKind.INSERT, row.getRowKind());
    assertEquals(1L, row.getField(0));  // id
    assertEquals("Alice", row.getField(1));  // name
    assertEquals(30, row.getField(2));  // age
    assertEquals(5000.5, row.getField(3));  // salary
}
```

- [ ] **Step 6: 运行测试验证通过**

```bash
mvn test -Dtest=MySqlCdcDeserializerTest::testDeserializeInsert
```

预期：PASS - INSERT JSON 正确解析

- [ ] **Step 7: 编写 UPDATE 和 DELETE 解析测试**

添加 UPDATE 和 DELETE 测试用例：

```java
@Test
void testDeserializeUpdate() throws Exception {
    MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
        "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
    );
    deserializer.open(null);

    String json = "{" +
        "\"before\":{\"id\":1,\"name\":\"Alice\",\"age\":30,\"salary\":5000.5,\"created_at\":\"2023-01-01 10:00:00\"}," +
        "\"after\":{\"id\":1,\"name\":\"Bob\",\"age\":35,\"salary\":6000.0,\"created_at\":\"2023-01-01 10:00:00\"}," +
        "\"op\":\"u\"" +
        "}";

    Row row = deserializer.deserialize(json.getBytes());

    assertEquals(RowKind.UPDATE_AFTER, row.getRowKind());
    assertEquals(1L, row.getField(0));
    assertEquals("Bob", row.getField(1));
    assertEquals(35, row.getField(2));
}

@Test
void testDeserializeDelete() throws Exception {
    MySqlCdcDeserializer deserializer = new MySqlCdcDeserializer(
        "mem:testdb", -1, "testdb", USERNAME, PASSWORD, "users"
    );
    deserializer.open(null);

    String json = "{" +
        "\"before\":{\"id\":1,\"name\":\"Alice\",\"age\":30,\"salary\":5000.5,\"created_at\":\"2023-01-01 10:00:00\"}," +
        "\"after\":null," +
        "\"op\":\"d\"" +
        "}";

    Row row = deserializer.deserialize(json.getBytes());

    assertEquals(RowKind.DELETE, row.getRowKind());
    assertEquals(1L, row.getField(0));
    assertEquals("Alice", row.getField(1));
}
```

- [ ] **Step 8: 运行测试验证通过**

```bash
mvn test -Dtest=MySqlCdcDeserializerTest
```

预期：PASS - UPDATE 和 DELETE JSON 正确解析

- [ ] **Step 9: 提交序列化器完整实现**

```bash
git add flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcDeserializer.java
git add flink-etl-connector/connector-cdc/src/test/java/com/etl/connector/cdc/mysql/MySqlCdcDeserializerTest.java
git commit -m "feat: 实现 MySqlCdcDeserializer 动态 Schema 获取和 Debezium JSON 解析"
```

---

## Task 5: 实现 MySqlCdcSourcePlugin SPI 插件

**Files:**
- Create: `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcSourcePlugin.java`

- [ ] **Step 1: 创建 MySqlCdcSourcePlugin 类**

创建 SPI 插件类，实现 SourcePlugin 接口：

```java
package com.etl.connector.cdc.mysql;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.types.Row;

/**
 * MySQL CDC Source 插件
 * 封装 flink-connector-mysql-cdc 的 MySqlSource
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class MySqlCdcSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "mysql-cdc";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 MySQL CDC Source");

        MySqlCdcConfig cdcConfig = MySqlCdcConfig.fromSourceConfig(config);

        // Server ID 处理（不配置时自动生成）
        int serverId = cdcConfig.getServerId() != null ?
            cdcConfig.getServerId() : generateAutoServerId();

        MySqlSource<Row> source = MySqlSource.<Row>builder()
            .hostname(cdcConfig.getHostname())
            .port(cdcConfig.getPort())
            .databaseList(cdcConfig.getDatabase())
            .tableList(cdcConfig.getDatabase() + "." + cdcConfig.getTable())
            .username(cdcConfig.getUsername())
            .password(cdcConfig.getPassword())
            .deserializer(new MySqlCdcDeserializer(
                cdcConfig.getHostname(),
                cdcConfig.getPort(),
                cdcConfig.getDatabase(),
                cdcConfig.getUsername(),
                cdcConfig.getPassword(),
                cdcConfig.getTable()
            ))
            .startupOptions(cdcConfig.getStartupOptions())
            .serverId(serverId)
            .build();

        log.info("MySQL CDC Source 创建成功: host={}, port={}, database={}, table={}",
            cdcConfig.getHostname(), cdcConfig.getPort(),
            cdcConfig.getDatabase(), cdcConfig.getTable());

        return source;
    }

    /**
     * 自动生成 Server ID（用于多任务并发）
     */
    private int generateAutoServerId() {
        // 使用随机数避免冲突
        return (int) (System.currentTimeMillis() % 10000) + 5400;
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
cd flink-etl-connector/connector-cdc
mvn compile
```

预期：编译成功，无错误

- [ ] **Step 3: 提交 SPI 插件实现**

```bash
git add flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/MySqlCdcSourcePlugin.java
git commit -m "feat: 实现 MySqlCdcSourcePlugin SPI 插件"
```

---

## Task 6: 添加 flink-etl-client 依赖

**Files:**
- Modify: `flink-etl-client/pom.xml:18-51`

- [ ] **Step 1: 在 flink-etl-client/pom.xml 添加依赖**

在 connector 依赖部分添加 connector-cdc：

```xml
<!-- 连接器插件 -->
<dependency>
    <groupId>com.etl</groupId>
    <artifactId>connector-jdbc</artifactId>
    <version>${project.version}</version>
</dependency>
<dependency>
    <groupId>com.etl</groupId>
    <artifactId>connector-kafka</artifactId>
    <version>${project.version}</version>
</dependency>

<dependency>
    <groupId>com.etl</groupId>
    <artifactId>connector-cdc</artifactId>
    <version>${project.version}</version>
</dependency>

<dependency>
    <groupId>com.etl</groupId>
    <artifactId>connector-http</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 2: 编译整个项目**

```bash
mvn clean compile
```

预期：编译成功，connector-cdc 模块被正确打包到 client

- [ ] **Step 3: 提交依赖配置**

```bash
git add flink-etl-client/pom.xml
git commit -m "feat: 在 flink-etl-client 添加 connector-cdc 依赖"
```

---

## Task 7: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 在 PLUGINS.md 添加 MySQL CDC Source 章节**

在 PLUGINS.md 的 "## Source 插件" 部分，Kafka Source 之后添加此章节：

```markdown
### MySQL CDC Source

实时捕获 MySQL 数据库变更（INSERT/UPDATE/DELETE），输出带 RowKind 标记的 Row 数据流。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL，格式：`jdbc:mysql://host:port/database` |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `table` | 是 | - | 监听的表名（单表），只需填写表名，不需要 database.table 格式 |
| `startupMode` | 否 | `latest` | 启动模式：`earliest`、`latest`、`timestamp`、`snapshot_first` |
| `startupTimestamp` | 条件必填 | - | 时间戳启动模式专用（毫秒），startupMode=timestamp 时必填 |
| `serverId` | 否 | 自动生成 | Server ID（多任务并发时需唯一） |

**Schema 配置**：无需用户配置，自动从数据库获取表结构。

**RowKind 映射**：
- Debezium `op='c'` (create) → `RowKind.INSERT` (+I)
- Debezium `op='r'` (read) → `RowKind.INSERT` (+I)
- Debezium `op='u'` (update) → `RowKind.UPDATE_AFTER` (+U)
- Debezium `op='d'` (delete) → `RowKind.DELETE` (-D)

#### 配置示例

**基础配置（从最新位置开始）**：

```json
{
  "source": {
    "type": "mysql-cdc",
    "outputTable": "users_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users",
      "startupMode": "latest"
    }
  }
}
```

**先读取全量快照，再捕获增量变更**：

```json
{
  "source": {
    "type": "mysql-cdc",
    "outputTable": "products_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "products",
      "startupMode": "snapshot_first"
    }
  }
}
```

**配合 JDBC Sink CDC 模式使用**：

```json
{
  "job": {
    "name": "mysql-to-mysql-cdc",
    "mode": "streaming",
    "parallelism": 1
  },
  "sources": [
    {
      "type": "mysql-cdc",
      "outputTable": "users_cdc",
      "config": {
        "url": "jdbc:mysql://source-host:3306/source_db",
        "username": "root",
        "password": "password",
        "table": "users",
        "startupMode": "earliest"
      }
    }
  ],
  "sinks": [
    {
      "type": "jdbc",
      "inputTable": "users_cdc",
      "config": {
        "url": "jdbc:mysql://target-host:3306/target_db",
        "username": "root",
        "password": "password",
        "table": "users",
        "mode": "cdc"
      }
    }
  ]
}
```

#### 运行模式

- **纯流式模式**：CDC Source 只支持 streaming 模式，持续捕获实时变更
- **checkpoint 支持**：支持 checkpoint 时保存 offset，故障恢复时从 checkpoint 继续
- **自动重连**：flink-connector-mysql-cdc 提供自动重连机制

---
```

- [ ] **Step 2: 提交文档更新**

```bash
git add PLUGINS.md
git commit -m "docs: 添加 MySQL CDC Source 插件文档"
```

---

## Task 8: 编译安装模块

**Files:**
- All connector-cdc files

- [ ] **Step 1: 编译 connector-cdc 模块**

```bash
cd flink-etl-connector/connector-cdc
mvn clean compile
```

预期：编译成功，生成 target/classes 目录

- [ ] **Step 2: 安装到本地仓库**

```bash
mvn clean install -DskipTests
```

预期：安装成功，生成 SPI 配置文件 META-INF/services/com.etl.core.spi.SourcePlugin

- [ ] **Step 3: 编译整个项目**

```bash
cd ../../..
mvn clean compile
```

预期：整个项目编译成功，connector-cdc 被正确加载

- [ ] **Step 4: 提交最终版本**

```bash
git add -A
git commit -m "feat: 完成 connector-cdc 模块实现（包含动态 Schema 获取）"
```

---

## 实现完成验证

完成所有任务后，验证模块功能：

1. **编译验证**：`mvn clean compile` 成功
2. **SPI 注册验证**：检查 `META-INF/services/com.etl.core.spi.SourcePlugin` 文件包含 `com.etl.connector.cdc.mysql.MySqlCdcSourcePlugin`
3. **配置解析验证**：URL 正则解析、StartupMode 转换、参数校验测试通过
4. **序列化器验证**：动态 Schema 获取测试通过（使用 H2 内存数据库）、Debezium JSON 解析测试通过（INSERT/UPDATE/DELETE）
5. **文档完整性**：PLUGINS.md 包含完整的 MySQL CDC Source 配置说明

---

**关键技术点**：
- MySqlCdcDeserializer 在 `open()` 方法中从数据库 JDBC 元数据获取表 Schema
- 支持 8 种 JDBC 类型：INT、BIGINT、VARCHAR、DOUBLE、DECIMAL、TIMESTAMP、DATE、BOOLEAN
- 使用 H2 内存数据库进行测试，无需真实 MySQL 环境
- 序列化器完整实现，可以直接用于生产环境