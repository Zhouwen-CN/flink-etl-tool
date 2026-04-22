# connector-cdc 模块设计文档

## 概述

新增 connector-cdc 模块，封装 `flink-connector-mysql-cdc`，实现 MySQL CDC Source，支持实时捕获数据库变更（INSERT/UPDATE/DELETE），输出带 RowKind 标记的 Row 数据流。

## 设计方案

采用**最小封装方案**，直接使用 flink-connector-mysql-cdc 的 MySqlSource，类似 Kafka Source 的成熟设计模式。

**核心定位**：
- 实时数据库同步：捕获数据库变更，实时同步到其他系统（Kafka/JDBC 等）
- 数据采集：作为流式数据源，与 JDBC Source（批处理）形成互补
- CDC 事件处理：输出带 RowKind 标记的 Row（+I/-U/+U/-D），配合 JDBC Sink 的 CDC 模式写入目标库

**单表捕获模式**：每个 CDC Source 配置对应一张表的变更，多个表需要配置多个 Source。

**优势**：
- 复用成熟的 flink-connector-mysql-cdc 实现，稳定可靠
- 代码量最小，只需创建 SPI 插件、配置解析和序列化器
- 自动获得 CDC Source 的所有特性（checkpoint、故障恢复、自动重连等）
- 易于扩展其他 CDC（如 PostgreSQL、Oracle）

## 使用场景

- 实时数据库同步：MySQL → Kafka、MySQL → MySQL、MySQL → 其他数据库
- 数据变更事件处理：捕获 INSERT/UPDATE/DELETE 事件，进行实时分析
- 流式 ETL：CDC 数据流配合 SQL Transform 进行数据清洗和转换
- 配合 JDBC Sink CDC 模式：实现端到端的数据库实时同步

## 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL，格式：`jdbc:mysql://host:port/database` |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `table` | 是 | - | 监听的表名（单表），只需填写表名，不需要 database.table 格式 |
| `startupMode` | 否 | `latest` | 启动模式：`earliest`、`latest`、`timestamp`、`snapshot_first` |
| `startupTimestamp` | 条件必填 | - | 时间戳启动模式专用（毫秒），startupMode=timestamp 时必填 |
| `serverId` | 否 | 自动生成 | Server ID（多任务并发时需唯一），不配置时自动生成 |

**Schema 配置**：无需用户配置，自动从数据库获取表结构。

**URL 正则解析**：
- 正则模式：`jdbc:mysql://([^:]+):(\d+)/([^?]+)`
- 提取：hostname、port、database
- 示例：`jdbc:mysql://localhost:3306/test_db` → hostname=localhost, port=3306, database=test_db

## 配置示例

### 基础配置（从最新位置开始）

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

### 从最早位置开始（读取历史变更）

```json
{
  "source": {
    "type": "mysql-cdc",
    "outputTable": "orders_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "orders",
      "startupMode": "earliest"
    }
  }
}
```

### 先读取全量快照，再捕获增量变更

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

### 从指定时间戳开始

```json
{
  "source": {
    "type": "mysql-cdc",
    "outputTable": "events_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "events",
      "startupMode": "timestamp",
      "startupTimestamp": 1234567890000
    }
  }
}
```

### 配合 JDBC Sink CDC 模式使用

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

## 模块结构

```
flink-etl-connector/connector-cdc/
├── pom.xml
└── src/main/java/com/etl/connector/cdc/
    ├── CdcSourcePlugin.java              # SPI 插件入口（通用 CDC 抽象）
    └── mysql/
        ├── MySqlCdcSourcePlugin.java     # MySQL CDC 专用插件实现
        ├── MySqlCdcConfig.java           # MySQL CDC 配置解析类
        └── MySqlCdcDeserializer.java     # Debezium JSON → Row 序列化器
```

**模块职责**：
- `CdcSourcePlugin`：通用 CDC Source 接口（为未来扩展预留）
- `mysql/` 子包：MySQL CDC 专用实现（符合包名规范）
- `MySqlCdcConfig`：配置解析 + URL 正则提取 hostname/port/database
- `MySqlCdcDeserializer`：实现 `DeserializationSchema<Row>`，输出带 RowKind 的 Row

## Maven 配置

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

        <!-- flink-etl-core -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-core</artifactId>
            <version>${project.version}</version>
        </dependency>
    </dependencies>
</project>
```

**关键依赖说明**：
- `com.ververica:flink-connector-mysql-cdc:2.3.0`：兼容 Flink 1.15
- MySQL 驱动：用于序列化器初始化时获取表 Schema
- flink-etl-core：SourcePlugin 接口、Row 类型等

## 核心组件

### MySqlCdcSourcePlugin

实现 `SourcePlugin` 接口，职责：
- 提供 `mysql-cdc` 类型标识
- 创建 MySqlSource 实例
- 配置启动模式、序列化器

```java
@AutoService(SourcePlugin.class)
public class MySqlCdcSourcePlugin implements SourcePlugin {
    @Override
    public String identifier() {
        return "mysql-cdc";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        MySqlCdcConfig cdcConfig = MySqlCdcConfig.fromSourceConfig(config);

        return MySqlSource.<Row>builder()
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
            .serverId(cdcConfig.getServerId() != null ? cdcConfig.getServerId() : generateAutoServerId())
            .build();
    }
}
```

**说明**：
- 使用 flink-connector-mysql-cdc 的 `MySqlSource.builder()` 构建 Source
- 序列化器传入数据库连接参数，用于 Schema 自动获取

### MySqlCdcConfig

配置封装类，职责：
- 从 SourceConfig 解析配置参数
- URL 正则解析：提取 hostname、port、database
- 参数校验
- 提供 StartupOptions 转换

```java
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

    public static MySqlCdcConfig fromSourceConfig(SourceConfig config) {
        Map<String, Object> configMap = config.getConfig();

        // 解析 URL（使用静态解析方法）
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
            startupTimestamp = (Long) configMap.get("startupTimestamp");
            if (startupTimestamp == null) {
                throw new IllegalArgumentException("startupMode=timestamp 时必须配置 startupTimestamp");
            }
        }

        // serverId（可选）
        Integer serverId = (Integer) configMap.get("serverId");

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

    // 静态解析方法，返回解析结果对象
    private static UrlParseResult parseUrl(String url) {
        // URL 格式校验
        if (!url.startsWith("jdbc:mysql://")) {
            throw new IllegalArgumentException("URL 必须以 jdbc:mysql:// 开头");
        }

        // 正则解析：jdbc:mysql://host:port/database
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

    // URL 解析结果内部类
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
}
```

**启动模式枚举**：
```java
public enum StartupMode {
    EARLIEST,        // 从最早可用位置开始
    LATEST,          // 从最新位置开始
    TIMESTAMP,       // 从指定时间戳开始
    SNAPSHOT_FIRST   // 先读取全量快照，再捕获增量变更
}
```

### MySqlCdcDeserializer

实现 `DeserializationSchema<Row>`，职责：
- 初始化时从数据库获取表 Schema
- Debezium JSON → Row 转换
- 根据 Debezium op 字段设置 RowKind（+I/-U/+U/-D）

```java
public class MySqlCdcDeserializer implements DeserializationSchema<Row> {
    private final String hostname;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final String table;

    private RowType rowType;  // Schema 信息
    private ObjectMapper objectMapper = new ObjectMapper();

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

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        // 从数据库获取表 Schema
        Connection connection = DriverManager.getConnection(
            "jdbc:mysql://" + hostname + ":" + port + "/" + database,
            username, password
        );

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(database, null, table, null);

        // 构建 RowType
        List<RowField> fields = new ArrayList<>();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");
            LogicalType logicalType = convertJdbcTypeToFlinkType(columnType);
            fields.add(new RowField(columnName, logicalType));
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
        JsonNode dataNode = null;
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
        return RowTypeInfo.of(rowType);
    }
}
```

**说明**：
- `open()` 方法在初始化时从数据库获取表 Schema
- `deserialize()` 方法将 Debezium JSON 转换为带 RowKind 的 Row
- RowKind 映射：`c/r` → INSERT, `u` → UPDATE_AFTER, `d` → DELETE
- DELETE 操作使用 `before` 字段，其他操作使用 `after` 字段

## 数据流转机制

### CDC 数据流转

```
MySQL Binlog
  ↓
flink-connector-mysql-cdc（MySqlSource）
  ↓
Debezium JSON 格式
  ↓
MySqlCdcDeserializer（序列化器）
  ↓
Row（with RowKind）
  ↓
Flink Table（outputTable）
```

### RowKind 映射逻辑

| Debezium op | 说明 | RowKind | Flink 标记 |
|-------------|------|---------|-----------|
| `c` | create（新增） | `RowKind.INSERT` | +I |
| `r` | read（快照读取） | `RowKind.INSERT` | +I |
| `u` | update（更新） | `RowKind.UPDATE_AFTER` | +U |
| `d` | delete（删除） | `RowKind.DELETE` | -D |

### Debezium JSON 结构示例

**INSERT 操作**：
```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com"
  },
  "op": "c",
  "ts_ms": 1234567890
}
```

**UPDATE 操作**：
```json
{
  "before": {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com"
  },
  "after": {
    "id": 1,
    "name": "Alice Updated",
    "email": "alice.updated@example.com"
  },
  "op": "u",
  "ts_ms": 1234567891
}
```

**DELETE 操作**：
```json
{
  "before": {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com"
  },
  "after": null,
  "op": "d",
  "ts_ms": 1234567892
}
```

### 下游配合使用

**配合 JDBC Sink CDC 模式**：
- JDBC Sink 的 CDC 模式可直接消费 CDC Source 的输出
- JDBC Sink 根据 RowKind 执行对应操作：
  - INSERT/UPDATE_AFTER → 执行 UPSERT SQL
  - DELETE → 执行 DELETE SQL

**配合 Kafka Sink**：
- CDC 数据写入 Kafka，供其他系统消费
- Kafka Sink 使用 JSON 格式序列化 Row（包含 RowKind）

## Schema 自动获取机制

### 获取时机

在 `MySqlCdcDeserializer.open()` 初始化时从数据库获取表 Schema，使用 JDBC 连接查询表的元数据。

### 获取逻辑

```java
Connection connection = DriverManager.getConnection(
    "jdbc:mysql://" + hostname + ":" + port + "/" + database,
    username, password
);

DatabaseMetaData metaData = connection.getMetaData();
ResultSet columns = metaData.getColumns(database, null, table, null);

List<RowField> fields = new ArrayList<>();
while (columns.next()) {
    String columnName = columns.getString("COLUMN_NAME");
    String columnType = columns.getString("TYPE_NAME");
    LogicalType logicalType = convertJdbcTypeToFlinkType(columnType);
    fields.add(new RowField(columnName, logicalType));
}

rowType = new RowType(fields);
connection.close();
```

### 支持的字段类型

| JDBC 类型 | Flink LogicalType |
|-----------|-------------------|
| INT、INTEGER | IntType |
| BIGINT | BigIntType |
| VARCHAR、CHAR、TEXT | VarCharType |
| DOUBLE、FLOAT | DoubleType |
| DECIMAL | DecimalType |
| TIMESTAMP、DATETIME | TimestampType |
| DATE | DateType |
| BOOLEAN、BIT | BooleanType |

## 错误处理机制

### 配置校验

| 参数 | 校验规则 |
|------|---------|
| `url` | 必须以 `jdbc:mysql://` 开头，格式符合正则 `jdbc:mysql://([^:]+):(\d+)/([^?]+)` |
| `username` | 不能为 null |
| `password` | 不能为 null |
| `table` | 不能为 null |
| `startupMode` | 必须是 `earliest`、`latest`、`timestamp`、`snapshot_first` 之一 |
| `startupTimestamp` | startupMode=timestamp 时必须配置 |

**校验示例**：
```java
// URL 格式校验
if (!url.startsWith("jdbc:mysql://")) {
    throw new IllegalArgumentException("URL 必须以 jdbc:mysql:// 开头");
}

// 正则解析失败
if (!matcher.matches()) {
    throw new IllegalArgumentException("URL 格式错误，应为 jdbc:mysql://host:port/database");
}

// timestamp 模式校验
if (startupMode == StartupMode.TIMESTAMP && startupTimestamp == null) {
    throw new IllegalArgumentException("startupMode=timestamp 时必须配置 startupTimestamp");
}
```

### 运行时异常处理

| 场景 | 处理方式 |
|------|---------|
| Schema 获取失败 | 抛出异常，任务失败（表不存在或连接失败） |
| JSON 解析失败 | 抛出 `IOException`，Flink 从 checkpoint 重试 |
| 数据库连接断开 | flink-connector-mysql-cdc 自动重连机制 |
| 不支持的 op 类型 | 抛出 `IllegalArgumentException` |
| 不支持的字段类型 | 抛出 `UnsupportedOperationException` |

**异常处理示例**：
```java
// Schema 获取失败
try {
    connection = DriverManager.getConnection(...);
} catch (SQLException e) {
    throw new RuntimeException("无法连接数据库获取 Schema: " + e.getMessage());
}

// JSON 解析失败
JsonNode jsonNode = objectMapper.readTree(message);
if (jsonNode == null || !jsonNode.has("op")) {
    throw new IOException("Debezium JSON 格式错误，缺少 op 字段");
}
```

## 测试策略

### 单元测试范围

**MySqlCdcConfigTest**：
- URL 正则解析测试（各种格式）
- 配置参数解析测试
- startupMode 转换测试
- timestamp 模式校验测试

**MySqlCdcDeserializerTest**：
- Debezium JSON 解析测试（各种 op 类型）
- RowKind 映射测试（c/r/u/d → INSERT/UPDATE_AFTER/DELETE）
- Row 数据提取测试（after/before 字段）
- 字段类型转换测试

### 测试数据准备

- 使用 Mock Debezium JSON 数据（不需要真实 MySQL）
- Schema 获取测试使用 H2 内存数据库或 Mock
- URL 解析测试使用静态字符串数据

### 测试示例

**URL 解析测试**：
```java
@Test
public void testParseUrl() {
    String url = "jdbc:mysql://localhost:3306/mydb";
    MySqlCdcConfig config = MySqlCdcConfig.fromSourceConfig(createSourceConfig(url));

    assertEquals("localhost", config.getHostname());
    assertEquals(3306, config.getPort());
    assertEquals("mydb", config.getDatabase());
}

@Test
public void testParseUrlWithInvalidFormat() {
    String url = "jdbc:postgresql://localhost:5432/mydb";
    assertThrows(IllegalArgumentException.class, () -> {
        MySqlCdcConfig.fromSourceConfig(createSourceConfig(url));
    });
}
```

**Debezium JSON 解析测试**：
```java
@Test
public void testDeserializeInsert() throws IOException {
    String json = "{\"after\":{\"id\":1,\"name\":\"Alice\"},\"op\":\"c\"}";
    Row row = deserializer.deserialize(json.getBytes());

    assertEquals(RowKind.INSERT, row.getRowKind());
    assertEquals(1, row.getField(0));
    assertEquals("Alice", row.getField(1));
}

@Test
public void testDeserializeUpdate() throws IOException {
    String json = "{\"before\":{\"id\":1,\"name\":\"Alice\"},\"after\":{\"id\":1,\"name\":\"Bob\"},\"op\":\"u\"}";
    Row row = deserializer.deserialize(json.getBytes());

    assertEquals(RowKind.UPDATE_AFTER, row.getRowKind());
    assertEquals(1, row.getField(0));
    assertEquals("Bob", row.getField(1));
}

@Test
public void testDeserializeDelete() throws IOException {
    String json = "{\"before\":{\"id\":1,\"name\":\"Alice\"},\"after\":null,\"op\":\"d\"}";
    Row row = deserializer.deserialize(json.getBytes());

    assertEquals(RowKind.DELETE, row.getRowKind());
    assertEquals(1, row.getField(0));
    assertEquals("Alice", row.getField(1));
}
```

**StartupMode 转换测试**：
```java
@Test
public void testStartupOptionsEarliest() {
    MySqlCdcConfig config = MySqlCdcConfig.builder()
        .startupMode(StartupMode.EARLIEST)
        .build();

    StartupOptions options = config.getStartupOptions();
    assertNotNull(options);
}
```

## 文档更新

实现完成后需更新 `PLUGINS.md`，添加 MySQL CDC Source 配置说明，包括：
- 配置参数说明
- 配置示例（各种启动模式）
- Schema 自动获取说明
- 配合 JDBC Sink CDC 模式使用示例
- RowKind 映射说明

## flink-etl-client 依赖配置

需要在 `flink-etl-client/pom.xml` 中添加 connector-cdc 依赖：

```xml
<dependency>
    <groupId>com.etl</groupId>
    <artifactId>connector-cdc</artifactId>
    <version>${project.version}</version>
</dependency>
```

## 并行度说明

flink-connector-mysql-cdc 会自动处理并行度：
- **单并行度模式**：传统 CDC 模式，binlog 读取只能单并行度
- **并行快照模式**：全量快照阶段可以多并行度并行读取，binlog 阶段单并行度

用户无需关注并行度细节，直接使用 flink-connector-mysql-cdc 的能力。

## 运行模式

- **纯流式模式**：CDC Source 只支持 streaming 模式，持续捕获实时变更
- **checkpoint 支持**：支持 checkpoint 时保存 offset，故障恢复时从 checkpoint 继续
- **自动重连**：flink-connector-mysql-cdc 提供自动重连机制

## 扩展性设计

当前只实现 MySQL CDC，未来可扩展其他 CDC：

**扩展 PostgreSQL CDC**：
- 添加 `postgres/` 子包
- 实现 `PostgresCdcSourcePlugin`、`PostgresCdcConfig`、`PostgresCdcDeserializer`
- 使用 `flink-connector-postgres-cdc`

**扩展 Oracle CDC**：
- 添加 `oracle/` 子包
- 实现 `OracleCdcSourcePlugin`、`OracleCdcConfig`、`OracleCdcDeserializer`
- 使用 `flink-connector-oracle-cdc`

顶层 `CdcSourcePlugin` 接口为未来扩展预留抽象能力。