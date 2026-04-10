# Debezium CDC 数据支持设计文档

**日期**: 2026-04-10
**作者**: Claude & User
**状态**: 待审批

---

## 1. 概述

### 1.1 背景

当前项目已经具备完整的 ETL 数据流转能力：
- Kafka Source 可以消费 JSON 格式消息
- Transform 支持基于 Table API 的 SQL 转换
- JDBC Sink 支持 INSERT 和 UPSERT 写入模式

但在实际生产场景中，大量用户使用 Debezium 等 CDC 工具捕获数据库变更，数据以 CDC 格式（包含 operation type、before/after 数据）存储在 Kafka Topic 中。当前框架无法直接处理这种场景。

### 1.2 目标

支持从 Kafka 消费 Debezium CDC 格式数据，通过 Transform 处理后，根据 RowKind 自动写入 JDBC 数据库（INSERT/UPDATE/DELETE），实现完整的 CDC 数据同步链路。

### 1.3 设计原则

- **标准化**: 使用 Flink 标准 RowKind 机制表示 Changelog
- **模块化**: Format SPI 封装在 Kafka Source 模块内部，保持清晰的模块边界
- **可扩展**: 通过 SPI 支持多种 CDC 格式（Debezium、OGG 等）
- **易用性**: Schema 配置简化，用户只需配置业务数据结构
- **向后兼容**: 现有功能保持不变，新功能为增量扩展

---

## 2. 技术方案

### 2.1 核心机制：RowKind

Flink Row 类型包含 `RowKind` 属性，用于表示 Changelog 操作类型：

| RowKind | 表示符号 | 说明 | SQL 映射 |
|---------|---------|------|---------|
| INSERT | +I | 插入操作 | INSERT INTO |
| UPDATE_BEFORE | -U | 更新前数据（通常忽略） | - |
| UPDATE_AFTER | +U | 更新后数据 | UPDATE |
| DELETE | -D | 删除操作 | DELETE FROM |

**Debezium op 字段映射规则:**

- `op='c'` (create) → RowKind.INSERT (+I)
- `op='r'` (read, initial snapshot) → RowKind.INSERT (+I)
- `op='u'` (update) → RowKind.UPDATE_AFTER (+U)
- `op='d'` (delete) → RowKind.DELETE (-D)

**数据提取规则:**

- INSERT/UPDATE_AFTER: 使用 Debezium `after` 字段数据
- DELETE: 使用 Debezium `before` 字段数据

---

## 3. 架构设计

### 3.1 整体架构

```
┌─────────────────────────────┐
│  Kafka Source Module        │
│                             │
│  ┌───────────────────────┐  │
│  │  Format SPI (内部)     │  │
│  │  - KafkaFormatPlugin  │  │
│  │  - JsonFormatPlugin   │  │
│  │  - DebeziumFormat     │  │
│  │  - KafkaFormatLoader  │  │
│  └───────────────────────┘  │
│                             │
│  Kafka Source Plugin        │
│  - 读取 format 配置         │
│  - 加载 Format Plugin       │
│  - 创建 Deserializer        │
│                             │
└─────────────────────────────┘
         │
         │ Row (with RowKind)
         ▼
┌─────────────────────────────┐
│  Transform Layer            │
│                             │
│  Table API 自动传递         │
│  RowKind，用户可用          │
│  SQL 处理数据               │
│                             │
└─────────────────────────────┘
         │
         │ Row (RowKind 保留)
         ▼
┌─────────────────────────────┐
│  JDBC Sink Module           │
│                             │
│  WriteMode.CDC (新增)       │
│  - INSERT → INSERT SQL      │
│  - UPDATE → UPDATE SQL      │
│  - DELETE → DELETE SQL      │
│                             │
│  批量执行优化：              │
│  分类型缓存 + flush         │
│                             │
└─────────────────────────────┘
```

### 3.2 模块结构

```
flink-etl-source-kafka/
├── src/main/java/com/etl/source/kafka/
│   ├── KafkaSourcePlugin.java
│   ├── KafkaSourceConfig.java  (新增 format 字段)
│   ├── format/  (新增包)
│   │   ├── KafkaFormatPlugin.java  (SPI 接口)
│   │   ├── KafkaFormatLoader.java  (加载器)
│   │   ├── JsonFormatPlugin.java
│   │   ├── JsonToRowDeserializationSchema.java
│   │   ├── DebeziumJsonFormatPlugin.java
│   │   ├── DebeziumJsonDeserializationSchema.java
│   │   └── StartupMode.java
│   └── resources/
│       └── META-INF/services/
│           ├── com.etl.core.spi.SourcePlugin
│           └── (AutoService 自动生成)

flink-etl-sink-jdbc/
├── src/main/java/com/etl/sink/jdbc/
│   ├── JdbcSinkPlugin.java
│   ├── JdbcSinkConfig.java  (已有 keyFields)
│   ├── JdbcSinkWriter.java  (新增 CDC 逻辑)
│   └── dialect/
│       ├── WriteMode.java  (新增 CDC enum)
│       └── JdbcDialect.java  (新增方法)
```

---

## 4. Kafka Source Format SPI 设计

### 4.1 KafkaFormatPlugin 接口

**位置**: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/KafkaFormatPlugin.java`

```java
package com.etl.source.kafka.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * Kafka 消息格式反序列化器 SPI 接口
 * 定义在 Kafka Source 模块内部，不污染 core 模块
 */
public interface KafkaFormatPlugin {
    /**
     * Format 标识符
     *
     * @return 格式名称，如 "json", "debezium-json", "ogg-json"
     */
    String identifier();

    /**
     * 创建反序列化器
     *
     * @param schema 业务数据的 schema（before/after 的数据结构，不包括 CDC 元数据）
     * @return KafkaRecordDeserializationSchema 实例
     */
    KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema);
}
```

### 4.2 JsonFormatPlugin 实现

**位置**: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/JsonFormatPlugin.java`

```java
package com.etl.source.kafka.format;

import com.etl.core.schema.EtlSchema;
import com.google.auto.service.AutoService;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * 标准 JSON 格式反序列化器插件
 * 将 Kafka 消息 JSON 直接解析为 Row，RowKind 默认为 INSERT
 */
@AutoService(KafkaFormatPlugin.class)
public class JsonFormatPlugin implements KafkaFormatPlugin {

    @Override
    public String identifier() {
        return "json";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        return new JsonToRowDeserializationSchema(schema);
    }
}
```

### 4.3 DebeziumJsonFormatPlugin 实现

**位置**: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/DebeziumJsonFormatPlugin.java`

```java
package com.etl.source.kafka.format;

import com.etl.core.schema.EtlSchema;
import com.google.auto.service.AutoService;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

/**
 * Debezium CDC JSON 格式反序列化器插件
 * 解析 Debezium JSON 结构，设置 RowKind，提取 after/before 数据
 */
@AutoService(KafkaFormatPlugin.class)
public class DebeziumJsonFormatPlugin implements KafkaFormatPlugin {

    @Override
    public String identifier() {
        return "debezium-json";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        return new DebeziumJsonDeserializationSchema(schema);
    }
}
```

### 4.4 DebeziumJsonDeserializationSchema 核心实现

**位置**: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/DebeziumJsonDeserializationSchema.java`

```java
package com.etl.source.kafka.format;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.utils.JsonUtils;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * Debezium JSON 反序列化器
 * 解析 Debezium CDC JSON 结构，设置 RowKind，提取业务数据
 */
public class DebeziumJsonDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final EtlSchema schema;  // 业务数据 schema

    public DebeziumJsonDeserializationSchema(EtlSchema schema) {
        this.schema = schema;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            return;
        }

        // 解析 Debezium JSON
        JsonNode debeziumJson = JsonUtils.readTree(record.value());

        // 提取操作类型
        String op = debeziumJson.get("op").asText();
        RowKind rowKind = mapOpToRowKind(op);

        // 根据操作类型提取数据源
        JsonNode dataNode;
        if ("d".equals(op)) {
            dataNode = debeziumJson.get("before");
        } else {
            dataNode = debeziumJson.get("after");
        }

        if (dataNode == null || dataNode.isNull()) {
            // before/after 可能为 null（某些场景）
            return;
        }

        // 转换为 Row
        Row row = JsonToRowConverter.convertJsonToRow(dataNode, schema);
        row.setRowKind(rowKind);

        out.collect(row);
    }

    /**
     * Debezium op 字段映射到 Flink RowKind
     */
    private RowKind mapOpToRowKind(String op) {
        switch (op) {
            case "c":  // create
            case "r":  // read (initial snapshot)
                return RowKind.INSERT;
            case "u":  // update
                return RowKind.UPDATE_AFTER;
            case "d":  // delete
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException(
                    String.format("未知的 Debezium op 类型: '%s'，支持的操作: c, r, u, d", op)
                );
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        // 返回业务数据 Row 的类型信息
        return schema.getRowTypeInfo();
    }
}
```

### 4.5 KafkaFormatLoader 加载器

**位置**: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/KafkaFormatLoader.java`

```java
package com.etl.source.kafka.format;

import java.util.ServiceLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

/**
 * Kafka Format Plugin 加载器
 * 使用 ServiceLoader 加载所有通过 @AutoService 注册的实现类
 */
public class KafkaFormatLoader {

    // 缓存已加载的 Plugin，避免重复加载
    private static volatile Map<String, KafkaFormatPlugin> formatPlugins;

    /**
     * 加载所有 Format Plugin 并缓存
     */
    private static void loadAllPlugins() {
        if (formatPlugins != null) {
            return;
        }

        synchronized (KafkaFormatLoader.class) {
            if (formatPlugins != null) {
                return;
            }

            Map<String, KafkaFormatPlugin> plugins = new HashMap<>();
            ServiceLoader<KafkaFormatPlugin> loader = ServiceLoader.load(KafkaFormatPlugin.class);

            for (KafkaFormatPlugin plugin : loader) {
                plugins.put(plugin.identifier(), plugin);
            }

            formatPlugins = plugins;
        }
    }

    /**
     * 根据格式名称获取 Plugin
     *
     * @param format 格式名称（如 "json", "debezium-json"）
     * @return KafkaFormatPlugin 实例，如果未找到则返回 null
     */
    public static KafkaFormatPlugin getFormatPlugin(String format) {
        loadAllPlugins();
        return formatPlugins.get(format);
    }

    /**
     * 列出所有支持的格式
     *
     * @return 支持的格式名称列表
     */
    public static String[] supportedFormats() {
        loadAllPlugins();
        return formatPlugins.keySet().toArray(new String[0]);
    }
}
```

### 4.6 KafkaSourceConfig 扩展

**位置**: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourceConfig.java`

```java
@Getter
@Builder
public class KafkaSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // 现有字段...
    private final String bootstrapServers;
    private final String groupId;
    private final List<String> topics;
    private final String topicPattern;
    private final StartupMode startupMode;
    private final Properties kafkaProperties;
    private final EtlSchema schema;

    // 新增字段
    private final String format;  // 消息格式：json、debezium-json 等

    /**
     * 从 SourceConfig 解析配置
     */
    public static KafkaSourceConfig fromSourceConfig(SourceConfig config) {
        // 现有解析逻辑...
        String bootstrapServers = config.getString("bootstrapServers");
        // ... 其他字段解析

        // 新增：解析 format（默认 "json"）
        String format = config.getString("format", "json");

        // 校验 format 是否支持
        KafkaFormatPlugin formatPlugin = KafkaFormatLoader.getFormatPlugin(format);
        if (formatPlugin == null) {
            String[] supported = KafkaFormatLoader.supportedFormats();
            throw new IllegalArgumentException(
                String.format("不支持的 Kafka format: '%s'，支持的格式: %s",
                    format, Arrays.toString(supported))
            );
        }

        // Schema 校验（必须有 schema，且只配置业务数据结构）
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            throw new IllegalArgumentException("schema 不能为空");
        }

        return KafkaSourceConfig.builder()
                .bootstrapServers(bootstrapServers)
                .groupId(groupId)
                .topics(topics)
                .topicPattern(topicPattern)
                .startupMode(startupMode)
                .kafkaProperties(kafkaProperties)
                .schema(schema)
                .format(format)  // 新增
                .build();
    }
}
```

### 4.7 KafkaSourcePlugin 改造

**位置**: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourcePlugin.java`

```java
@AutoService(SourcePlugin.class)
public class KafkaSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "kafka";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        log.info("创建 Kafka Source");

        KafkaSourceConfig kafkaConfig = KafkaSourceConfig.fromSourceConfig(config);

        // 加载 Format Plugin
        KafkaFormatPlugin formatPlugin = KafkaFormatLoader.getFormatPlugin(kafkaConfig.getFormat());
        if (formatPlugin == null) {
            throw new IllegalArgumentException("不支持的 format: " + kafkaConfig.getFormat());
        }

        // 创建反序列化器（schema 是业务数据 schema）
        KafkaRecordDeserializationSchema<Row> deserializer =
            formatPlugin.createDeserializer(kafkaConfig.getSchema());

        log.info("Kafka Source format: {}", kafkaConfig.getFormat());

        // 构建 KafkaSource
        KafkaSourceBuilder<Row> builder = KafkaSource.<Row>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setGroupId(kafkaConfig.getGroupId())
                .setStartingOffsets(kafkaConfig.getOffsetsInitializer())
                .setDeserializer(deserializer);

        // Topic 配置...
        // Kafka 属性配置...

        return builder.build();
    }
}
```

---

## 5. JDBC Sink CDC Mode 设计

### 5.1 WriteMode 扩展

**位置**: `flink-etl-core/src/main/java/com/etl/core/dialect/WriteMode.java`

```java
package com.etl.core.dialect;

/**
 * JDBC Sink 写入模式
 */
public enum WriteMode {
    /**
     * 插入模式，直接插入数据
     */
    INSERT,

    /**
     * Upsert 模式，存在则更新，不存在则插入
     */
    UPSERT,

    /**
     * CDC 模式，根据 RowKind 执行 INSERT/UPDATE/DELETE（新增）
     */
    CDC
}
```

### 5.2 JdbcDialect 接口扩展

**位置**: `flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialect.java`

```java
public interface JdbcDialect {
    // 现有方法
    String getInsertSql(String table, String[] columns);
    String getUpsertSql(String table, String[] columns, List<String> keyFields);

    // 新增 CDC 方法
    /**
     * 构建 UPDATE SQL
     *
     * @param table 表名
     * @param columns 所有列名
     * @param keyFields 主键/唯一键字段列表（用于 WHERE 条件）
     * @return UPDATE SQL
     */
    String getUpdateSql(String table, String[] columns, List<String> keyFields);

    /**
     * 构建 DELETE SQL
     *
     * @param table 表名
     * @param keyFields 主键/唯一键字段列表（用于 WHERE 条件）
     * @return DELETE SQL
     */
    String getDeleteSql(String table, List<String> keyFields);
}
```

### 5.3 MySQLDialect CDC 方法实现

**位置**: `flink-etl-core/src/main/java/com/etl/core/dialect/MySQLDialect.java`

```java
@Override
public String getUpdateSql(String table, String[] columns, List<String> keyFields) {
    // UPDATE table SET col1=?, col2=? WHERE key1=? AND key2=?

    String setClause = Arrays.stream(columns)
        .filter(col -> !keyFields.contains(col))
        .map(col -> quoteIdentifier(col) + " = ?")
        .collect(Collectors.joining(", "));

    String whereClause = keyFields.stream()
        .map(key -> quoteIdentifier(key) + " = ?")
        .collect(Collectors.joining(" AND "));

    return String.format("UPDATE %s SET %s WHERE %s",
        quoteIdentifier(table), setClause, whereClause);
}

@Override
public String getDeleteSql(String table, List<String> keyFields) {
    // DELETE FROM table WHERE key1=? AND key2=?

    String whereClause = keyFields.stream()
        .map(key -> quoteIdentifier(key) + " = ?")
        .collect(Collectors.joining(" AND "));

    return String.format("DELETE FROM %s WHERE %s",
        quoteIdentifier(table), whereClause);
}
```

### 5.4 JdbcSinkWriter CDC 实现

**位置**: `flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java`

```java
public class JdbcSinkWriter extends AbstractSinkWriter<JdbcSinkConfig> {

    private final transient Connection connection;

    // 现有字段
    private transient PreparedStatement normalStatement;
    private transient String[] normalColumns;

    // CDC 模式专用字段
    private transient Map<RowKind, PreparedStatement> cdcStatements;
    private transient Map<RowKind, List<Row>> cdcBatchBuffer;

    private final int batchSize;
    private int pendingCount = 0;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
        super(context, config);
        this.batchSize = config.getBatchSize();

        // 初始化数据库连接
        try {
            connection = DriverManager.getConnection(
                config.getUrl(),
                config.getUsername(),
                config.getPassword()
            );
            connection.setAutoCommit(false);

            // CDC 模式：初始化分类型缓存
            if (config.getMode() == WriteMode.CDC) {
                cdcStatements = new HashMap<>();
                cdcBatchBuffer = new HashMap<>();
            }

            log.info("JDBC Sink 已连接: url={}, mode={}, subtaskId={}",
                config.getUrl(), config.getMode(), context.getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to initialize JDBC connection", e);
        }
    }

    @Override
    public void write(Row row, Context context) throws IOException, InterruptedException {
        try {
            if (config.getMode() == WriteMode.CDC) {
                writeCdcRow(row);
            } else {
                writeNormalRow(row);
            }

            pendingCount++;
            if (pendingCount >= batchSize) {
                flush(false);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to write row", e);
        }
    }

    /**
     * CDC 模式写入逻辑
     */
    private void writeCdcRow(Row row) throws SQLException {
        RowKind kind = row.getRowKind();

        // 分类型缓存
        cdcBatchBuffer.computeIfAbsent(kind, k -> new ArrayList<>()).add(row);

        // 某类型达到批量大小时，flush 该类型
        if (cdcBatchBuffer.get(kind).size() >= batchSize) {
            flushCdcKind(kind);
        }
    }

    /**
     * 正常模式写入逻辑（INSERT/UPSERT）
     */
    private void writeNormalRow(Row row) throws SQLException {
        if (normalStatement == null) {
            initNormalStatement(row);
        }

        for (int i = 0; i < normalColumns.length; i++) {
            normalStatement.setObject(i + 1, row.getField(normalColumns[i]));
        }

        normalStatement.addBatch();
    }

    /**
     * Flush CDC 模式下某个 RowKind 的数据
     */
    private void flushCdcKind(RowKind kind) throws SQLException {
        List<Row> rows = cdcBatchBuffer.get(kind);
        if (rows.isEmpty()) {
            return;
        }

        PreparedStatement stmt = getCdcStatement(kind, rows.get(0));

        for (Row row : rows) {
            setCdcParameters(stmt, row, kind);
            stmt.addBatch();
        }

        stmt.executeBatch();
        connection.commit();
        rows.clear();

        log.debug("CDC flush 完成: kind={}, count={}, subtaskId={}",
            kind, rows.size(), context.getSubtaskId());
    }

    /**
     * 获取或创建指定 RowKind 的 PreparedStatement
     */
    private PreparedStatement getCdcStatement(RowKind kind, Row sampleRow) throws SQLException {
        if (!cdcStatements.containsKey(kind)) {
            String sql = buildCdcSql(kind, sampleRow);
            PreparedStatement stmt = connection.prepareStatement(sql);
            cdcStatements.put(kind, stmt);
            log.info("CDC SQL 创建: kind={}, sql={}", kind, sql);
        }

        return cdcStatements.get(kind);
    }

    /**
     * 构建指定 RowKind 的 SQL
     */
    private String buildCdcSql(RowKind kind, Row sampleRow) {
        String[] columns = sampleRow.getFieldNames(true).toArray(new String[0]);
        String table = config.getTable();
        List<String> keyFields = config.getKeyFields();

        switch (kind) {
            case INSERT:
                return config.getDialect().getInsertSql(table, columns);
            case UPDATE_AFTER:
                return config.getDialect().getUpdateSql(table, columns, keyFields);
            case DELETE:
                return config.getDialect().getDeleteSql(table, keyFields);
            default:
                throw new IllegalArgumentException(
                    String.format("CDC 模式不支持 RowKind: %s，支持: INSERT, UPDATE_AFTER, DELETE", kind)
                );
        }
    }

    /**
     * 设置 CDC SQL 参数
     */
    private void setCdcParameters(PreparedStatement stmt, Row row, RowKind kind) throws SQLException {
        String[] columns = row.getFieldNames(true).toArray(new String[0]);
        List<String> keyFields = config.getKeyFields();

        int index = 1;

        if (kind == RowKind.UPDATE_AFTER) {
            // UPDATE: SET 部分用非主键字段，WHERE 用主键字段
            for (String col : columns) {
                if (!keyFields.contains(col)) {
                    stmt.setObject(index++, row.getField(col));
                }
            }
            for (String key : keyFields) {
                stmt.setObject(index++, row.getField(key));
            }
        } else {
            // INSERT/DELETE: 直接按字段顺序设置
            for (String col : columns) {
                stmt.setObject(index++, row.getField(col));
            }
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        try {
            if (config.getMode() == WriteMode.CDC) {
                // CDC 模式：逐个类型 flush
                for (RowKind kind : cdcBatchBuffer.keySet()) {
                    flushCdcKind(kind);
                }
            } else {
                // INSERT/UPSERT 模式：flush 正常批次
                if (normalStatement != null && pendingCount > 0) {
                    normalStatement.executeBatch();
                    connection.commit();
                    pendingCount = 0;
                }
            }

            log.debug("Flush 完成: mode={}, subtaskId={}", config.getMode(), context.getSubtaskId());
        } catch (SQLException e) {
            try {
                connection.rollback();
                log.warn("Flush 失败，已回滚事务");
            } catch (SQLException rollbackEx) {
                log.error("回滚失败", rollbackEx);
            }
            throw new IOException("Failed to flush batch", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flush(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while flushing during close", e);
        } finally {
            try {
                // 关闭所有 PreparedStatement
                if (normalStatement != null) {
                    normalStatement.close();
                }
                if (cdcStatements != null) {
                    for (PreparedStatement stmt : cdcStatements.values()) {
                        stmt.close();
                    }
                }
                if (connection != null) {
                    connection.close();
                }
                log.info("JDBC Sink 资源清理完成, subtaskId={}", context.getSubtaskId());
            } catch (SQLException e) {
                throw new IOException("Failed to cleanup JDBC resources", e);
            }
        }
    }
}
```

---

## 6. 配置设计

### 6.1 Kafka Source 配置示例

**Debezium CDC 配置:**

```json
{
  "sources": [{
    "type": "kafka",
    "outputTable": "users_cdc",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "cdc-consumer",
      "topics": ["dbserver1.inventory.users"],
      "startupMode": "earliest",
      "format": "debezium-json",  // 新增：指定 Debezium 格式
      "schema": {  // 只配置业务数据结构（after/before 的字段）
        "id": "LONG",
        "name": "STRING",
        "email": "STRING",
        "updated_at": "TIMESTAMP"
      }
    }
  }]
}
```

**标准 JSON 配置（默认）:**

```json
{
  "sources": [{
    "type": "kafka",
    "outputTable": "events",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "json-consumer",
      "topics": ["user-events"],
      "startupMode": "latest",
      "format": "json",  // 默认值，可不配置
      "schema": {
        "userId": "LONG",
        "eventType": "STRING",
        "timestamp": "LONG"
      }
    }
  }]
}
```

### 6.2 JDBC Sink 配置示例

**CDC 模式配置:**

```json
{
  "sinks": [{
    "type": "jdbc",
    "inputTable": "users_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/target_db",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "cdc",  // 新增 CDC 模式
      "keyFields": ["id"],  // UPDATE/DELETE 的主键字段
      "batchSize": 100
    }
  }]
}
```

**INSERT 模式配置（默认）:**

```json
{
  "sinks": [{
    "type": "jdbc",
    "inputTable": "events",
    "config": {
      "url": "jdbc:mysql://localhost:3306/analytics_db",
      "username": "root",
      "password": "password",
      "table": "user_events",
      "mode": "insert",  // 默认值，可不配置
      "batchSize": 500
    }
  }]
}
```

### 6.3 Transform 配置示例

**过滤 delete 操作:**

```json
{
  "transforms": [{
    "type": "sql",
    "outputTable": "active_users",
    "config": {
      "sql": "SELECT id, UPPER(name) AS name, email FROM users_cdc WHERE id > 0"
    }
  }]
}
```

---

## 7. 数据流转示例

### 7.1 Debezium JSON 示例

**INSERT 操作:**

```json
{
  "op": "c",
  "ts_ms": 1465581854912,
  "before": null,
  "after": {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "updated_at": "2026-04-10T10:30:00Z"
  },
  "source": {
    "version": "1.0.0",
    "connector": "mysql"
  }
}
```

**转换后的 Row:**

```
Row(kind=INSERT, fields: {id=1, name="Alice", email="alice@example.com", updated_at=...})
```

**JDBC Sink 执行:**

```sql
INSERT INTO users(id, name, email, updated_at) VALUES(1, 'Alice', 'alice@example.com', ...)
```

---

**UPDATE 操作:**

```json
{
  "op": "u",
  "ts_ms": 1465581855000,
  "before": {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com"
  },
  "after": {
    "id": 1,
    "name": "Alice Updated",
    "email": "alice_updated@example.com",
    "updated_at": "2026-04-10T10:35:00Z"
  }
}
```

**转换后的 Row:**

```
Row(kind=UPDATE_AFTER, fields: {id=1, name="Alice Updated", email="alice_updated@example.com", ...})
```

**JDBC Sink 执行:**

```sql
UPDATE users SET name='Alice Updated', email='alice_updated@example.com', updated_at=... WHERE id=1
```

---

**DELETE 操作:**

```json
{
  "op": "d",
  "ts_ms": 1465581856000,
  "before": {
    "id": 1,
    "name": "Alice Updated",
    "email": "alice_updated@example.com"
  },
  "after": null
}
```

**转换后的 Row:**

```
Row(kind=DELETE, fields: {id=1, name="Alice Updated", email="alice_updated@example.com"})
```

**JDBC Sink 执行:**

```sql
DELETE FROM users WHERE id=1
```

---

## 8. 批量执行优化

### 8.1 问题分析

CDC 模式下，不同 RowKind 的数据需要使用不同的 PreparedStatement：
- INSERT → `INSERT INTO ... VALUES ...`
- UPDATE → `UPDATE ... SET ... WHERE ...`
- DELETE → `DELETE FROM ... WHERE ...`

如果所有 RowKind 使用同一个 batch buffer，会导致 PreparedStatement 切换频繁，性能下降。

### 8.2 优化策略

**分类型批量缓存:**

```java
private Map<RowKind, List<Row>> cdcBatchBuffer = new HashMap<>();
```

**分类型 flush:**

```java
// 某个 RowKind 达到 batchSize 时，单独 flush 该类型
if (cdcBatchBuffer.get(kind).size() >= batchSize) {
    flushCdcKind(kind);
}
```

**优势:**

- ✅ 减少 PreparedStatement 切换次数
- ✅ 每个 RowKind 使用独立的 batch buffer，最大化批量执行效率
- ✅ 不同类型的数据独立提交，避免相互影响

---

## 9. 测试计划

### 9.1 Kafka Format SPI 测试

**测试类**: `flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/`

| 测试项 | 说明 |
|--------|------|
| JsonFormatPluginTest | 正常 JSON 解析测试 |
| DebeziumJsonFormatPluginTest | Debezium 各种 op 测试 |
| KafkaFormatLoaderTest | Format 加载和校验测试 |
| Schema 校验测试 | 只配置业务数据 schema |
| Format 加载失败测试 | 不支持的格式抛异常 |

### 9.2 JDBC Sink CDC 测试

**测试类**: `flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/`

| 测试项 | 说明 |
|--------|------|
| JdbcSinkWriterCdcTest | CDC 模式核心逻辑测试 |
| INSERT 操作测试 | RowKind.INSERT → INSERT SQL |
| UPDATE 操作测试 | RowKind.UPDATE_AFTER → UPDATE SQL |
| DELETE 操作测试 | RowKind.DELETE → DELETE SQL |
| 批量执行测试 | 同类型混合批量测试 |
| keyFields 配置测试 | 主键字段校验测试 |

### 9.3 Dialect CDC SQL 测试

**测试类**: `flink-etl-core/src/test/java/com/etl/core/dialect/`

| 测试项 | 说明 |
|--------|------|
| MySQLDialectCdcTest | UPDATE/DELETE SQL 生成测试 |
| PostgreSQLDialectCdcTest | PostgreSQL 方言测试（未来） |
| OracleDialectCdcTest | Oracle 方言测试（未来） |

### 9.4 端到端测试

**测试场景:**

1. Kafka → Transform → JDBC 完整链路测试
2. Debezium 数据同步验证（包含 insert/update/delete）
3. Transform 过滤 delete 操作测试
4. 批量数据同步性能测试

---

## 10. 扩展性设计

### 10.1 新增 CDC 格式支持

只需实现 `KafkaFormatPlugin` 接口并添加 `@AutoService` 注解：

```java
@AutoService(KafkaFormatPlugin.class)
public class OggJsonFormatPlugin implements KafkaFormatPlugin {
    @Override
    public String identifier() {
        return "ogg-json";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        return new OggJsonDeserializationSchema(schema);
    }
}
```

**无需修改:**
- KafkaSourcePlugin（自动加载新格式）
- KafkaFormatLoader（ServiceLoader 自动发现）
- META-INF/services 文件（AutoService 自动生成）

### 10.2 新增数据库方言 CDC 支持

只需实现 `JdbcDialect` 的 CDC 方法：

```java
public class PostgreSQLDialect implements JdbcDialect {
    @Override
    public String getUpdateSql(String table, String[] columns, List<String> keyFields) {
        // PostgreSQL UPDATE 语法
    }

    @Override
    public String getDeleteSql(String table, List<String> keyFields) {
        // PostgreSQL DELETE 语法
    }
}
```

---

## 11. 向后兼容性

### 11.1 现有功能保持不变

- Kafka Source 默认 format="json"，现有配置无需修改
- JDBC Sink 默认 mode="insert"，现有配置无需修改
- Transform 层无需修改，Table API 自动处理 RowKind

### 11.2 新功能增量扩展

- Format SPI 是新增功能，不影响现有模块
- CDC 模式是新增选项，不影响 INSERT/UPSERT 模式
- 所有新配置项都有默认值，用户可选择性配置

---

## 12. 实施计划

### Phase 1: Kafka Format SPI 基础实现

1. 创建 `KafkaFormatPlugin` 接口
2. 实现 `JsonFormatPlugin`（迁移现有逻辑）
3. 实现 `DebeziumJsonFormatPlugin` 和 `DebeziumJsonDeserializationSchema`
4. 实现 `KafkaFormatLoader`
5. 改造 `KafkaSourceConfig` 和 `KafkaSourcePlugin`
6. 编写单元测试

### Phase 2: JDBC Sink CDC 模式实现

1. 扩展 `WriteMode` enum，新增 CDC
2. 扩展 `JdbcDialect` 接口，新增 UPDATE/DELETE 方法
3. 实现 `MySQLDialect` CDC 方法
4. 改造 `JdbcSinkWriter`，实现 CDC 写入逻辑
5. 实现批量执行优化策略
6. 编写单元测试

### Phase 3: 端到端集成和测试

1. 编写端到端测试案例
2. 编写完整配置示例
3. 性能测试和优化
4. 文档更新（PLUGINS.md）

### Phase 4: 其他方言和格式扩展（可选）

1. PostgreSQLDialect CDC 实现
2. OracleDialect CDC 实现
3. OggJsonFormatPlugin 实现

---

## 13. 总结

本设计实现了一个完整、可扩展的 CDC 数据处理方案：

**核心优势:**

- ✅ 标准化：使用 Flink RowKind 标准 Changelog 机制
- ✅ 模块化：Format SPI 封装在 Kafka Source 模块，保持清晰边界
- ✅ 可扩展：通过 SPI 支持多种 CDC 格式，新增格式成本极低
- ✅ 易用性：Schema 配置简化，用户只需关注业务数据结构
- ✅ 性能优化：分类型批量缓存策略，最大化执行效率
- ✅ 向后兼容：现有功能保持不变，新功能增量扩展

**适用场景:**

- Debezium CDC 数据同步
- 数据库实时同步（MySQL → MySQL、MySQL → PostgreSQL 等）
- CDC 数据过滤和转换
- 实时数据仓库建设

该设计为 Flink ETL 工具提供了完整的 CDC 数据处理能力，填补了实时数据同步场景的关键缺失。