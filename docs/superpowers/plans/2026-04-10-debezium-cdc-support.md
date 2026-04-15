# Debezium CDC 数据支持实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Kafka 消费 Debezium CDC 数据，通过 RowKind 机制自动执行 JDBC INSERT/UPDATE/DELETE 操作

**Architecture:** Kafka Source 通过 Format SPI 支持 Debezium JSON，解析 CDC 数据并设置 RowKind；JDBC Sink 新增 CDC 模式，根据 RowKind 执行对应 SQL；Transform 层自动传递 RowKind

**Tech Stack:** Flink 1.15.2, Flink Table API, AutoService 1.1.1, Jackson JSON

---

## 文件结构映射

### 新增文件

**Kafka Source Format SPI:**
```
flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/
├── KafkaFormatPlugin.java              # Format SPI 接口
├── KafkaFormatLoader.java              # Format 加载器
├── JsonFormatPlugin.java               # JSON 格式插件
├── DebeziumJsonFormatPlugin.java       # Debezium JSON 格式插件
└── DebeziumJsonDeserializationSchema.java  # Debezium 反序列化器
```

**测试文件:**
```
flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/
├── KafkaFormatLoaderTest.java
├── JsonFormatPluginTest.java
└── DebeziumJsonFormatPluginTest.java

flink-etl-core/src/test/java/com/etl/core/dialect/
└── MySQLDialectCdcTest.java

flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/
└── JdbcSinkWriterCdcTest.java
```

### 修改文件

**Kafka Source:**
- `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourceConfig.java` - 新增 format 字段
- `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourcePlugin.java` - 加载 Format Plugin
- `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/JsonToRowDeserializationSchema.java` - 移动到 format 包

**JDBC Sink:**
- `flink-etl-core/src/main/java/com/etl/core/dialect/WriteMode.java` - 新增 CDC 枚举
- `flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialect.java` - 新增 UPDATE/DELETE 方法
- `flink-etl-core/src/main/java/com/etl/core/dialect/MySQLDialect.java` - 实现 CDC SQL 生成
- `flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java` - 实现 CDC 写入逻辑

**文档:**
- `PLUGINS.md` - 更新 Kafka Source 和 JDBC Sink 配置说明

---

## Phase 1: Kafka Format SPI 基础实现

### Task 1: 创建 KafkaFormatPlugin 接口

**Files:**
- Create: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/KafkaFormatPlugin.java`
- Test: `flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/KafkaFormatPluginTest.java`

- [ ] **Step 1: 创建 Format SPI 接口**

```java
package com.etl.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Kafka 消息格式反序列化器 SPI 接口
 * 定义在 Kafka Source 模块内部，不污染 core 模块
 */
public interface KafkaFormatPlugin extends Serializable {

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

- [ ] **Step 2: 提交接口定义**

```bash
git add flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/KafkaFormatPlugin.java
git commit -m "feat: 新增 KafkaFormatPlugin SPI 接口

定义 Kafka 消息格式反序列化器接口：
- identifier() 返回格式标识
- createDeserializer() 创建反序列化器

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 2: 实现 KafkaFormatLoader 加载器

**Files:**
- Create: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/KafkaFormatLoader.java`
- Test: `flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/KafkaFormatLoaderTest.java`

- [ ] **Step 1: 编写测试 - 加载不存在的格式应返回 null**

```java
package com.etl.kafka.source.format;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaFormatLoaderTest {

    @Test
    void testLoadNonExistentFormat() {
        KafkaFormatPlugin plugin = KafkaFormatLoader.getFormatPlugin("non-existent");
        assertNull(plugin, "不存在的格式应返回 null");
    }

    @Test
    void testSupportedFormatsIncludesJson() {
        String[] formats = KafkaFormatLoader.supportedFormats();
        assertTrue(formats.length > 0, "应至少支持一种格式");
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=KafkaFormatLoaderTest
```

Expected: FAIL - KafkaFormatLoader 类不存在

- [ ] **Step 3: 实现 KafkaFormatLoader**

```java
package com.etl.kafka.source.format;

import java.util.ServiceLoader;
import java.util.HashMap;
import java.util.Map;

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

- [ ] **Step 4: 运行测试验证通过**

```bash
mvn test -Dtest=KafkaFormatLoaderTest
```

Expected: PASS

- [ ] **Step 5: 提交 KafkaFormatLoader**

```bash
git add flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/KafkaFormatLoader.java \
        flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/KafkaFormatLoaderTest.java
git commit -m "feat: 实现 KafkaFormatLoader 加载器

使用 ServiceLoader 加载所有 Format Plugin 实现类：
- getFormatPlugin() 根据格式名获取插件
- supportedFormats() 列出所有支持的格式
- 双重检查锁定 + volatile 缓存

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 3: 实现 JsonFormatPlugin（迁移现有逻辑）

**Files:**
- Create: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/JsonFormatPlugin.java`
- Move: `JsonToRowDeserializationSchema.java` 从 `kafka` 包移动到 `format` 包
- Test: `flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/JsonFormatPluginTest.java`

- [ ] **Step 1: 移动 JsonToRowDeserializationSchema 到 format 包**

```bash
# 移动文件
git mv flink-etl-source-kafka/src/main/java/com/etl/source/kafka/JsonToRowDeserializationSchema.java \
       flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/JsonToRowDeserializationSchema.java

# 更新 package 声明
# 文件内容保持不变，只需修改 package
```

- [ ] **Step 2: 编写测试 - JsonFormatPlugin 应返回 "json" 标识符**

```java
package com.etl.kafka.source.format;

import com.etl.core.schema.EtlSchema;
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
        EtlSchema schema = EtlSchema.builder()
                .field("id", "LONG")
                .field("name", "STRING")
                .build();

        JsonFormatPlugin plugin = new JsonFormatPlugin();
        assertNotNull(plugin.createDeserializer(schema));
    }
}
```

- [ ] **Step 3: 运行测试验证失败**

```bash
mvn test -Dtest=JsonFormatPluginTest
```

Expected: FAIL - JsonFormatPlugin 类不存在

- [ ] **Step 4: 实现 JsonFormatPlugin**

```java
package com.etl.kafka.source.format;

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

    private static final long serialVersionUID = 1L;

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

- [ ] **Step 5: 运行测试验证通过**

```bash
mvn test -Dtest=JsonFormatPluginTest
```

Expected: PASS

- [ ] **Step 6: 提交 JsonFormatPlugin**

```bash
git add flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/
git commit -m "feat: 实现 JsonFormatPlugin 并迁移现有逻辑

- 移动 JsonToRowDeserializationSchema 到 format 包
- 新增 JsonFormatPlugin，使用 @AutoService 自动注册
- 测试验证 identifier 和 createDeserializer

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 4: 实现 DebeziumJsonDeserializationSchema

**Files:**
- Create: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/DebeziumJsonDeserializationSchema.java`
- Test: `flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/DebeziumJsonDeserializationSchemaTest.java`

- [ ] **Step 1: 编写测试 - INSERT 操作 (op='c')**

```java
package com.etl.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import com.etl.core.utils.JsonUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DebeziumJsonDeserializationSchemaTest {

    @Test
    void testInsertOperation() throws Exception {
        // 准备 schema
        EtlSchema schema = EtlSchema.builder()
                .field("id", "LONG")
                .field("name", "STRING")
                .build();

        // 准备 Debezium INSERT 数据
        String debeziumJson = "{\"op\":\"c\",\"ts_ms\":1234567890,\"after\":{\"id\":1,\"name\":\"Alice\"}}";

        DebeziumJsonDeserializationSchema deserializer =
                new DebeziumJsonDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, null, debeziumJson.getBytes(StandardCharsets.UTF_8)
        );

        // Mock Collector
        List<Row> collectedRows = new ArrayList<>();
        Collector<Row> collector = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                collectedRows.add(record);
            }

            @Override
            public void close() {
            }
        };

        deserializer.deserialize(record, collector);

        // 验证结果
        assertEquals(1, collectedRows.size());
        Row row = collectedRows.get(0);
        assertEquals(RowKind.INSERT, row.getRowKind());
        assertEquals(1L, row.getField("id"));
        assertEquals("Alice", row.getField("name"));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=DebeziumJsonDeserializationSchemaTest
```

Expected: FAIL - DebeziumJsonDeserializationSchema 类不存在

- [ ] **Step 3: 实现 DebeziumJsonDeserializationSchema**

```java
package com.etl.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.utils.JsonUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

- [ ] **Step 4: 运行测试验证通过**

```bash
mvn test -Dtest=DebeziumJsonDeserializationSchemaTest
```

Expected: PASS

- [ ] **Step 5: 编写更多测试 - UPDATE 和 DELETE 操作**

```java
@Test
void testUpdateOperation() throws Exception {
    EtlSchema schema = EtlSchema.builder()
        .field("id", "LONG")
        .field("name", "STRING")
        .build();

    String debeziumJson = "{\"op\":\"u\",\"before\":{\"id\":1,\"name\":\"Old\"},\"after\":{\"id\":1,\"name\":\"New\"}}";

    DebeziumJsonDeserializationSchema deserializer =
        new DebeziumJsonDeserializationSchema(schema);

    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        "test-topic", 0, 0, null, debeziumJson.getBytes(StandardCharsets.UTF_8)
    );

    List<Row> collectedRows = new ArrayList<>();
    Collector<Row> collector = collectedRows::add;

    deserializer.deserialize(record, collector);

    assertEquals(1, collectedRows.size());
    Row row = collectedRows.get(0);
    assertEquals(RowKind.UPDATE_AFTER, row.getRowKind());
    assertEquals(1L, row.getField("id"));
    assertEquals("New", row.getField("name"));  // 使用 after 数据
}

@Test
void testDeleteOperation() throws Exception {
    EtlSchema schema = EtlSchema.builder()
        .field("id", "LONG")
        .field("name", "STRING")
        .build();

    String debeziumJson = "{\"op\":\"d\",\"before\":{\"id\":1,\"name\":\"Old\"},\"after\":null}";

    DebeziumJsonDeserializationSchema deserializer =
        new DebeziumJsonDeserializationSchema(schema);

    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        "test-topic", 0, 0, null, debeziumJson.getBytes(StandardCharsets.UTF_8)
    );

    List<Row> collectedRows = new ArrayList<>();
    Collector<Row> collector = collectedRows::add;

    deserializer.deserialize(record, collector);

    assertEquals(1, collectedRows.size());
    Row row = collectedRows.get(0);
    assertEquals(RowKind.DELETE, row.getRowKind());
    assertEquals(1L, row.getField("id"));
    assertEquals("Old", row.getField("name"));  // DELETE 使用 before 数据
}
```

- [ ] **Step 6: 运行所有测试**

```bash
mvn test -Dtest=DebeziumJsonDeserializationSchemaTest
```

Expected: PASS

- [ ] **Step 7: 提交 DebeziumJsonDeserializationSchema**

```bash
git add flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/DebeziumJsonDeserializationSchema.java \
        flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/DebeziumJsonDeserializationSchemaTest.java
git commit -m "feat: 实现 DebeziumJsonDeserializationSchema

解析 Debezium CDC JSON：
- op='c/r' → RowKind.INSERT + after 数据
- op='u' → RowKind.UPDATE_AFTER + after 数据
- op='d' → RowKind.DELETE + before 数据

测试覆盖 INSERT/UPDATE/DELETE 三种操作

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 5: 实现 DebeziumJsonFormatPlugin

**Files:**
- Create: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/DebeziumJsonFormatPlugin.java`
- Test: `flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/DebeziumJsonFormatPluginTest.java`

- [ ] **Step 1: 编写测试**

```java
package com.etl.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumJsonFormatPluginTest {

    @Test
    void testIdentifier() {
        DebeziumJsonFormatPlugin plugin = new DebeziumJsonFormatPlugin();
        assertEquals("debezium-json", plugin.identifier());
    }

    @Test
    void testCreateDeserializer() {
        EtlSchema schema = EtlSchema.builder()
                .field("id", "LONG")
                .build();

        DebeziumJsonFormatPlugin plugin = new DebeziumJsonFormatPlugin();
        assertNotNull(plugin.createDeserializer(schema));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=DebeziumJsonFormatPluginTest
```

Expected: FAIL

- [ ] **Step 3: 实现 DebeziumJsonFormatPlugin**

```java
package com.etl.kafka.source.format;

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

    private static final long serialVersionUID = 1L;

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

- [ ] **Step 4: 运行测试验证通过**

```bash
mvn test -Dtest=DebeziumJsonFormatPluginTest
```

Expected: PASS

- [ ] **Step 5: 提交 DebeziumJsonFormatPlugin**

```bash
git add flink-etl-source-kafka/src/main/java/com/etl/source/kafka/format/DebeziumJsonFormatPlugin.java \
        flink-etl-source-kafka/src/test/java/com/etl/source/kafka/format/DebeziumJsonFormatPluginTest.java
git commit -m "feat: 实现 DebeziumJsonFormatPlugin

使用 @AutoService 自动注册 SPI
identifier: debezium-json

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 6: 改造 KafkaSourceConfig 支持 format 配置

**Files:**
- Modify: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourceConfig.java`
- Modify: `flink-etl-source-kafka/src/test/java/com/etl/source/kafka/KafkaSourceConfigTest.java`

- [ ] **Step 1: 编写测试 - format 默认值为 "json"**

```java
@Test
void testDefaultFormat() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("bootstrapServers", "localhost:9092");
    configMap.put("groupId", "test-group");
    configMap.put("topics", Arrays.asList("test-topic"));
    configMap.put("schema", Collections.singletonMap("id", "LONG"));

    SourceConfig sourceConfig = new SourceConfig(configMap);
    KafkaSourceConfig config = KafkaSourceConfig.fromSourceConfig(sourceConfig);

    assertEquals("json", config.getFormat());
}

@Test
void testCustomFormat() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("bootstrapServers", "localhost:9092");
    configMap.put("groupId", "test-group");
    configMap.put("topics", Arrays.asList("test-topic"));
    configMap.put("format", "debezium-json");
    configMap.put("schema", Collections.singletonMap("id", "LONG"));

    SourceConfig sourceConfig = new SourceConfig(configMap);
    KafkaSourceConfig config = KafkaSourceConfig.fromSourceConfig(sourceConfig);

    assertEquals("debezium-json", config.getFormat());
}

@Test
void testUnsupportedFormat() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("bootstrapServers", "localhost:9092");
    configMap.put("groupId", "test-group");
    configMap.put("topics", Arrays.asList("test-topic"));
    configMap.put("format", "unsupported");
    configMap.put("schema", Collections.singletonMap("id", "LONG"));

    SourceConfig sourceConfig = new SourceConfig(configMap);

    assertThrows(IllegalArgumentException.class, () -> {
        KafkaSourceConfig.fromSourceConfig(sourceConfig);
    });
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=KafkaSourceConfigTest
```

Expected: FAIL - getFormat() 方法不存在

- [ ] **Step 3: 修改 KafkaSourceConfig**

在 `KafkaSourceConfig.java` 中：

1. 添加 `format` 字段：

```java
@Getter
@Builder
public class KafkaSourceConfig implements Serializable {
    // ... 现有字段 ...

    /** 消息格式：json、debezium-json 等 */
    private final String format;
}
```

2. 在 `fromSourceConfig()` 方法中解析 format：

```java
public static KafkaSourceConfig fromSourceConfig(SourceConfig config) {
    // ... 现有解析逻辑 ...

    // 解析 format（默认 "json"）
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
```

- [ ] **Step 4: 运行测试验证通过**

```bash
mvn test -Dtest=KafkaSourceConfigTest
```

Expected: PASS

- [ ] **Step 5: 提交 KafkaSourceConfig 改造**

```bash
git add flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourceConfig.java \
        flink-etl-source-kafka/src/test/java/com/etl/source/kafka/KafkaSourceConfigTest.java
git commit -m "feat: KafkaSourceConfig 新增 format 配置支持

- 新增 format 字段，默认值 \"json\"
- 校验 format 是否支持，不支持则抛异常
- 测试覆盖默认值、自定义值、不支持格式

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 7: 改造 KafkaSourcePlugin 加载 Format Plugin

**Files:**
- Modify: `flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourcePlugin.java`

- [ ] **Step 1: 修改 KafkaSourcePlugin.createSource()**

在 `createSource()` 方法中加载 Format Plugin：

```java
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

    // 设置 Topic 订阅方式
    if (kafkaConfig.isTopicsMode()) {
        builder.setTopics(kafkaConfig.getTopics());
        log.info("订阅 Topic 列表: {}", kafkaConfig.getTopics());
    } else {
        builder.setTopicPattern(Pattern.compile(kafkaConfig.getTopicPattern()));
        builder.setProperty("partition.discovery.interval.ms", "10000");
        log.info("订阅 Topic 正则: {}", kafkaConfig.getTopicPattern());
    }

    // 设置额外的 Kafka 属性
    Properties kafkaProperties = kafkaConfig.getKafkaProperties();
    if (kafkaProperties != null && !kafkaProperties.isEmpty()) {
        builder.setProperties(kafkaProperties);
        log.info("额外 Kafka 配置: {}", kafkaProperties);
    }

    return builder.build();
}
```

- [ ] **Step 2: 编译验证无语法错误**

```bash
mvn clean compile -pl flink-etl-source-kafka
```

Expected: SUCCESS

- [ ] **Step 3: 提交 KafkaSourcePlugin 改造**

```bash
git add flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourcePlugin.java
git commit -m "feat: KafkaSourcePlugin 加载 Format Plugin

替换硬编码的 JsonToRowDeserializationSchema：
- 通过 KafkaFormatLoader 加载指定 format 的 Plugin
- 使用 Plugin 创建反序列化器
- 日志输出当前使用的 format

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Phase 2: JDBC Sink CDC 模式实现

### Task 8: 扩展 WriteMode enum

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/dialect/WriteMode.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/dialect/WriteModeTest.java`

- [ ] **Step 1: 编写测试 - CDC 枚举值存在**

```java
package com.etl.core.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WriteModeTest {

    @Test
    void testCdcModeExists() {
        WriteMode mode = WriteMode.CDC;
        assertNotNull(mode);
        assertEquals("CDC", mode.name());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=WriteModeTest
```

Expected: FAIL - CDC 枚举值不存在

- [ ] **Step 3: 扩展 WriteMode**

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
     * CDC 模式，根据 RowKind 执行 INSERT/UPDATE/DELETE
     */
    CDC
}
```

- [ ] **Step 4: 运行测试验证通过**

```bash
mvn test -Dtest=WriteModeTest
```

Expected: PASS

- [ ] **Step 5: 提交 WriteMode 扩展**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/WriteMode.java \
        flink-etl-core/src/test/java/com/etl/core/dialect/WriteModeTest.java
git commit -m "feat: WriteMode 新增 CDC 枚举值

CDC 模式：根据 RowKind 执行 INSERT/UPDATE/DELETE

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 9: 扩展 JdbcDialect 接口

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialect.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/dialect/JdbcDialectTest.java`

- [ ] **Step 1: 编写测试 - 验证新方法存在**

```java
package com.etl.core.dialect;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class JdbcDialectTest {

    @Test
    void testGetUpdateSqlExists() throws NoSuchMethodException {
        // 验证 getUpdateSql 方法存在
        JdbcDialect.class.getMethod("getUpdateSql", String.class, String[].class, List.class);
    }

    @Test
    void testGetDeleteSqlExists() throws NoSuchMethodException {
        // 验证 getDeleteSql 方法存在
        JdbcDialect.class.getMethod("getDeleteSql", String.class, List.class);
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=JdbcDialectTest
```

Expected: FAIL - 方法不存在

- [ ] **Step 3: 扩展 JdbcDialect 接口**

在 `JdbcDialect.java` 中添加新方法：

```java
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
```

- [ ] **Step 4: 运行测试验证通过**

```bash
mvn test -Dtest=JdbcDialectTest
```

Expected: PASS

- [ ] **Step 5: 提交 JdbcDialect 接口扩展**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialect.java \
        flink-etl-core/src/test/java/com/etl/core/dialect/JdbcDialectTest.java
git commit -m "feat: JdbcDialect 新增 UPDATE/DELETE SQL 生成方法

- getUpdateSql(): 构建 UPDATE ... SET ... WHERE ...
- getDeleteSql(): 构建 DELETE FROM ... WHERE ...

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 10: 实现 MySQLDialect CDC 方法

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/dialect/MySQLDialect.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/dialect/MySQLDialectCdcTest.java`

- [ ] **Step 1: 编写测试 - UPDATE SQL 生成**

```java
package com.etl.core.dialect;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class MySQLDialectCdcTest {

    @Test
    void testGetUpdateSql() {
        MySQLDialect dialect = new MySQLDialect();
        String[] columns = {"id", "name", "email"};
        List<String> keyFields = Arrays.asList("id");

        String sql = dialect.getUpdateSql("users", columns, keyFields);

        assertEquals("UPDATE `users` SET `name` = ?, `email` = ? WHERE `id` = ?", sql);
    }

    @Test
    void testGetUpdateSqlWithCompositeKey() {
        MySQLDialect dialect = new MySQLDialect();
        String[] columns = {"user_id", "order_id", "status"};
        List<String> keyFields = Arrays.asList("user_id", "order_id");

        String sql = dialect.getUpdateSql("orders", columns, keyFields);

        assertTrue(sql.contains("SET `status` = ?"));
        assertTrue(sql.contains("WHERE `user_id` = ? AND `order_id` = ?"));
    }

    @Test
    void testGetDeleteSql() {
        MySQLDialect dialect = new MySQLDialect();
        List<String> keyFields = Arrays.asList("id");

        String sql = dialect.getDeleteSql("users", keyFields);

        assertEquals("DELETE FROM `users` WHERE `id` = ?", sql);
    }

    @Test
    void testGetDeleteSqlWithCompositeKey() {
        MySQLDialect dialect = new MySQLDialect();
        List<String> keyFields = Arrays.asList("user_id", "order_id");

        String sql = dialect.getDeleteSql("orders", keyFields);

        assertEquals("DELETE FROM `orders` WHERE `user_id` = ? AND `order_id` = ?", sql);
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=MySQLDialectCdcTest
```

Expected: FAIL - 方法未实现

- [ ] **Step 3: 实现 MySQLDialect CDC 方法**

在 `MySQLDialect.java` 中添加：

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

- [ ] **Step 4: 运行测试验证通过**

```bash
mvn test -Dtest=MySQLDialectCdcTest
```

Expected: PASS

- [ ] **Step 5: 提交 MySQLDialect CDC 实现**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/MySQLDialect.java \
        flink-etl-core/src/test/java/com/etl/core/dialect/MySQLDialectCdcTest.java
git commit -m "feat: MySQLDialect 实现 CDC SQL 生成

- getUpdateSql(): SET 子句排除主键字段
- getDeleteSql(): WHERE 子句使用主键字段
- 支持复合主键

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 11: 实现 JdbcSinkWriter CDC 写入逻辑

**Files:**
- Modify: `flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java`
- Test: `flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterCdcTest.java`

这个任务较大，分为多个子步骤：

#### 11.1: 添加 CDC 字段和初始化

- [ ] **Step 1: 添加 CDC 字段**

在 `JdbcSinkWriter` 类中添加：

```java
// 列名缓存（CDC 和普通模式共用）
private transient String[] columns;

// CDC 模式专用字段
private transient Map<RowKind, PreparedStatement> cdcStatements;
```

- [ ] **Step 2: 在构造函数中初始化**

```java
public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
    super(context, config);
    this.batchSize = config.getBatchSize();

    try {
        connection = DriverManager.getConnection(
            config.getUrl(),
            config.getUsername(),
            config.getPassword()
        );
        connection.setAutoCommit(false);

        // CDC 模式：初始化 Statement 缓存
        if (config.getMode() == WriteMode.CDC) {
            cdcStatements = new HashMap<>();
        }

        log.info("JDBC Sink 已连接: url={}, mode={}, subtaskId={}",
            config.getUrl(), config.getMode(), context.getSubtaskId());
    } catch (SQLException e) {
        throw new IOException("Failed to initialize JDBC connection", e);
    }
}
```

#### 11.2: 实现 writeCdcRow()

- [ ] **Step 3: 编写测试 - INSERT 操作**

```java
@Test
void testWriteCdcInsert() throws Exception {
    // Mock configuration
    JdbcSinkConfig config = mock(JdbcSinkConfig.class);
    when(config.getMode()).thenReturn(WriteMode.CDC);
    when(config.getBatchSize()).thenReturn(100);

    // Mock connection
    Connection connection = mock(Connection.class);
    PreparedStatement stmt = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(stmt);

    JdbcSinkWriter writer = new JdbcSinkWriter(mockContext, config);

    Row row = Row.ofKind(RowKind.INSERT, 1L, "Alice", "alice@example.com");
    writer.write(row, mockContext);

    verify(stmt).addBatch();
}
```

- [ ] **Step 4: 实现 writeCdcRow()**

```java
private void writeCdcRow(Row row) throws SQLException {
    RowKind kind = row.getRowKind();

    // 获取或创建 PreparedStatement
    PreparedStatement stmt = getCdcStatement(kind);

    // 设置参数并 addBatch
    setCdcParameters(stmt, row, kind);
    stmt.addBatch();
}
```

#### 11.3: 实现 getCdcStatement()

- [ ] **Step 5: 实现 getCdcStatement()**

```java
private PreparedStatement getCdcStatement(RowKind kind) throws SQLException {
    if (!cdcStatements.containsKey(kind)) {
        String sql = buildCdcSql(kind);
        PreparedStatement stmt = connection.prepareStatement(sql);
        cdcStatements.put(kind, stmt);
        log.info("CDC SQL 创建: kind={}, sql={}", kind, sql);
    }

    return cdcStatements.get(kind);
}
```

#### 11.4: 实现 buildCdcSql()

- [ ] **Step 6: 实现 buildCdcSql()**

```java
private String buildCdcSql(RowKind kind) {
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
```

#### 11.5: 实现 setCdcParameters()

- [ ] **Step 7: 实现 setCdcParameters()**

```java
private void setCdcParameters(PreparedStatement stmt, Row row, RowKind kind) throws SQLException {
    List<String> keyFields = config.getKeyFields();
    int index = 1;

    switch (kind) {
        case INSERT:
            // INSERT: 设置所有字段
            for (String col : columns) {
                stmt.setObject(index++, row.getField(col));
            }
            break;

        case UPDATE_AFTER:
            // UPDATE: SET 部分用非主键字段，WHERE 用主键字段
            for (String col : columns) {
                if (!keyFields.contains(col)) {
                    stmt.setObject(index++, row.getField(col));
                }
            }
            for (String key : keyFields) {
                stmt.setObject(index++, row.getField(key));
            }
            break;

        case DELETE:
            // DELETE: 只设置主键字段（WHERE 条件）
            for (String key : keyFields) {
                stmt.setObject(index++, row.getField(key));
            }
            break;

        default:
            throw new IllegalArgumentException("CDC 模式不支持 RowKind: " + kind);
    }
}
```

#### 11.6: 修改 write() 方法

- [ ] **Step 8: 修改 write() 方法支持 CDC**

```java
@Override
public void write(Row row, Context context) throws IOException, InterruptedException {
    try {
        // 首次写入时缓存列名（CDC 和普通模式共用）
        if (columns == null) {
            columns = row.getFieldNames(true).toArray(new String[0]);
        }

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
```

#### 11.7: 修改 flush() 方法

- [ ] **Step 9: 修改 flush() 支持 CDC**

```java
@Override
public void flush(boolean endOfInput) throws IOException, InterruptedException {
    try {
        if (config.getMode() == WriteMode.CDC) {
            // CDC 模式：遍历所有 Statement 执行 executeBatch
            for (PreparedStatement stmt : cdcStatements.values()) {
                stmt.executeBatch();
            }
            connection.commit();
            pendingCount = 0;
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
```

- [ ] **Step 10: 编译验证**

```bash
mvn clean compile -pl flink-etl-sink-jdbc
```

Expected: SUCCESS

- [ ] **Step 11: 提交 JdbcSinkWriter CDC 实现**

```bash
git add flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java \
        flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterCdcTest.java
git commit -m "feat: JdbcSinkWriter 实现 CDC 写入逻辑

核心功能：
- writeCdcRow(): 根据 RowKind 获取对应 Statement 并 addBatch
- getCdcStatement(): 缓存多个 PreparedStatement
- setCdcParameters(): 根据操作类型设置参数
  * INSERT: 所有字段
  * UPDATE: 非主键 + 主键
  * DELETE: 仅主键
- flush(): 遍历所有 Statement 执行 executeBatch

优化：
- columns 字段缓存，CDC 和普通模式共用
- pendingCount 复用，统一触发 flush

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Phase 3: 文档更新和端到端测试

### Task 12: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 更新 Kafka Source 配置说明**

在 `### Kafka Source` 章节添加 format 配置说明：

```markdown
#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| ... | ... | ... | ... |
| `format` | 否 | `json` | 消息格式：`json`（标准 JSON）、`debezium-json`（Debezium CDC） |

#### Debezium CDC 格式配置示例

**Kafka Source Debezium 配置:**

```json
{
  "source": {
    "type": "kafka",
    "outputTable": "users_cdc",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "cdc-consumer",
      "topics": ["dbserver1.inventory.users"],
      "startupMode": "earliest",
      "format": "debezium-json",  // 使用 Debezium 格式
      "schema": {  // 只配置业务数据结构（after/before 的字段）
        "id": "LONG",
        "name": "STRING",
        "email": "STRING"
      }
    }
  }
}
```

**说明:**
- `format: "debezium-json"` 启用 Debezium CDC 数据解析
- `schema` 只需配置业务数据的字段结构，无需配置 Debezium 元数据
- 解析后的 Row 会自动设置 RowKind：
  - `op='c'/'r'` → INSERT
  - `op='u'` → UPDATE_AFTER
  - `op='d'` → DELETE
```

- [ ] **Step 2: 更新 JDBC Sink 配置说明**

在 `### JDBC Sink` 章节的 mode 参数说明中添加 CDC：

```markdown
| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| ... | ... | ... | ... |
| `mode` | 否 | `insert` | 写入模式：`insert`（插入）、`upsert`（存在则更新）、`cdc`（根据 RowKind 执行操作） |
| `keyFields` | 条件必填 | - | **CDC 模式必填**：主键字段列表，用于 UPDATE/DELETE 的 WHERE 条件 |

#### CDC 模式配置示例

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "users_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/target_db",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "cdc",  // CDC 模式
      "keyFields": ["id"],  // 主键字段
      "batchSize": 100
    }
  }
}
```

**CDC 模式行为:**
- 根据 Row 的 RowKind 执行对应操作：
  - INSERT → 执行 `INSERT INTO ... VALUES ...`
  - UPDATE_AFTER → 执行 `UPDATE ... SET ... WHERE ...`
  - DELETE → 执行 `DELETE FROM ... WHERE ...`
- `keyFields` 用于 UPDATE 和 DELETE 的 WHERE 条件
- 适用于 Kafka Source 使用 `format: "debezium-json"` 的场景
```

- [ ] **Step 3: 提交文档更新**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md - Kafka Source Format 和 JDBC Sink CDC

新增内容：
- Kafka Source format 参数说明
- Debezium CDC 配置示例
- JDBC Sink CDC 模式说明
- CDC 行为和配置示例

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 13: 创建端到端测试配置示例

**Files:**
- Create: `docs/examples/kafka-debezium-to-jdbc-cdc.json`

- [ ] **Step 1: 创建完整配置示例**

```json
{
  "job": {
    "name": "debezium-cdc-sync",
    "mode": "streaming",
    "parallelism": 4
  },
  "sources": [{
    "type": "kafka",
    "outputTable": "users_cdc",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "cdc-consumer",
      "topics": ["dbserver1.inventory.users"],
      "startupMode": "earliest",
      "format": "debezium-json",
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "email": "STRING",
        "updated_at": "TIMESTAMP"
      }
    }
  }],
  "transforms": [{
    "type": "sql",
    "outputTable": "filtered_users",
    "config": {
      "sql": "SELECT id, UPPER(name) AS name, email FROM users_cdc WHERE id > 0"
    }
  }],
  "sinks": [{
    "type": "jdbc",
    "inputTable": "filtered_users",
    "config": {
      "url": "jdbc:mysql://localhost:3306/target_db",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "cdc",
      "keyFields": ["id"],
      "batchSize": 100
    }
  }]
}
```

- [ ] **Step 2: 提交示例配置**

```bash
git add docs/examples/kafka-debezium-to-jdbc-cdc.json
git commit -m "docs: 新增 Debezium CDC 完整配置示例

展示 Kafka → Transform → JDBC CDC 完整链路

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## 完成标记

完成所有 Phase 后，标记计划为已完成：

- [ ] **Phase 1 完成**: Kafka Format SPI 实现并测试通过
- [ ] **Phase 2 完成**: JDBC Sink CDC 模式实现并测试通过
- [ ] **Phase 3 完成**: 文档更新和示例配置

---

## 后续优化（可选）

### Phase 4: 其他方言和格式扩展

1. PostgreSQLDialect CDC 实现
2. OracleDialect CDC 实现
3. OggJsonFormatPlugin 实现

---

**计划文档保存位置**: `docs/superpowers/plans/2026-04-10-debezium-cdc-support.md`