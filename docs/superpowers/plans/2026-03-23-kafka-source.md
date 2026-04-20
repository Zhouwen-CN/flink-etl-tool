# Kafka Source 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 Kafka Source 插件，封装 `flink-connector-kafka`，支持从 Kafka 消费 JSON 格式消息并转换为 Flink Row 类型输出。

**Architecture:** 直接封装 Flink 官方 KafkaSource，通过自定义 `KafkaRecordDeserializationSchema<Row>` 反序列化器将 JSON
消息转换为 Row。复用现有 `TypeConverter.convertJsonToRows()` 方法处理 JSON 解析，自动追加 `__topic__` 隐藏字段记录消息来源。

**Tech Stack:** Java 11, Flink 1.19.0, flink-connector-kafka 3.3.0-1.19, Jackson

---

## 文件结构

| 文件                                    | 职责                      |
|---------------------------------------|-------------------------|
| `flink-etl-source-kafka/pom.xml`      | 模块依赖配置                  |
| `KafkaSourcePlugin.java`              | SPI 插件入口，创建 KafkaSource |
| `KafkaSourceConfig.java`              | 配置封装类，参数校验和转换           |
| `JsonToRowDeserializationSchema.java` | JSON → Row 反序列化器        |
| `flink-etl-source/pom.xml`            | 添加 kafka 模块             |
| `flink-etl-client/pom.xml`            | 添加 kafka 依赖             |
| `PLUGINS.md`                          | 文档更新                    |

---

## Task 1: 创建模块结构和 pom.xml

**Files:**

- Create: `flink-etl-source/flink-etl-source-kafka/pom.xml`
- Modify: `flink-etl-source/pom.xml:19-23`

- [ ] **Step 1: 创建 Kafka Source 模块 pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-source</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flink-etl-source-kafka</artifactId>
    <name>Flink ETL Source - Kafka</name>
    <description>Kafka Source 插件，支持 JSON 格式消息消费</description>

    <dependencies>
        <!-- Flink Kafka Connector -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka</artifactId>
            <version>3.3.0-1.19</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 修改 flink-etl-source/pom.xml，添加 kafka 模块**

在 `<modules>` 中添加：

```xml

<module>flink-etl-source-kafka</module>
```

- [ ] **Step 3: 验证模块编译**

Run: `mvn clean compile -pl flink-etl-source/flink-etl-source-kafka -am`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-source/flink-etl-source-kafka/pom.xml flink-etl-source/pom.xml
git commit -m "feat: 新增 Kafka Source 模块结构"
```

---

## Task 2: 实现 KafkaSourceConfig 配置类

**Files:**

- Create: `flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourceConfig.java`

- [ ] **Step 1: 创建 KafkaSourceConfig 类**

```java
package com.etl.source.kafka;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * Kafka Source 配置
 */
@Getter
@Builder
public class KafkaSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Kafka 集群地址 */
    private final String bootstrapServers;
    /** 消费者组 ID */
    private final String groupId;
    /** Topic 列表（与 topicPattern 二选一） */
    private final List<String> topics;
    /** Topic 正则表达式（与 topics 二选一） */
    private final String topicPattern;
    /** 起始位置 */
    private final String startingOffsets;
    /** 额外的 Kafka consumer 配置 */
    private final Properties kafkaProperties;
    /** Schema 定义 */
    private final EtlSchema schema;

    /**
     * 从 SourceConfig 解析配置
     */
    public static KafkaSourceConfig fromSourceConfig(SourceConfig config) {
        // 校验必填参数
        String bootstrapServers = config.getString("bootstrapServers");
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrapServers 不能为空");
        }

        String groupId = config.getString("groupId");
        if (groupId == null) {
            throw new IllegalArgumentException("groupId 不能为空");
        }

        // 校验 topics 和 topicPattern 至少配置一个
        List<String> topics = config.getStringList("topics");
        String topicPattern = config.getString("topicPattern");
        if ((topics == null || topics.isEmpty()) && topicPattern == null) {
            throw new IllegalArgumentException("topics 和 topicPattern 至少需要配置一个");
        }

        // 校验 startingOffsets
        String startingOffsets = config.getString("startingOffsets", "earliest");
        if (!isValidStartingOffset(startingOffsets)) {
            throw new IllegalArgumentException(
                    "startingOffsets 必须是 earliest、latest 或 committed，当前值: " + startingOffsets);
        }

        // 校验 schema
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            throw new IllegalArgumentException("schema 不能为空");
        }

        // 解析额外的 Kafka 属性
        Properties kafkaProperties = parseKafkaProperties(config);

        return KafkaSourceConfig.builder()
                .bootstrapServers(bootstrapServers)
                .groupId(groupId)
                .topics(topics)
                .topicPattern(topicPattern)
                .startingOffsets(startingOffsets)
                .kafkaProperties(kafkaProperties)
                .schema(schema)
                .build();
    }

    /**
     * 获取 Flink OffsetsInitializer
     */
    public OffsetsInitializer getOffsetsInitializer() {
        switch (startingOffsets.toLowerCase()) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            case "committed":
                return OffsetsInitializer.committedOffsets();
            default:
                return OffsetsInitializer.earliest();
        }
    }

    /**
     * 判断是否使用 Topic 列表模式
     */
    public boolean isTopicsMode() {
        return topics != null && !topics.isEmpty();
    }

    /**
     * 校验 startingOffsets 值
     */
    private static boolean isValidStartingOffset(String offset) {
        return "earliest".equalsIgnoreCase(offset)
                || "latest".equalsIgnoreCase(offset)
                || "committed".equalsIgnoreCase(offset);
    }

    /**
     * 解析额外的 Kafka 配置属性
     */
    @SuppressWarnings("unchecked")
    private static Properties parseKafkaProperties(SourceConfig config) {
        Properties properties = new Properties();
        Object propsObj = config.get("properties");
        if (propsObj instanceof java.util.Map) {
            ((java.util.Map<String, Object>) propsObj).forEach((key, value) -> {
                if (key != null && value != null) {
                    properties.setProperty(key.toString(), value.toString());
                }
            });
        }
        return properties;
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn clean compile -pl flink-etl-source/flink-etl-source-kafka -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourceConfig.java
git commit -m "feat: 新增 KafkaSourceConfig 配置类"
```

---

## Task 3: 实现 JsonToRowDeserializationSchema 反序列化器

**Files:**

- Create:
  `flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/JsonToRowDeserializationSchema.java`

- [ ] **Step 1: 创建 JsonToRowDeserializationSchema 类**

```java
package com.etl.source.kafka;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.TypeConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * JSON 到 Row 的反序列化器
 * 将 Kafka 消息的 value 解析为 JsonNode，然后转换为 Flink Row
 */
public class JsonToRowDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    /** 隐藏字段名，用于存储消息来源 Topic */
    public static final String TOPIC_FIELD = "__topic__";

    private final EtlSchema schema;

    /** ObjectMapper 用于 JSON 解析，使用 transient 标记，序列化时会忽略 */
    private transient ObjectMapper objectMapper;

    /**
     * 构造函数
     *
     * @param schema Schema 定义
     */
    public JsonToRowDeserializationSchema(EtlSchema schema) {
        this.schema = schema;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        // 初始化 ObjectMapper（在任务启动时初始化，避免序列化问题）
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            return;
        }

        // 解析 JSON
        JsonNode jsonNode = objectMapper.readTree(record.value());

        // 使用 TypeConverter.convertJsonToRows 方法，支持 JSONObject 和 JSONArray
        List<Row> rows = TypeConverter.convertJsonToRows(jsonNode, schema);

        // 为每个 Row 添加 __topic__ 隐藏字段
        String topic = record.topic();
        for (Row row : rows) {
            Row rowWithTopic = appendTopicField(row, topic);
            out.collect(rowWithTopic);
        }
    }

    /**
     * 在 Row 末尾追加 __topic__ 字段
     *
     * @param row   原始 Row
     * @param topic Topic 名称
     * @return 追加了 __topic__ 字段的新 Row
     */
    private Row appendTopicField(Row row, String topic) {
        int fieldCount = row.getArity();
        Row newRow = Row.withPositions(fieldCount + 1);

        // 复制原有字段
        for (int i = 0; i < fieldCount; i++) {
            newRow.setField(i, row.getField(i));
        }

        // 追加 __topic__ 字段
        newRow.setField(fieldCount, topic);
        return newRow;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        // 在原有 schema 字段基础上，追加 __topic__ 字段
        String[] fieldNames = schema.getFieldNames();
        TypeInformation<?>[] fieldTypes = schema.getFieldTypes();

        String[] newFieldNames = Arrays.copyOf(fieldNames, fieldNames.length + 1);
        newFieldNames[fieldNames.length] = TOPIC_FIELD;

        TypeInformation<?>[] newFieldTypes = Arrays.copyOf(fieldTypes, fieldTypes.length + 1);
        newFieldTypes[fieldTypes.length] = Types.STRING;

        return Types.ROW_NAMED(newFieldNames, newFieldTypes);
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn clean compile -pl flink-etl-source/flink-etl-source-kafka -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/JsonToRowDeserializationSchema.java
git commit -m "feat: 新增 JsonToRowDeserializationSchema 反序列化器"
```

---

## Task 4: 实现 KafkaSourcePlugin 插件入口

**Files:**

- Create: `flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourcePlugin.java`

- [ ] **Step 1: 创建 KafkaSourcePlugin 类**

```java
package com.etl.source.kafka;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Kafka Source 插件
 * 封装 Flink KafkaSource，支持 JSON 格式消息消费
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class KafkaSourcePlugin implements SourcePlugin {

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        log.info("创建 Kafka Source");

        // 解析配置
        KafkaSourceConfig kafkaConfig = KafkaSourceConfig.fromSourceConfig(config);

        // 构建 KafkaSource
        KafkaSource.Builder<Row> builder = KafkaSource.<Row>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setGroupId(kafkaConfig.getGroupId())
                .setStartingOffsets(kafkaConfig.getOffsetsInitializer())
                .setDeserializer(new JsonToRowDeserializationSchema(kafkaConfig.getSchema()));

        // 设置 Topic 订阅方式
        if (kafkaConfig.isTopicsMode()) {
            builder.setTopics(kafkaConfig.getTopics());
            log.info("订阅 Topic 列表: {}", kafkaConfig.getTopics());
        } else {
            builder.setTopicPattern(kafkaConfig.getTopicPattern());
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
}
```

- [ ] **Step 2: 添加 Properties import**

修复 import：

```java
import java.util.Properties;
```

- [ ] **Step 3: 验证编译**

Run: `mvn clean compile -pl flink-etl-source/flink-etl-source-kafka -am`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourcePlugin.java
git commit -m "feat: 新增 KafkaSourcePlugin 插件入口"
```

---

## Task 5: 添加模块依赖到 flink-etl-client

**Files:**

- Modify: `flink-etl-client/pom.xml:45-49`

- [ ] **Step 1: 在 flink-etl-client/pom.xml 中添加 Kafka Source 依赖**

在 `<!-- Source 插件 -->` 部分添加：

```xml

<dependency>
    <groupId>com.etl</groupId>
    <artifactId>flink-etl-source-kafka</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 2: 验证整体编译**

Run: `mvn clean compile`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-client/pom.xml
git commit -m "feat: 添加 Kafka Source 到客户端依赖"
```

---

## Task 6: 创建配置示例文件

**Files:**

- Create: `docs/examples/kafka-to-console.json`

- [ ] **Step 1: 创建 Kafka Source 配置示例**

```json
{
  "job": {
    "name": "kafka-to-console",
    "mode": "streaming"
  },
  "sources": [
    {
      "type": "kafka",
      "outputTable": "user_events",
      "config": {
        "bootstrapServers": "localhost:9092",
        "groupId": "etl-consumer",
        "topics": [
          "user-events"
        ],
        "startingOffsets": "earliest",
        "schema": {
          "userId": "LONG",
          "eventType": "STRING",
          "timestamp": "LONG",
          "data": {
            "name": "STRING",
            "value": "DOUBLE"
          }
        }
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "user_events",
      "config": {}
    }
  ]
}
```

- [ ] **Step 2: 创建正则匹配模式示例**

创建 `docs/examples/kafka-regex-to-console.json`：

```json
{
  "job": {
    "name": "kafka-regex-to-console",
    "mode": "streaming"
  },
  "sources": [
    {
      "type": "kafka",
      "outputTable": "metrics",
      "config": {
        "bootstrapServers": "localhost:9092",
        "groupId": "metrics-consumer",
        "topicPattern": "metrics-.*",
        "startingOffsets": "latest",
        "properties": {
          "fetch.max.bytes": "52428800"
        },
        "schema": {
          "metric": "STRING",
          "value": "DOUBLE",
          "tags": [
            "STRING"
          ]
        }
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "metrics",
      "config": {}
    }
  ]
}
```

- [ ] **Step 3: Commit**

```bash
git add docs/examples/stream-kafka2console.json docs/examples/stream-kafka2console-topic-pattern.json
git commit -m "docs: 新增 Kafka Source 配置示例"
```

---

## Task 7: 更新 PLUGINS.md 文档

**Files:**

- Modify: `PLUGINS.md`

- [ ] **Step 1: 在目录中添加 Kafka Source 链接**

在 Source 插件目录部分添加：

```markdown
  - [Kafka Source](#kafka-source)
```

- [ ] **Step 2: 在 HTTP Source 后添加 Kafka Source 文档**

```markdown
---

### Kafka Source

从 Kafka 消费 JSON 格式消息，支持 Topic 列表和正则匹配两种订阅方式。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `bootstrapServers` | 是 | - | Kafka 集群地址，如 `localhost:9092` |
| `groupId` | 是 | - | 消费者组 ID |
| `topics` | 条件必填 | - | Topic 列表，与 `topicPattern` 二选一 |
| `topicPattern` | 条件必填 | - | Topic 正则表达式，与 `topics` 二选一 |
| `startingOffsets` | 否 | `earliest` | 起始位置：`earliest`、`latest`、`committed` |
| `properties` | 否 | `{}` | 额外的 Kafka consumer 配置 |
| `schema` | 是 | - | 消息体字段定义 |

**隐藏字段**：输出 Row 自动包含 `__topic__` 字段（STRING 类型），记录消息来源 Topic。

#### 配置示例

**Topic 列表模式：**

```json
{
"source": {
"type": "kafka",
"outputTable": "user_events",
"config": {
"bootstrapServers": "localhost:9092",
"groupId": "etl-consumer",
"topics": ["user-events", "order-events"],
"startingOffsets": "earliest",
"schema": {
"userId": "LONG",
"eventType": "STRING",
"timestamp": "LONG"
}
}
}
}
```

**正则匹配模式：**

```json
{
  "source": {
    "type": "kafka",
    "outputTable": "metrics",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "metrics-consumer",
      "topicPattern": "metrics-.*",
      "startingOffsets": "latest",
      "properties": {
        "fetch.max.bytes": "52428800"
      },
      "schema": {
        "metric": "STRING",
        "value": "DOUBLE",
        "tags": [
          "STRING"
        ]
      }
    }
  }
}
```

#### 数据解析说明

- 支持 JSON 对象和 JSON 数组两种消息格式
- JSON 数组会展开为多条 Row 记录
- Schema 始终描述单条记录的结构
- 自动追加 `__topic__` 字段记录消息来源

#### 运行模式

- 流式消费，持续运行（`mode: "streaming"`）
- 支持 checkpoint 时自动提交 offset 到 Kafka

```

- [ ] **Step 3: 验证文档格式**

Run: `cat PLUGINS.md | grep -A 5 "Kafka Source"`
Expected: 正确显示 Kafka Source 文档

- [ ] **Step 4: Commit**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md 添加 Kafka Source 配置说明"
```

---

## Task 8: 整体验证

- [ ] **Step 1: 完整编译**

Run: `mvn clean compile`
Expected: BUILD SUCCESS

- [ ] **Step 2: 打包验证**

Run: `mvn clean package -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: 验证 SPI 配置生成**

Run: `find flink-etl-source/flink-etl-source-kafka -name "com.etl.core.spi.SourcePlugin"`
Expected: 存在 SPI 配置文件

- [ ] **Step 4: 最终 Commit**

```bash
git add -A
git status
```

确认所有更改已提交。

---

## 验收标准

1. ✅ Kafka Source 模块编译通过
2. ✅ SPI 配置正确生成
3. ✅ 配置示例文件可用
4. ✅ PLUGINS.md 文档更新完整
5. ✅ 整体项目打包成功