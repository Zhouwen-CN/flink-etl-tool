---
name: Kafka Sink 设计文档
description: 开发 Kafka Sink 插件，复用 Flink Kafka connector，支持将 Row 数据写入 Kafka topic
type: project
---

# Kafka Sink 插件设计文档

**日期：** 2026-04-01
**状态：** 设计阶段
**目标：** 开发 Kafka Sink 插件，将数据写入 Kafka topic，复用 Flink Kafka connector

## 项目背景

### 当前状态
项目已实现 Kafka Source 插件（`flink-etl-source-kafka`），支持从 Kafka 消费 JSON 格式消息并转换为 Flink Row。

### 需求目标
开发对应的 Kafka Sink 插件，实现数据写入 Kafka topic，形成完整的数据闭环：
- **Source**: Kafka → Row（JSON 反序列化）
- **Sink**: Row → Kafka（JSON 序列化）

### 核心要求
1. 消息体使用 JSON 字符串格式
2. 支持可选的 `keyField` 配置（从 Row 字段提取消息 key）
3. 复用 Flink Kafka connector 的新 Sink API（`org.apache.flink.connector.kafka.sink.KafkaSink`）
4. 保持架构一致性（参考 Kafka Source 实现）

---

## 架构设计

### 设计理念

**核心原则：参考 Kafka Source，直接使用 Flink connector 提供的 Sink 实现**

与 JDBC Sink 不同（继承 AbstractSink），Kafka connector 已提供完整的 Sink 实现，因此：
- **不继承 AbstractSink/AbstractSinkWriter**
- 插件职责仅限于：参数校验、配置构建、序列化器定义
- 直接返回 `KafkaSink` 实例（Flink connector 内部管理 Writer、状态、容错）

### 模块结构

新建模块：`flink-etl-sink/flink-etl-sink-kafka`

**依赖关系：**
- 依赖 `flink-etl-core`（核心框架）
- 继承父 pom 的 Kafka connector 依赖（`flink-connector-kafka:1.15.2`）

### 核心类设计

#### 1. KafkaSinkPlugin（SPI 插件入口）

**职责：**
- 实现 `SinkPlugin` 接口
- 使用 `@AutoService(SinkPlugin.class)` 注册 SPI
- 在 `createSink()` 方法中构建并返回 `KafkaSink` 实例

**关键逻辑：**
```java
@Override
public Sink<Row> createSink(SinkConfig config) {
    // 解析配置
    KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(config);

    // 构建 KafkaSink
    return KafkaSink.<Row>builder()
        .setBootstrapServers(kafkaConfig.getBootstrapServers())
        .setRecordSerializer(new RowToJsonSerializationSchema(kafkaConfig))
        .setKafkaProducerConfig(kafkaConfig.getKafkaProperties())
        .build();
}
```

---

#### 2. KafkaSinkConfig（配置封装类）

**职责：**
- 集中校验参数
- 封装所有 Kafka producer 配置
- 提供 `fromSinkConfig()` 静态工厂方法

**设计模式：**
- 使用 `final` 字段 + `@Builder`（参考 KafkaSourceConfig）
- 实现 `Serializable`

**字段定义：**
```java
@Getter
@Builder
public class KafkaSinkConfig implements Serializable {
    private final String bootstrapServers;  // 必填
    private final String topic;             // 必填
    private final String keyField;          // 可选
    private final Properties kafkaProperties; // 可选
}
```

**参数校验（fromSinkConfig）：**
- `bootstrapServers` 为空 → `IllegalArgumentException`
- `topic` 为空 → `IllegalArgumentException`
- `keyField` 可选，无需校验
- 解析 `properties` 配置（Map → Properties）

---

#### 3. RowToJsonSerializationSchema（序列化器）

**职责：**
- 实现 `KafkaRecordSerializationSchema<Row>` 接口
- 将 Row 序列化为 JSON 字符串（消息 value）
- 可选提取 keyField 作为消息 key

**核心方法：**
```java
@Override
public ProducerRecord<byte[], byte[]> serialize(
    Row row,
    KafkaRecordSinkContext context,
    Long timestamp) {

    // 1. Key 序列化
    byte[] key = serializeKey(row);

    // 2. Value 序列化
    byte[] value = serializeValue(row);

    // 3. 返回 ProducerRecord
    return new ProducerRecord<>(topic, null, key, value);
}
```

**Key 序列化逻辑：**
- 配置了 `keyField` → 提取字段值 → 字符串 → UTF-8 bytes
- 未配置 → 返回 `null`
- 字段不存在 → `IllegalArgumentException`

**Value 序列化逻辑：**
- 调用 `TypeConverter.convertRowToJsonNode(row)` 获取 JsonNode
- 使用专用 ObjectMapper（配置 JSR310）将 JsonNode 转为 JSON 字符串
- 转为 UTF-8 bytes

**专用 ObjectMapper 配置：**
- 注册 `JavaTimeModule`（JSR310 支持）
- 配置日期格式：`yyyy-MM-dd HH:mm:ss`
- Jackson 自动处理 LocalDateTime、LocalDate、LocalTime

---

#### 4. TypeConverter 扩展

**新增方法：**
```java
public static JsonNode convertRowToJsonNode(Row row)
```

**设计原则：**
- 与 `convertJsonToRow()` 形成对称
- 直接构建 JsonNode，避免中间 Map 对象（性能优化）

**处理逻辑：**
1. 遍历 Row 字段（使用 `row.getFieldNames(true)`）
2. 根据字段类型调用 JSON_NODE_CONVERTERS 映射表
3. 支持：基本类型、数组、嵌套 Row（递归）、null 值
4. LocalDateTime 自动转为字符串（通过 Jackson）

**类型转换映射（复用现有）：**
- 使用 `TypeConverter` 中已有的 `JSON_NODE_CONVERTERS`
- 嵌套 Row：递归调用 `convertRowToJsonNode()`

---

### 与现有架构的对称性

| 方向 | 插件 | 配置类 | 序列化/反序列化 | 类型转换 |
|------|------|--------|----------------|----------|
| Source | KafkaSourcePlugin | KafkaSourceConfig | JsonToRowDeserializationSchema | convertJsonToRow |
| Sink | KafkaSinkPlugin | KafkaSinkConfig | RowToJsonSerializationSchema | convertRowToJsonNode |

---

## 配置参数

### JSON 配置格式

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `bootstrapServers` | 是 | - | Kafka 集群地址，如 `localhost:9092` |
| `topic` | 是 | - | 目标 Topic 名称 |
| `keyField` | 否 | - | 从 Row 字段提取消息 key，不配置则消息无 key |
| `properties` | 否 | `{}` | 额外的 Kafka producer 配置 |

### 配置示例

**基础配置：**
```json
{
  "sink": {
    "type": "kafka",
    "inputTable": "processed_data",
    "config": {
      "bootstrapServers": "localhost:9092",
      "topic": "output-topic"
    }
  }
}
```

**带 keyField 配置：**
```json
{
  "sink": {
    "type": "kafka",
    "inputTable": "user_events",
    "config": {
      "bootstrapServers": "localhost:9092",
      "topic": "user-events-output",
      "keyField": "userId",
      "properties": {
        "acks": "all",
        "retries": "3"
      }
    }
  }
}
```

---

## 错误处理

### 配置校验

| 场景 | 异常类型 | 处理方式 |
|------|---------|----------|
| `bootstrapServers` 为空 | `IllegalArgumentException` | 阻止 Job 启动 |
| `topic` 为空 | `IllegalArgumentException` | 阻止 Job 启动 |
| `keyField` 配置但字段不存在 | `IllegalArgumentException` | 运行时抛出，Flink 从 checkpoint 重试 |

### 序列化异常

| 场景 | 异常类型 | 处理方式 |
|------|---------|----------|
| JSON 序列化失败 | `IOException` | Flink 从 checkpoint 重试 |
| 字段类型不支持 | `IllegalArgumentException` | 运行时抛出 |

### Kafka Producer 异常

- 由 Flink Kafka connector 内部处理
- 自动重试、容错机制

---

## 测试策略

### 单元测试

#### 1. KafkaSinkConfigTest
- 测试配置解析（必填参数、可选参数）
- 测试参数校验（空值、缺失）
- 测试 properties 解析（Map → Properties）

#### 2. TypeConverterTest（扩展）
- 测试 `convertRowToJsonNode()` 方法
- 覆盖场景：
  - 简单 Row（基本类型）
  - 复杂 Row（嵌套、数组）
  - LocalDateTime 类型
  - null 值
  - 无字段名 Row（位置模式）

#### 3. RowToJsonSerializationSchemaTest
- 测试 Key 序列化（有/无 keyField）
- 测试 Value 序列化（JSON 格式正确性）
- Mock `KafkaRecordSinkContext`

### 集成测试（可选）

- 使用本地 Kafka（或 Docker）
- 端到端测试：Source → Transform → Sink
- 验证消息写入、格式、key 设置

---

## 文档更新

### PLUGINS.md 新增章节

**章节位置：** `Sink 插件` → JDBC Sink 之后

**章节内容：**
- Kafka Sink 简介
- 配置参数表
- 配置示例（基础、带 keyField）
- 数据格式说明
- 与 Kafka Source 的对应关系

---

## 依赖更新

### 1. flink-etl-sink-kafka/pom.xml

```xml
<dependencies>
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-core</artifactId>
        <version>${project.version}</version>
    </dependency>
</dependencies>
```

**说明：** Kafka connector 依赖已在父 pom 中声明（provided scope）

### 2. flink-etl-client/pom.xml

添加 sink-kafka 模块依赖：
```xml
<dependency>
    <groupId>com.etl</groupId>
    <artifactId>flink-etl-sink-kafka</artifactId>
    <version>${project.version}</version>
</dependency>
```

---

## 实现清单

### 核心文件

| 文件 | 位置 | 说明 |
|------|------|------|
| KafkaSinkPlugin.java | flink-etl-sink-kafka | SPI 插件入口 |
| KafkaSinkConfig.java | flink-etl-sink-kafka | 配置封装类 |
| RowToJsonSerializationSchema.java | flink-etl-sink-kafka | 序列化器 |
| TypeConverter.java | flink-etl-core | 扩展 convertRowToJsonNode() |

### 测试文件

| 文件 | 位置 |
|------|------|
| KafkaSinkConfigTest.java | flink-etl-sink-kafka/src/test |
| RowToJsonSerializationSchemaTest.java | flink-etl-sink-kafka/src/test |
| TypeConverterTest.java | flink-etl-core/src/test（扩展） |

### 配置文件

| 文件 | 操作 |
|------|------|
| flink-etl-sink-kafka/pom.xml | 新建 |
| flink-etl-client/pom.xml | 添加依赖 |
| PLUGINS.md | 新增 Kafka Sink 章节 |

---

## 技术要点

### 为什么不继承 AbstractSink？

**Kafka connector vs JDBC Sink 的区别：**

| 特性 | JDBC Sink | Kafka connector |
|------|-----------|-----------------|
| Sink 实现 | 需要自己实现（继承 AbstractSink） | connector 已提供完整 Sink |
| Writer 管理 | 需要实现 AbstractSinkWriter | connector 内部管理 |
| 容错机制 | 需要自行处理 flush/close | connector 自动处理 |
| 状态管理 | AbstractSink 不保存状态 | connector 支持 exactly-once |

**结论：** Kafka connector 提供的功能更完善，直接使用其 Sink 实现即可。

### Row 转 JSON 的性能优化

**为什么选择 TypeConverter.convertRowToJsonNode()？**

| 方案 | 性能 | 架构一致性 |
|------|------|-----------|
| Row → Map → JSON | 中间对象开销 | 不一致 |
| Row → JsonNode（直接） | 无中间对象 | 与 Source 对称 |

**关键：**
- 直接构建 JsonNode，避免创建 Map
- 复用现有的类型转换映射表
- Jackson 内部优化（ObjectNode 性能良好）

### LocalDateTime 序列化

**如何处理？**

- **不手动判断类型**
- **利用 Jackson JSR310 模块**
- 配置：注册 `JavaTimeModule` + 设置日期格式
- Jackson 自动转换 LocalDateTime → 字符串

---

## 风险与约束

### 已知限制

1. **消息格式固定为 JSON** — 不支持 Avro、Protobuf等其他格式
2. **Key 只支持字符串** — 从字段提取后转为字符串，不支持复杂类型 key
3. **不支持消息 header/timestamp** — 只支持 key 和 value

### 容错语义

- **At-least-once** — Flink Kafka connector 默认语义
- 如果启用 checkpoint，connector 会提交 offset
- 发生故障时，从上次 checkpoint 重放数据

---

## 参考资料

### 现有实现
- Kafka Source 实现：`flink-etl-source-kafka`
- JDBC Sink 实现：`flink-etl-sink-jdbc`
- TypeConverter：`flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java`

### Flink 文档
- [Flink Kafka Connector 1.15](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/kafka/)
- [KafkaSink API](https://nightlies.apache.org/flink/flink-docs-release-1.15/api/java/org/apache/flink/connector/kafka/sink/KafkaSink.html)

---

## 下一步行动

调用 `writing-plans` 技能，生成详细的实现计划，包括：
- 文件创建顺序
- 代码实现步骤
- 测试编写顺序
- 文档更新步骤