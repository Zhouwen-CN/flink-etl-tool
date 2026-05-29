# Kafka Source Raw Format 设计文档

## 概述

为 Kafka Source 新增 `raw` format，将 Kafka 消息的 value 原始内容（byte[]）以 UTF-8 编码转为 String，作为单个字段输出到
Row，不做任何结构化解析。

## 需求

- Kafka 消息 value 不做 JSON/XML 等结构化解析，直接作为单字段输出
- schema 必须配置，且必须是单个 STRING 类型字段，字段名用户自定义
- 忽略 Kafka key
- value 为 null 时输出 Row 字段值为 null；value 为空 byte[] 时输出空字符串
- 使用 UTF-8 编码

## 设计方案

采用方案 A：Format Plugin 内推断字段。完全复用现有 KafkaFormatPlugin SPI 架构，新增 raw format 实现，不修改任何现有代码。

### 新增文件

#### 1. RawFormatPlugin

路径：`connector-kafka/src/main/java/com/etl/connector/kafka/source/format/RawFormatPlugin.java`

- `@AutoService(KafkaFormatPlugin.class)` 注解，编译时自动注册到 SPI
- `identifier()` 返回 `"raw"`
- `createDeserializer(EtlSchema schema)` 中校验 schema：
    - 必须是单字段（`schema.getFieldNames().length == 1`），否则抛 `IllegalArgumentException`
    - 字段类型必须是 STRING（`schema.getFieldTypes()[0] == Types.STRING`），否则抛 `IllegalArgumentException`
- 校验通过后创建 `RawDeserializationSchema(schema)`

#### 2. RawDeserializationSchema

路径：`connector-kafka/src/main/java/com/etl/connector/kafka/source/format/RawDeserializationSchema.java`

实现 `KafkaRecordDeserializationSchema<Row>`：

- `deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out)`：
    - `record.value() == null` → `out.collect(Row.of(null))`
    - `record.value()` 非空 → `out.collect(Row.of(new String(record.value(), StandardCharsets.UTF_8)))`
- `getProducedType()` → `Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes())`

#### 3. 测试文件

- `RawFormatPluginTest.java`：测试 identifier 返回 `"raw"`、正常 schema 通过校验、多字段 schema 抛异常、非 STRING 类型
  schema 抛异常
- `RawDeserializationSchemaTest.java`：测试正常消息反序列化、null value 输出 null Row、空 byte[] 输出空字符串

### 不修改的现有代码

- `KafkaSourceConfig` — 现有 schema 校验和 format 加载逻辑无需改动
- `KafkaFormatLoader` — 通过 ServiceLoader 自动发现 RawFormatPlugin
- SPI 注册文件 — `@AutoService` 编译时自动生成

## 配置示例

```json
{
  "sources": [{
    "type": "kafka",
    "config": {
      "format": "raw",
      "bootstrapServers": "127.0.0.1:9092",
      "groupId": "flink-etl-tool",
      "topics": ["raw-topic"],
      "startupMode": "earliest",
      "schema": { "message": "STRING" }
    }
  }],
  "sinks": [{
    "type": "console",
    "config": {
      "inputTable": "raw_data"
    }
  }]
}
```

字段名 `"message"` 由用户自定义，类型必须是 STRING。

## 错误处理

| 场景                 | 行为                                                                                                  |
|--------------------|-----------------------------------------------------------------------------------------------------|
| schema 不是单字段       | `createDeserializer()` 抛 `IllegalArgumentException`：raw format requires exactly one STRING field    |
| schema 字段不是 STRING | `createDeserializer()` 抛 `IllegalArgumentException`：raw format requires the field type to be STRING |
| value 为 null       | 输出 `Row.of(null)`                                                                                   |
| value 为空 byte[]    | 输出 `Row.of("")`                                                                                     |

## 文档更新

实施完成后需同步更新 PLUGINS.md，在 Kafka Source 部分添加 raw format 的说明。