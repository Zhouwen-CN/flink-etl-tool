# Kafka Source 设计文档

## 概述

新增 Kafka Source 插件，封装 `flink-connector-kafka`，支持从 Kafka 消费 JSON 格式消息，转换为 Flink Row 类型输出到下游。

## 设计方案

采用**直接封装 KafkaSource** 方案，复用官方 connector 的成熟实现，通过自定义反序列化器将 JSON 消息转换为 Row。

**优势**：
- 复用成熟的 Kafka Source 实现，稳定可靠
- 代码量最小，只需创建 SPI 插件和反序列化器
- 自动获得 Kafka Source 的所有特性（checkpoint、分区发现、水位线等）

## 使用场景

- 流式消费 Kafka 消息
- 支持 Topic 列表和正则匹配两种订阅方式
- 支持 JSON 格式消息，转换为结构化 Row 数据
- 自动添加 `__topic__` 隐藏字段，记录消息来源 Topic

## 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `bootstrapServers` | 是 | - | Kafka 集群地址，如 `localhost:9092` |
| `groupId` | 是 | - | 消费者组 ID |
| `topics` | 条件必填 | - | Topic 列表，与 `topicPattern` 二选一 |
| `topicPattern` | 条件必填 | - | Topic 正则表达式，与 `topics` 二选一 |
| `startingOffsets` | 否 | `earliest` | 起始位置：`earliest`、`latest`、`committed` |
| `properties` | 否 | `{}` | 额外的 Kafka consumer 配置 |
| `schema` | 是 | - | 消息体字段定义 |

**隐藏字段**：输出 Row 自动包含 `__topic__` 字段（STRING 类型），无需在 schema 中定义。

## 配置示例

### Topic 列表模式

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
        "timestamp": "LONG",
        "data": {
          "name": "STRING",
          "value": "DOUBLE"
        }
      }
    }
  }
}
```

### 正则匹配模式

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
        "tags": ["STRING"]
      }
    }
  }
}
```

## 模块结构

```
flink-etl-source-kafka/
├── pom.xml
└── src/main/java/com/etl/source/kafka/
    ├── KafkaSourcePlugin.java              # SPI 插件入口
    ├── KafkaSourceConfig.java              # 配置封装类
    └── JsonToRowDeserializationSchema.java # JSON → Row 反序列化器
```

## 核心组件

### KafkaSourcePlugin

实现 `SourcePlugin` 接口，职责：
- 提供 `kafka` 类型标识
- 创建 KafkaSource 实例

```java
@AutoService(SourcePlugin.class)
public class KafkaSourcePlugin implements SourcePlugin {
    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        KafkaSourceConfig kafkaConfig = KafkaSourceConfig.fromSourceConfig(config);
        // 构建 KafkaSource...
    }
}
```

### KafkaSourceConfig

配置封装类，职责：
- 从 SourceConfig 解析配置参数
- 进行参数校验
- 提供 OffsetsInitializer 转换

```java
@Getter
@Builder
public class KafkaSourceConfig implements Serializable {
    private final String bootstrapServers;
    private final String groupId;
    private final List<String> topics;
    private final String topicPattern;
    private final String startingOffsets;
    private final Properties kafkaProperties;
    private final EtlSchema schema;

    public static KafkaSourceConfig fromSourceConfig(SourceConfig config) {
        // 参数校验和解析...
    }

    public OffsetsInitializer getOffsetsInitializer() {
        // 转换为 Flink OffsetsInitializer...
    }
}
```

### JsonToRowDeserializationSchema

实现 `KafkaRecordDeserializationSchema<Row>`，职责：
- 将 Kafka 消息的 value 解析为 JsonNode
- 根据 Schema 定义转换为 Flink Row
- 复用 `TypeConverter.convertJsonToRows()` 方法
- 自动添加 `__topic__` 隐藏字段

```java
public class JsonToRowDeserializationSchema
        implements KafkaRecordDeserializationSchema<Row> {

    private final EtlSchema schema;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 隐藏字段名，用于存储消息来源 Topic */
    public static final String TOPIC_FIELD = "__topic__";

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out)
            throws IOException {
        JsonNode jsonNode = objectMapper.readTree(record.value());
        // 使用 convertJsonToRows 方法，支持 JSONObject 和 JSONArray
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

**说明**：
- 使用 `convertJsonToRows()` 支持单条 JSON 对象和 JSON 数组两种消息格式
- `__topic__` 字段自动追加到 Row 末尾，类型为 STRING

## 参数校验规则

| 参数 | 校验规则 |
|------|---------|
| `bootstrapServers` | 不能为 null |
| `groupId` | 不能为 null |
| `topics` / `topicPattern` | 至少配置一个 |
| `schema` | 不能为 null |
| `startingOffsets` | 必须是 `earliest`、`latest`、`committed` 之一 |

## 运行模式

- `Boundedness.CONTINUOUS_UNBOUNDED`：流式消费，持续运行
- 支持 checkpoint 时自动提交 offset 到 Kafka

## 依赖

```xml
<dependencies>
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-core</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>3.3.0-1.19</version>
    </dependency>
</dependencies>
```

## 错误处理

| 场景 | 处理方式 |
|------|---------|
| 必填参数缺失 | 构造函数抛出 `NullPointerException` 或 `IllegalArgumentException` |
| topics 和 topicPattern 都未配置 | 抛出 `IllegalArgumentException` |
| startingOffsets 值非法 | 抛出 `IllegalArgumentException` |
| JSON 解析失败 | 反序列化器抛出 `IOException`，任务失败重试 |
| Schema 字段不匹配 | 抛出类型转换异常 |

## 文档更新

实现完成后需更新 `PLUGINS.md`，添加 Kafka Source 配置说明。