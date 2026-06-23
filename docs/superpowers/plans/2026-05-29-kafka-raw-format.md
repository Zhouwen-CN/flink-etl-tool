# Kafka Source Raw Format 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 Kafka Source 新增 `raw` format，将消息 value 原始内容作为单字段 STRING Row 输出

**Architecture:** 新增 `RawFormatPlugin`（SPI 入口，schema 校验）和 `RawDeserializationSchema`（反序列化实现），完全复用现有
KafkaFormatPlugin SPI 架构，不修改任何现有代码

**Tech Stack:** Java 1.8, Flink 1.15.2, Google AutoService, JUnit 5

---

## 文件结构

| 操作 | 文件                                                                                                      | 负责内容                              |
|----|---------------------------------------------------------------------------------------------------------|-----------------------------------|
| 创建 | `connector-kafka/src/main/java/com/etl/connector/kafka/source/format/RawFormatPlugin.java`              | SPI 入口，identifier="raw"，schema 校验 |
| 创建 | `connector-kafka/src/main/java/com/etl/connector/kafka/source/format/RawDeserializationSchema.java`     | 反序列化实现，value→String→Row.of()      |
| 创建 | `connector-kafka/src/test/java/com/etl/connector/kafka/source/format/RawFormatPluginTest.java`          | Plugin 单元测试                       |
| 创建 | `connector-kafka/src/test/java/com/etl/connector/kafka/source/format/RawDeserializationSchemaTest.java` | DeserializationSchema 单元测试        |
| 修改 | `PLUGINS.md`                                                                                            | Kafka Source 部分新增 raw format 说明   |
| 创建 | `docs/examples/stream-kafka2console-raw.json`                                                           | raw format 示例配置                   |

---

### Task 1: RawFormatPlugin

**Files:**

- Create: `connector-kafka/src/main/java/com/etl/connector/kafka/source/format/RawFormatPlugin.java`
- Test: `connector-kafka/src/test/java/com/etl/connector/kafka/source/format/RawFormatPluginTest.java`

- [ ] **Step 1: 写 RawFormatPlugin 测试**

```java
package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RawFormatPluginTest {

    @Test
    void testIdentifier() {
        RawFormatPlugin plugin = new RawFormatPlugin();
        assertEquals("raw", plugin.identifier());
    }

    @Test
    void testCreateDeserializerWithValidSchema() {
        String[] fieldNames = {"message"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawFormatPlugin plugin = new RawFormatPlugin();
        assertNotNull(plugin.createDeserializer(schema));
    }

    @Test
    void testCreateDeserializerWithMultipleFieldsThrows() {
        String[] fieldNames = {"message", "extra"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.STRING, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawFormatPlugin plugin = new RawFormatPlugin();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> plugin.createDeserializer(schema));
        assertTrue(exception.getMessage().contains("raw format requires exactly one STRING field"));
    }

    @Test
    void testCreateDeserializerWithNonStringTypeThrows() {
        String[] fieldNames = {"message"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.LONG};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawFormatPlugin plugin = new RawFormatPlugin();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> plugin.createDeserializer(schema));
        assertTrue(exception.getMessage().contains("raw format requires the field type to be STRING"));
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flink-etl-connector/connector-kafka -Dtest=RawFormatPluginTest -DfailIfNoTests=false`
Expected: FAIL — `RawFormatPlugin` 类不存在

- [ ] **Step 3: 写 RawFormatPlugin 实现**

```java
package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import com.google.auto.service.AutoService;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

@AutoService(KafkaFormatPlugin.class)
public class RawFormatPlugin implements KafkaFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "raw";
    }

    @Override
    public KafkaRecordDeserializationSchema<Row> createDeserializer(EtlSchema schema) {
        if (schema.getFieldCount() != 1) {
            throw new IllegalArgumentException(
                    "raw format requires exactly one STRING field, but got " + schema.getFieldCount() + " fields");
        }
        if (schema.getFieldType(0) != Types.STRING) {
            throw new IllegalArgumentException(
                    "raw format requires the field type to be STRING, but got " + schema.getFieldType(0));
        }
        return new RawDeserializationSchema(schema);
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn test -pl flink-etl-connector/connector-kafka -Dtest=RawFormatPluginTest`
Expected: 3 个测试 PASS（identifier、validSchema、multipleFields 抛异常）

注意：此步骤需要先完成 Task 2 的 `RawDeserializationSchema` 类，否则编译失败。所以此步骤与 Task 2 合并执行。

- [ ] **Step 5: Commit**

```bash
git add flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/source/format/RawFormatPlugin.java flink-etl-connector/connector-kafka/src/test/java/com/etl/connector/kafka/source/format/RawFormatPluginTest.java
git commit -m "feat: 新增 Kafka Source raw format 插件"
```

---

### Task 2: RawDeserializationSchema

**Files:**

- Create: `connector-kafka/src/main/java/com/etl/connector/kafka/source/format/RawDeserializationSchema.java`
- Test: `connector-kafka/src/test/java/com/etl/connector/kafka/source/format/RawDeserializationSchemaTest.java`

- [ ] **Step 1: 写 RawDeserializationSchema 测试**

```java
package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RawDeserializationSchemaTest {

    @Test
    void testDeserializeNormalMessage() throws Exception {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawDeserializationSchema deserializer = new RawDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, null, "hello world".getBytes(StandardCharsets.UTF_8)
        );

        List<Row> collectedRows = new ArrayList<>();
        Collector<Row> collector = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                collectedRows.add(record);
            }
            @Override
            public void close() {}
        };

        deserializer.deserialize(record, collector);

        assertEquals(1, collectedRows.size());
        Row row = collectedRows.get(0);
        assertEquals("hello world", row.getField(0));
    }

    @Test
    void testDeserializeNullValue() throws Exception {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawDeserializationSchema deserializer = new RawDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, null, null
        );

        List<Row> collectedRows = new ArrayList<>();
        Collector<Row> collector = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                collectedRows.add(record);
            }
            @Override
            public void close() {}
        };

        deserializer.deserialize(record, collector);

        assertEquals(1, collectedRows.size());
        Row row = collectedRows.get(0);
        assertNull(row.getField(0));
    }

    @Test
    void testDeserializeEmptyBytes() throws Exception {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawDeserializationSchema deserializer = new RawDeserializationSchema(schema);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test-topic", 0, 0, null, new byte[0]
        );

        List<Row> collectedRows = new ArrayList<>();
        Collector<Row> collector = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                collectedRows.add(record);
            }
            @Override
            public void close() {}
        };

        deserializer.deserialize(record, collector);

        assertEquals(1, collectedRows.size());
        Row row = collectedRows.get(0);
        assertEquals("", row.getField(0));
    }

    @Test
    void testGetProducedType() {
        String[] fieldNames = {"message"};
        TypeInformation<?>[] fieldTypes = {Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        RawDeserializationSchema deserializer = new RawDeserializationSchema(schema);

        TypeInformation<Row> producedType = deserializer.getProducedType();
        assertEquals(Types.ROW_NAMED(fieldNames, fieldTypes), producedType);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flink-etl-connector/connector-kafka -Dtest=RawDeserializationSchemaTest -DfailIfNoTests=false`
Expected: FAIL — `RawDeserializationSchema` 类不存在

- [ ] **Step 3: 写 RawDeserializationSchema 实现**

```java
package com.etl.connector.kafka.source.format;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RawDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final EtlSchema schema;

    public RawDeserializationSchema(EtlSchema schema) {
        this.schema = schema;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
        if (record.value() == null) {
            out.collect(Row.of(null));
            return;
        }
        out.collect(Row.of(new String(record.value(), StandardCharsets.UTF_8)));
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}
```

- [ ] **Step 4: 运行所有 raw format 相关测试确认通过**

Run: `mvn test -pl flink-etl-connector/connector-kafka -Dtest="RawFormatPluginTest,RawDeserializationSchemaTest"`
Expected: 7 个测试全部 PASS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/source/format/RawDeserializationSchema.java flink-etl-connector/connector-kafka/src/test/java/com/etl/connector/kafka/source/format/RawDeserializationSchemaTest.java
git commit -m "feat: 新增 Kafka Source raw format 反序列化器"
```

---

### Task 3: SPI 注册验证 + 示例配置 + PLUGINS.md 更新

**Files:**

- Modify: `PLUGINS.md` (Kafka Source 部分)
- Create: `docs/examples/stream-kafka2console-raw.json`

- [ ] **Step 1: 安装模块验证 SPI 注册**

Run: `mvn clean install -pl flink-etl-connector/connector-kafka -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 2: 验证 SPI 文件包含 RawFormatPlugin**

检查 `connector-kafka/target/classes/META-INF/services/com.etl.connector.kafka.source.format.KafkaFormatPlugin` 是否包含
`com.etl.connector.kafka.source.format.raw.RawFormatPlugin`

- [ ] **Step 3: 验证 KafkaFormatLoader 能加载 raw format**

Run: `mvn test -pl flink-etl-connector/connector-kafka -Dtest=KafkaFormatLoaderTest`
Expected: PASS，`supportedFormats()` 应包含 `"raw"`

- [ ] **Step 4: 更新 PLUGINS.md**

在 Kafka Source 的 `format` 参数说明行（第 655 行附近），将：

```
| `format`           |  否   | `json`     | 消息格式：`json`（标准 JSON）、`debezium-json`（Debezium CDC JSON）            |
```

改为：

```
| `format`           |  否   | `json`     | 消息格式：`json`（标准 JSON）、`debezium-json`（Debezium CDC JSON）、`raw`（原始文本）            |
```

在 Kafka Source "数据解析说明" 之后（约第 755 行附近），新增 raw format 说明章节：

```markdown
#### Raw 格式配置示例

**Kafka Source Raw 格式：**

```json
{
  "source": {
    "type": "kafka",
    "outputTable": "raw_data",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "raw-consumer",
      "topics": [
        "raw-topic"
      ],
      "startupMode": "earliest",
      "format": "raw",
      "schema": {
        "message": "STRING"
      }
    }
  }
}
```

**说明：**

- `format: "raw"` 启用原始文本模式，不做任何结构化解析
- `schema` 必须配置为单个 STRING 类型字段，字段名由用户自定义（如 `message`、`content`、`value` 等）
- Kafka 消息的 value 以 UTF-8 编码转为 String，作为该字段的值输出
- Kafka key 被忽略
- 消息 value 为 null 时，输出字段值为 null

```

- [ ] **Step 5: 创建示例配置文件**

创建 `docs/examples/stream-kafka2console-raw.json`：

```json
{
  "job": {
    "name": "kafka-raw-to-console",
    "mode": "streaming",
    "parallelism": 1
  },
  "sources": [
    {
      "type": "kafka",
      "outputTable": "raw_data",
      "config": {
        "bootstrapServers": "127.0.0.1:9092",
        "groupId": "flink-etl-tool",
        "topics": ["raw-topic"],
        "startupMode": "earliest",
        "format": "raw",
        "schema": {
          "message": "STRING"
        }
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "raw_data"
    }
  ]
}
```

- [ ] **Step 6: Commit**

```bash
git add PLUGINS.md docs/examples/stream-kafka2console-raw.json
git commit -m "docs: 更新 Kafka Source raw format 文档和示例配置"
```

---

### Task 4: 全量测试验证

- [ ] **Step 1: 运行 connector-kafka 全量测试**

Run: `mvn test -pl flink-etl-connector/connector-kafka`
Expected: 所有测试 PASS

- [ ] **Step 2: 运行项目编译验证**

Run: `mvn clean compile`
Expected: BUILD SUCCESS