# Kafka Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Kafka Sink 插件，将 Flink Row 数据写入 Kafka topic，支持 JSON 序列化和可选的消息 key

**Architecture:** 参考 Kafka Source，直接使用 Flink connector 的 KafkaSink API。扩展 TypeConverter 添加 Row 转 JsonNode 能力，实现自定义序列化器，通过 SPI 注册插件。

**Tech Stack:** Java 8, Flink 1.15.2, Flink Kafka Connector, Jackson (JSR310), Lombok, AutoService

---

## File Structure

### New Files
- `flink-etl-sink/flink-etl-sink-kafka/pom.xml` — 模块配置
- `flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/KafkaSinkConfig.java` — 配置封装类
- `flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/RowToJsonSerializationSchema.java` — 序列化器
- `flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/KafkaSinkPlugin.java` — SPI 插件入口
- `flink-etl-sink/flink-etl-sink-kafka/src/test/java/com/etl/sink/kafka/KafkaSinkConfigTest.java` — 配置测试
- `flink-etl-sink/flink-etl-sink-kafka/src/test/java/com/etl/sink/kafka/RowToJsonSerializationSchemaTest.java` — 序列化测试

### Modified Files
- `flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java` — 添加 convertRowToJsonNode()
- `flink-etl-core/src/test/java/com/etl/core/schema/TypeConverterTest.java` — 添加测试用例
- `flink-etl-client/pom.xml` — 添加 sink-kafka 依赖
- `PLUGINS.md` — 新增 Kafka Sink 文档

---

## Task 1: 创建模块结构

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-kafka/pom.xml`
- Create: `flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/`
- Create: `flink-etl-sink/flink-etl-sink-kafka/src/test/java/com/etl/sink/kafka/`

- [ ] **Step 1: 创建模块目录**

```bash
mkdir -p flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka
mkdir -p flink-etl-sink/flink-etl-sink-kafka/src/test/java/com/etl/sink/kafka
```

- [ ] **Step 2: 创建 pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-sink</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flink-etl-sink-kafka</artifactId>
    <name>Flink ETL Sink - Kafka</name>
    <description>Kafka Sink 插件</description>

    <dependencies>
        <!-- 依赖 flink-etl-core -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-core</artifactId>
            <version>${project.version}</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: 更新父 pom**

修改 `flink-etl-sink/pom.xml`，在 `<modules>` 中添加：

```xml
<module>flink-etl-sink-kafka</module>
```

- [ ] **Step 4: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-kafka/
git add flink-etl-sink/pom.xml
git commit -m "feat: 创建 Kafka Sink 模块结构"
```

---

## Task 2: 扩展 TypeConverter - 编写测试

**Files:**
- Modify: `flink-etl-core/src/test/java/com/etl/core/schema/TypeConverterTest.java`

- [ ] **Step 1: 添加测试方法 - 简单 Row 转 JsonNode**

在 `TypeConverterTest.java` 中添加测试方法：

```java
@Test
void testConvertRowToJsonNode_SimpleRow() {
    // 创建简单 Row（有字段名）
    Row row = Row.withPositions(3);
    row.setField("name", "张三");
    row.setField("age", 25);
    row.setField("score", 95.5);

    // 转换为 JsonNode
    JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

    // 验证结果
    assertNotNull(jsonNode);
    assertTrue(jsonNode.isObject());
    assertEquals("张三", jsonNode.get("name").asText());
    assertEquals(25, jsonNode.get("age").asInt());
    assertEquals(95.5, jsonNode.get("score").asDouble(), 0.001);
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=TypeConverterTest#testConvertRowToJsonNode_SimpleRow -pl flink-etl-core
```

Expected: FAIL - 方法不存在

- [ ] **Step 3: 添加测试方法 - 复杂类型（嵌套、数组）**

```java
@Test
void testConvertRowToJsonNode_ComplexRow() {
    // 创建嵌套 Row
    Row addressRow = Row.withPositions(2);
    addressRow.setField("city", "北京");
    addressRow.setField("zip", "100001");

    Row userRow = Row.withPositions(3);
    userRow.setField("id", 1L);
    userRow.setField("name", "李四");
    userRow.setField("address", addressRow);

    // 转换为 JsonNode
    JsonNode jsonNode = TypeConverter.convertRowToJsonNode(userRow);

    // 验证结果
    assertNotNull(jsonNode);
    assertEquals(1L, jsonNode.get("id").asLong());
    assertEquals("李四", jsonNode.get("name").asText());
    JsonNode addressNode = jsonNode.get("address");
    assertNotNull(addressNode);
    assertEquals("北京", addressNode.get("city").asText());
    assertEquals("100001", addressNode.get("zip").asText());
}

@Test
void testConvertRowToJsonNode_WithArray() {
    // 创建包含数组的 Row
    Row row = Row.withPositions(2);
    row.setField("id", 1);
    row.setField("tags", new String[]{"tag1", "tag2", "tag3"});

    // 转换为 JsonNode
    JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

    // 验证结果
    assertNotNull(jsonNode);
    assertEquals(1, jsonNode.get("id").asInt());
    JsonNode tagsNode = jsonNode.get("tags");
    assertTrue(tagsNode.isArray());
    assertEquals(3, tagsNode.size());
    assertEquals("tag1", tagsNode.get(0).asText());
    assertEquals("tag2", tagsNode.get(1).asText());
    assertEquals("tag3", tagsNode.get(2).asText());
}
```

- [ ] **Step 4: 添加测试方法 - LocalDateTime 类型**

```java
@Test
void testConvertRowToJsonNode_LocalDateTime() {
    // 创建包含 LocalDateTime 的 Row
    LocalDateTime dateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 0);
    Row row = Row.withPositions(2);
    row.setField("id", 1);
    row.setField("createTime", dateTime);

    // 转换为 JsonNode
    JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

    // 验证结果
    assertNotNull(jsonNode);
    assertEquals(1, jsonNode.get("id").asInt());
    // LocalDateTime 应该被转为字符串
    JsonNode timeNode = jsonNode.get("createTime");
    assertNotNull(timeNode);
    assertTrue(timeNode.isTextual());
}
```

- [ ] **Step 5: 添加测试方法 - null 值处理**

```java
@Test
void testConvertRowToJsonNode_WithNull() {
    // 创建包含 null 的 Row
    Row row = Row.withPositions(3);
    row.setField("id", 1);
    row.setField("name", null);
    row.setField("score", 95.5);

    // 转换为 JsonNode
    JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

    // 验证结果
    assertNotNull(jsonNode);
    assertEquals(1, jsonNode.get("id").asInt());
    assertTrue(jsonNode.get("name").isNull());
    assertEquals(95.5, jsonNode.get("score").asDouble(), 0.001);
}
```

- [ ] **Step 6: 提交测试**

```bash
git add flink-etl-core/src/test/java/com/etl/core/schema/TypeConverterTest.java
git commit -m "test: 添加 TypeConverter.convertRowToJsonNode() 测试用例"
```

---

## Task 3: 实现 TypeConverter.convertRowToJsonNode()

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java`

- [ ] **Step 1: 添加 convertRowToJsonNode 方法签名**

在 TypeConverter 类的 `// endregion` 之前添加新方法：

```java
/**
 * 将 Flink Row 转换为 Jackson JsonNode
 * 与 convertJsonToRow() 形成对称，用于 Kafka Sink 序列化
 *
 * @param row Flink Row 对象
 * @return JsonNode 对象
 */
public static JsonNode convertRowToJsonNode(Row row) {
    if (row == null) {
        return null;
    }

    // 使用 JsonUtils.MAPPER 创建 ObjectNode
    ObjectMapper mapper = JsonUtils.getMapper();
    ObjectNode objectNode = mapper.createObjectNode();

    // 获取字段名
    Set<String> fieldNames = row.getFieldNames(true);

    if (fieldNames != null && !fieldNames.isEmpty()) {
        // 有字段名：遍历字段名
        for (String fieldName : fieldNames) {
            Object value = row.getField(fieldName);
            JsonNode fieldNode = convertValueToJsonNode(value, mapper);
            objectNode.set(fieldName, fieldNode);
        }
    } else {
        // 无字段名：使用位置索引
        int arity = row.getArity();
        for (int i = 0; i < arity; i++) {
            Object value = row.getField(i);
            JsonNode fieldNode = convertValueToJsonNode(value, mapper);
            objectNode.set("field" + i, fieldNode);
        }
    }

    return objectNode;
}
```

- [ ] **Step 2: 添加 convertValueToJsonNode 辅助方法**

```java
/**
 * 将单个值转换为 JsonNode
 *
 * @param value 字段值
 * @param mapper ObjectMapper 实例
 * @return JsonNode
 */
private static JsonNode convertValueToJsonNode(Object value, ObjectMapper mapper) {
    if (value == null) {
        return mapper.getNodeFactory().nullNode();
    }

    // 如果是 Row，递归转换
    if (value instanceof Row) {
        return convertRowToJsonNode((Row) value);
    }

    // 其他类型：使用 mapper.valueToTree
    return mapper.valueToTree(value);
}
```

- [ ] **Step 3: 在 JsonUtils 中暴露 ObjectMapper**

修改 `flink-etl-core/src/main/java/com/etl/core/utils/JsonUtils.java`，添加 getter 方法：

```java
/**
 * 获取 ObjectMapper 实例
 * 用于 TypeConverter 等 需要 ObjectMapper 的场景
 *
 * @return ObjectMapper 实例
 */
public static ObjectMapper getMapper() {
    return MAPPER;
}
```

- [ ] **Step 4: 运行测试验证通过**

```bash
mvn test -Dtest=TypeConverterTest -pl flink-etl-core
```

Expected: PASS

- [ ] **Step 5: 提交实现**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java
git add flink-etl-core/src/main/java/com/etl/core/utils/JsonUtils.java
git commit -m "feat: 实现 TypeConverter.convertRowToJsonNode()"
```

---

## Task 4: 实现 KafkaSinkConfig - 编写测试

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-kafka/src/test/java/com/etl/sink/kafka/KafkaSinkConfigTest.java`

- [ ] **Step 1: 创建测试类文件**

```java
package com.etl.sink.kafka;

import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaSinkConfigTest {

    @Test
    void testFromSinkConfig_ValidConfig() {
        // 准备配置
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");

        SinkConfig sinkConfig = new SinkConfig(configMap);

        // 解析配置
        KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(sinkConfig);

        // 验证
        assertNotNull(kafkaConfig);
        assertEquals("localhost:9092", kafkaConfig.getBootstrapServers());
        assertEquals("test-topic", kafkaConfig.getTopic());
        assertNull(kafkaConfig.getKeyField());
        assertNotNull(kafkaConfig.getKafkaProperties());
    }

    @Test
    void testFromSinkConfig_WithKeyField() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");
        configMap.put("keyField", "userId");

        SinkConfig sinkConfig = new SinkConfig(configMap);
        KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(sinkConfig);

        assertEquals("userId", kafkaConfig.getKeyField());
    }

    @Test
    void testFromSinkConfig_WithProperties() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");

        Map<String, Object> properties = new HashMap<>();
        properties.put("acks", "all");
        properties.put("retries", "3");
        configMap.put("properties", properties);

        SinkConfig sinkConfig = new SinkConfig(configMap);
        KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(sinkConfig);

        Properties kafkaProps = kafkaConfig.getKafkaProperties();
        assertNotNull(kafkaProps);
        assertEquals("all", kafkaProps.getProperty("acks"));
        assertEquals("3", kafkaProps.getProperty("retries"));
    }

    @Test
    void testFromSinkConfig_MissingBootstrapServers() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("topic", "test-topic");

        SinkConfig sinkConfig = new SinkConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            KafkaSinkConfig.fromSinkConfig(sinkConfig);
        });
    }

    @Test
    void testFromSinkConfig_MissingTopic() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");

        SinkConfig sinkConfig = new SinkConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            KafkaSinkConfig.fromSinkConfig(sinkConfig);
        });
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=KafkaSinkConfigTest -pl flink-etl-sink/flink-etl-sink-kafka
```

Expected: FAIL - 类不存在

- [ ] **Step 3: 提交测试**

```bash
git add flink-etl-sink/flink-etl-sink-kafka/src/test/java/com/etl/sink/kafka/KafkaSinkConfigTest.java
git commit -m "test: 添加 KafkaSinkConfig 测试用例"
```

---

## Task 5: 实现 KafkaSinkConfig 类

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/KafkaSinkConfig.java`

- [ ] **Step 1: 创建 KafkaSinkConfig 类**

```java
package com.etl.sink.kafka;

import com.etl.core.config.SinkConfig;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Sink 配置
 */
@Getter
@Builder
public class KafkaSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Kafka 集群地址 */
    private final String bootstrapServers;
    /** 目标 Topic */
    private final String topic;
    /** Key 字段名（可选） */
    private final String keyField;
    /** Kafka Producer 配置 */
    private final Properties kafkaProperties;

    /**
     * 从 SinkConfig 解析配置
     */
    public static KafkaSinkConfig fromSinkConfig(SinkConfig config) {
        // 校验必填参数
        String bootstrapServers = config.getString("bootstrapServers");
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException("bootstrapServers 不能为空");
        }

        String topic = config.getString("topic");
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空");
        }

        // 可选参数
        String keyField = config.getString("keyField");

        // 解析 properties
        Properties kafkaProperties = parseKafkaProperties(config);

        return KafkaSinkConfig.builder()
                .bootstrapServers(bootstrapServers)
                .topic(topic)
                .keyField(keyField)
                .kafkaProperties(kafkaProperties)
                .build();
    }

    /**
     * 解析额外的 Kafka 配置属性
     */
    @SuppressWarnings("unchecked")
    private static Properties parseKafkaProperties(SinkConfig config) {
        Properties properties = new Properties();
        Object propsObj = config.get("properties");
        if (propsObj instanceof Map) {
            ((Map<String, Object>) propsObj).forEach((key, value) -> {
                if (key != null && value != null) {
                    properties.setProperty(key, value.toString());
                }
            });
        }
        return properties;
    }
}
```

- [ ] **Step 2: 运行测试验证通过**

```bash
mvn test -Dtest=KafkaSinkConfigTest -pl flink-etl-sink/flink-etl-sink-kafka
```

Expected: PASS

- [ ] **Step 3: 提交实现**

```bash
git add flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/KafkaSinkConfig.java
git commit -m "feat: 实现 KafkaSinkConfig 配置类"
```

---

## Task 6: 实现 RowToJsonSerializationSchema - 编写测试

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-kafka/src/test/java/com/etl/sink/kafka/RowToJsonSerializationSchemaTest.java`

- [ ] **Step 1: 创建测试类**

```java
package com.etl.sink.kafka;

import com.etl.core.config.SinkConfig;
import org.apache.flink.connector.kafka.sink.KafkaRecordSinkContext;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class RowToJsonSerializationSchemaTest {

    private KafkaSinkConfig kafkaConfig;
    private KafkaRecordSinkContext mockContext;

    @BeforeEach
    void setUp() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");

        kafkaConfig = KafkaSinkConfig.fromSinkConfig(new SinkConfig(configMap));
        mockContext = mock(KafkaRecordSinkContext.class);
    }

    @Test
    void testSerialize_WithoutKey() {
        // 创建序列化器
        RowToJsonSerializationSchema schema = new RowToJsonSerializationSchema(kafkaConfig);
        schema.open(mockContext);

        // 创建 Row
        Row row = Row.withPositions(2);
        row.setField("id", 1);
        row.setField("name", "张三");

        // 序列化
        ProducerRecord<byte[], byte[]> record = schema.serialize(row, mockContext, null);

        // 验证
        assertNotNull(record);
        assertEquals("test-topic", record.topic());
        assertNull(record.key());

        // 验证 value
        byte[] value = record.value();
        assertNotNull(value);
        String json = new String(value, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"id\":1"));
        assertTrue(json.contains("\"name\":\"张三\""));
    }

    @Test
    void testSerialize_WithKey() {
        // 配置 keyField
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrapServers", "localhost:9092");
        configMap.put("topic", "test-topic");
        configMap.put("keyField", "userId");

        KafkaSinkConfig configWithKey = KafkaSinkConfig.fromSinkConfig(new SinkConfig(configMap));

        // 创建序列化器
        RowToJsonSerializationSchema schema = new RowToJsonSerializationSchema(configWithKey);
        schema.open(mockContext);

        // 创建 Row
        Row row = Row.withPositions(2);
        row.setField("userId", "user123");
        row.setField("event", "click");

        // 序列化
        ProducerRecord<byte[], byte[]> record = schema.serialize(row, mockContext, null);

        // 验证 key
        assertNotNull(record.key());
        String key = new String(record.key(), StandardCharsets.UTF_8);
        assertEquals("user123", key);

        // 验证 value
        assertNotNull(record.value());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

```bash
mvn test -Dtest=RowToJsonSerializationSchemaTest -pl flink-etl-sink/flink-etl-sink-kafka
```

Expected: FAIL - 类不存在

- [ ] **Step 3: 提交测试**

```bash
git add flink-etl-sink/flink-etl-sink-kafka/src/test/java/com/etl/sink/kafka/RowToJsonSerializationSchemaTest.java
git commit -m "test: 添加 RowToJsonSerializationSchema 测试用例"
```

---

## Task 7: 实现 RowToJsonSerializationSchema 类

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/RowToJsonSerializationSchema.java`

- [ ] **Step 1: 创建序列化器类**

```java
package com.etl.sink.kafka;

import com.etl.core.schema.TypeConverter;
import com.etl.core.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSinkContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Row 到 Kafka 消息的 JSON 序列化器
 */
@Slf4j
public class RowToJsonSerializationSchema implements KafkaRecordSerializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private final KafkaSinkConfig config;
    private transient ObjectMapper objectMapper;

    public RowToJsonSerializationSchema(KafkaSinkConfig config) {
        this.config = config;
    }

    @Override
    public void open(KafkaRecordSinkContext context) throws IOException {
        // 创建专用的 ObjectMapper（配置 JSR310 支持）
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        log.info("Kafka Sink 序列化器已初始化: topic={}, keyField={}",
                 config.getTopic(), config.getKeyField());
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Row row, KafkaRecordSinkContext context, Long timestamp) {
        // 1. 序列化 Key
        byte[] key = serializeKey(row);

        // 2. 序列化 Value
        byte[] value = serializeValue(row);

        // 3. 返回 ProducerRecord
        return new ProducerRecord<>(config.getTopic(), null, key, value);
    }

    /**
     * 序列化消息 Key
     */
    private byte[] serializeKey(Row row) {
        String keyField = config.getKeyField();
        if (keyField == null || keyField.isEmpty()) {
            return null;
        }

        // 提取字段值
        Object keyValue = row.getField(keyField);
        if (keyValue == null) {
            log.warn("Key 字段 '{}' 的值为 null", keyField);
            return null;
        }

        // 转为字符串
        String keyString = keyValue.toString();
        return keyString.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 序列化消息 Value
     */
    private byte[] serializeValue(Row row) {
        try {
            // Row -> JsonNode
            JsonNode jsonNode = TypeConverter.convertRowToJsonNode(row);

            // JsonNode -> JSON 字符串
            String jsonString = objectMapper.writeValueAsString(jsonNode);

            // 转为 bytes
            return jsonString.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("JSON 序列化失败: " + e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 2: 运行测试验证通过**

```bash
mvn test -Dtest=RowToJsonSerializationSchemaTest -pl flink-etl-sink/flink-etl-sink-kafka
```

Expected: PASS

- [ ] **Step 3: 提交实现**

```bash
git add flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/RowToJsonSerializationSchema.java
git commit -m "feat: 实现 RowToJsonSerializationSchema 序列化器"
```

---

## Task 8: 实现 KafkaSinkPlugin

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/KafkaSinkPlugin.java`

- [ ] **Step 1: 创建插件类**

```java
package com.etl.sink.kafka;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.types.Row;

/**
 * Kafka Sink 插件
 * 将数据写入 Kafka Topic
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class KafkaSinkPlugin implements SinkPlugin {

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public Sink<Row> createSink(SinkConfig config) {
        log.info("创建 Kafka Sink");

        // 解析配置
        KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(config);

        // 构建 KafkaSink
        return KafkaSink.<Row>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setRecordSerializer(new RowToJsonSerializationSchema(kafkaConfig))
                .setKafkaProducerConfig(kafkaConfig.getKafkaProperties())
                .build();
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
mvn compile -pl flink-etl-sink/flink-etl-sink-kafka
```

Expected: SUCCESS

- [ ] **Step 3: 提交实现**

```bash
git add flink-etl-sink/flink-etl-sink-kafka/src/main/java/com/etl/sink/kafka/KafkaSinkPlugin.java
git commit -m "feat: 实现 KafkaSinkPlugin SPI 入口"
```

---

## Task 9: 更新 flink-etl-client 依赖

**Files:**
- Modify: `flink-etl-client/pom.xml`

- [ ] **Step 1: 添加 sink-kafka 依赖**

在 `flink-etl-client/pom.xml` 的 `<dependencies>` 中添加：

```xml
<dependency>
    <groupId>com.etl</groupId>
    <artifactId>flink-etl-sink-kafka</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 2: 编译验证**

```bash
mvn compile -pl flink-etl-client
```

Expected: SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-client/pom.xml
git commit -m "feat: 在 flink-etl-client 中添加 Kafka Sink 依赖"
```

---

## Task 10: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 在 JDBC Sink 章节之后添加 Kafka Sink 文档**

在 `PLUGINS.md` 的 JDBC Sink 章节之后添加：

```markdown
---

### Kafka Sink

将数据写入 Kafka Topic，消息体为 JSON 字符串格式。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `bootstrapServers` | 是 | - | Kafka 集群地址，如 `localhost:9092` |
| `topic` | 是 | - | 目标 Topic 名称 |
| `keyField` | 否 | - | 从 Row 字段提取消息 key，不配置则消息无 key |
| `properties` | 否 | `{}` | 额外的 Kafka producer 配置 |

#### 配置示例

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

#### 数据格式说明

- **消息体格式**：JSON 字符串
- **字段映射**：Row 字段名 → JSON key
- **嵌套结构**：Row 嵌套 Row → JSON 嵌套对象
- **数组类型**：Row 数组字段 → JSON 数组
- **日期时间**：LocalDateTime 自动转为 `yyyy-MM-dd HH:mm:ss` 格式字符串

#### 与 Kafka Source 的对应关系

Kafka Source 消费的消息可以被 Kafka Sink 写回，形成完整闭环：
- **Source**：Kafka JSON 消息 → Row
- **Sink**：Row → Kafka JSON 消息
```

- [ ] **Step 2: 提交文档**

```bash
git add PLUGINS.md
git commit -m "docs: 在 PLUGINS.md 中添加 Kafka Sink 文档"
```

---

## Task 11: 运行完整测试

- [ ] **Step 1: 编译整个项目**

```bash
mvn clean compile
```

Expected: SUCCESS

- [ ] **Step 2: 运行所有测试**

```bash
mvn test
```

Expected: 所有测试通过

- [ ] **Step 3: 打包验证**

```bash
mvn clean package -DskipTests
```

Expected: 生成 JAR 文件

---

## Task 12: 最终提交

- [ ] **Step 1: 检查所有更改**

```bash
git status
```

确认所有文件已提交

- [ ] **Step 2: 推送到远程（如需要）**

```bash
git push origin master
```

---

## 实现完成检查清单

- [ ] TypeConverter 扩展完成并通过测试
- [ ] KafkaSinkConfig 实现完成并通过测试
- [ ] RowToJsonSerializationSchema 实现完成并通过测试
- [ ] KafkaSinkPlugin SPI 注册成功
- [ ] flink-etl-client 依赖更新
- [ ] PLUGINS.md 文档更新
- [ ] 所有测试通过
- [ ] 项目编译打包成功