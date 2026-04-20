# Kafka Source 配置项重命名实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Kafka Source 配置项 `startingOffsets` 重命名为 `startupMode`，并使用枚举类替代字符串类型

**Architecture:** 创建 `StartupMode` 枚举类封装启动模式逻辑，修改 `KafkaSourceConfig` 使用枚举类型，更新所有相关文档和示例

**Tech Stack:** Java 11, Lombok, Flink Kafka Connector

---

## 文件结构

| 文件 | 操作 | 说明 |
|------|------|------|
| `flink-etl-source-kafka/.../StartupMode.java` | 创建 | 启动模式枚举类 |
| `flink-etl-source-kafka/.../KafkaSourceConfig.java` | 修改 | 使用枚举替代字符串 |
| `PLUGINS.md` | 修改 | 更新配置文档 |
| `docs/examples/kafka-to-console.json` | 修改 | 更新示例配置 |
| `docs/examples/kafka-regex-to-console.json` | 修改 | 更新示例配置 |

---

### Task 1: 创建 StartupMode 枚举类

**Files:**
- Create: `flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/StartupMode.java`

- [ ] **Step 1: 创建 StartupMode 枚举类**

```java
package com.etl.source.kafka;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Kafka Source 启动模式枚举
 */
public enum StartupMode {
    /** 从最早的记录开始消费 */
    EARLIEST("earliest") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.earliest();
        }
    },
    /** 从最新的记录开始消费 */
    LATEST("latest") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.latest();
        }
    },
    /** 从已提交的 offset 开始消费 */
    COMMITTED("committed") {
        @Override
        public OffsetsInitializer toOffsetsInitializer() {
            return OffsetsInitializer.committedOffsets();
        }
    };

    private final String configValue;

    StartupMode(String configValue) {
        this.configValue = configValue;
    }

    /**
     * 获取配置值
     */
    public String getConfigValue() {
        return configValue;
    }

    /**
     * 转换为 Flink OffsetsInitializer
     */
    public abstract OffsetsInitializer toOffsetsInitializer();

    /**
     * 从配置字符串解析启动模式
     *
     * @param value 配置值（不区分大小写）
     * @return 对应的启动模式，如果未找到则返回 EARLIEST
     */
    public static StartupMode fromConfigValue(String value) {
        if (value == null) {
            return EARLIEST;
        }
        for (StartupMode mode : values()) {
            if (mode.configValue.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        return EARLIEST;
    }

    /**
     * 校验配置值是否有效
     *
     * @param value 配置值
     * @return 是否有效
     */
    public static boolean isValid(String value) {
        if (value == null) {
            return false;
        }
        for (StartupMode mode : values()) {
            if (mode.configValue.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn compile -pl flink-etl-source/flink-etl-source-kafka -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/StartupMode.java
git commit -m "feat(kafka): 新增 StartupMode 枚举类"
```

---

### Task 2: 修改 KafkaSourceConfig 使用枚举

**Files:**
- Modify: `flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourceConfig.java`

- [ ] **Step 1: 更新 KafkaSourceConfig.java**

完整替换文件内容：

```java
package com.etl.source.kafka;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    /** 启动模式 */
    private final StartupMode startupMode;
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
        List<String> topics = parseTopics(config);
        String topicPattern = config.getString("topicPattern");
        if ((topics == null || topics.isEmpty()) && topicPattern == null) {
            throw new IllegalArgumentException("topics 和 topicPattern 至少需要配置一个");
        }

        // 解析 startupMode（支持新旧配置名）
        StartupMode startupMode = parseStartupMode(config);

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
                .startupMode(startupMode)
                .kafkaProperties(kafkaProperties)
                .schema(schema)
                .build();
    }

    /**
     * 解析启动模式配置
     * 优先使用 startupMode，兼容旧的 startingOffsets
     */
    private static StartupMode parseStartupMode(SourceConfig config) {
        // 优先读取新配置名
        String startupModeValue = config.getString("startupMode");
        if (startupModeValue != null) {
            if (!StartupMode.isValid(startupModeValue)) {
                throw new IllegalArgumentException(
                        "startupMode 必须是 earliest、latest 或 committed，当前值: " + startupModeValue);
            }
            return StartupMode.fromConfigValue(startupModeValue);
        }

        // 兼容旧配置名 startingOffsets
        String legacyValue = config.getString("startingOffsets");
        if (legacyValue != null) {
            if (!StartupMode.isValid(legacyValue)) {
                throw new IllegalArgumentException(
                        "startingOffsets 必须是 earliest、latest 或 committed，当前值: " + legacyValue);
            }
            return StartupMode.fromConfigValue(legacyValue);
        }

        // 默认值
        return StartupMode.EARLIEST;
    }

    /**
     * 判断是否使用 Topic 列表模式
     */
    public boolean isTopicsMode() {
        return topics != null && !topics.isEmpty();
    }

    /**
     * 获取 Flink OffsetsInitializer
     * 封装枚举到 Flink 组件的转换，调用方无需了解枚举内部实现
     */
    public OffsetsInitializer getOffsetsInitializer() {
        return startupMode.toOffsetsInitializer();
    }

    /**
     * 解析 topics 列表
     */
    private static List<String> parseTopics(SourceConfig config) {
        Object topicsObj = config.get("topics");
        if (topicsObj == null) {
            return null;
        }
        if (topicsObj instanceof List) {
            List<String> result = new ArrayList<>();
            for (Object item : (List<?>) topicsObj) {
                if (item != null) {
                    result.add(item.toString());
                }
            }
            return result;
        }
        return null;
    }

    /**
     * 解析额外的 Kafka 配置属性
     */
    @SuppressWarnings("unchecked")
    private static Properties parseKafkaProperties(SourceConfig config) {
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

- [ ] **Step 2: 编译验证**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn compile -pl flink-etl-source/flink-etl-source-kafka -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-source/flink-etl-source-kafka/src/main/java/com/etl/source/kafka/KafkaSourceConfig.java
git commit -m "refactor(kafka): 使用 StartupMode 枚举替代字符串，配置项重命名为 startupMode（向后兼容）"
```

---

### Task 3: 更新文档 PLUGINS.md

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 更新配置参数说明**

找到第 265 行，将：

```markdown
| `startingOffsets` | 否 | `earliest` | 起始位置：`earliest`、`latest`、`committed` |
```

替换为：

```markdown
| `startupMode` | 否 | `earliest` | 启动模式：`earliest`（从最早开始）、`latest`（从最新开始）、`committed`（从已提交 offset 开始） |
```

- [ ] **Step 2: 更新 Topic 列表模式示例**

找到第 284 行，将：

```json
"startingOffsets": "earliest",
```

替换为：

```json
"startupMode": "earliest",
```

- [ ] **Step 3: 更新正则匹配模式示例**

找到第 306 行，将：

```json
"startingOffsets": "latest",
```

替换为：

```json
"startupMode": "latest",
```

- [ ] **Step 4: 提交**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 Kafka Source 配置文档，startingOffsets 改为 startupMode"
```

---

### Task 4: 更新示例配置文件

**Files:**
- Modify: `docs/examples/kafka-to-console.json`
- Modify: `docs/examples/kafka-regex-to-console.json`

- [ ] **Step 1: 更新 kafka-to-console.json**

将 `"startingOffsets": "earliest"` 改为 `"startupMode": "earliest"`

- [ ] **Step 2: 更新 kafka-regex-to-console.json**

将 `"startingOffsets": "latest"` 改为 `"startupMode": "latest"`

- [ ] **Step 3: 提交**

```bash
git add docs/examples/stream-kafka2console.json docs/examples/stream-kafka2console-topic-pattern.json
git commit -m "docs: 更新 Kafka Source 示例配置，使用 startupMode"
```

---

### Task 5: 最终验证

- [ ] **Step 1: 完整编译**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn clean compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 2: 打包验证**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn clean package -DskipTests -q`
Expected: BUILD SUCCESS

---

## 设计决策

### 向后兼容

为避免破坏现有用户配置，`parseStartupMode` 方法同时支持新旧配置名：
- 优先读取 `startupMode`（新配置名）
- 如果不存在，回退到 `startingOffsets`（旧配置名）
- 两者都不存在时，使用默认值 `EARLIEST`

### 枚举设计

`StartupMode` 枚举类：
- 封装了配置值到 Flink `OffsetsInitializer` 的转换逻辑
- 提供类型安全的配置解析
- 支持不区分大小写的配置值解析