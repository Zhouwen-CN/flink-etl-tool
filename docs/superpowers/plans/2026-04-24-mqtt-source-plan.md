# MQTT Source 插件实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 MQTT Source 插件，使用 Eclipse Paho 客户端订阅 MQTT topic，消费 JSON 消息并转换为 Flink Row 数据。

**Architecture:** 使用项目抽象层（AbstractSplitSource + BaseSplitEnumerator + BaseSourceReader），单 topic 单分片模式，流式运行。

**Tech Stack:** Eclipse Paho MQTT Client 1.2.5, Flink 1.15.2 Source API, JsonToRowConverter

---

## 文件结构

```
flink-etl-connector/connector-mqtt/
├── pom.xml                                    # Maven 模块配置
└── src/main/java/com/etl/connector/mqtt/source/
    ├── MqttSourcePlugin.java                   # SPI 入口
    ├── MqttSource.java                         # AbstractSplitSource 实现
    ├── MqttSourceConfig.java                   # 配置封装
    ├── MqttSplit.java                          # 分片定义
    ├── MqttSplitEnumerator.java                # 分片枚举器
    ├── MqttSourceReader.java                   # Source Reader
    ├── MqttSplitReader.java                    # MQTT 消费逻辑
    ├── MqttRecordEmitter.java                  # JSON → Row 转换
    ├── MqttSplitState.java                     # 分片状态
    ├── MqttEnumCheckpoint.java                 # 检查点
    └── StartupMode.java                        # 启动模式枚举
```

---

### Task 1: 创建 Maven 模块

**Files:**
- Create: `flink-etl-connector/connector-mqtt/pom.xml`
- Modify: `flink-etl-connector/pom.xml:20-28` (添加 module)
- Modify: `flink-etl-client/pom.xml:19-56` (添加依赖)

- [ ] **Step 1: 创建 connector-mqtt/pom.xml**

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

    <artifactId>connector-mqtt</artifactId>

    <name>Flink ETL Connector - MQTT</name>
    <description>MQTT 连接器（Source）</description>

    <dependencies>
        <!-- Eclipse Paho MQTT Client -->
        <dependency>
            <groupId>org.eclipse.paho</groupId>
            <artifactId>org.eclipse.paho.client.mqttv3</artifactId>
            <version>1.2.5</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 在 flink-etl-connector/pom.xml 中添加 module**

在 `<modules>` 标签内添加 `<module>connector-mqtt</module>`：

```xml
    <modules>
        <module>connector-jdbc</module>
        <module>connector-kafka</module>
        <module>connector-localfile</module>
        <module>connector-console</module>
        <module>connector-http</module>
        <module>connector-mock</module>
        <module>connector-cdc</module>
        <module>connector-mqtt</module>
    </modules>
```

- [ ] **Step 3: 在 flink-etl-client/pom.xml 中添加依赖**

在连接器依赖区域添加：

```xml
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>connector-mqtt</artifactId>
            <version>${project.version}</version>
        </dependency>
```

- [ ] **Step 4: 验证模块结构正确**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: 编译失败（无源文件），但模块结构正确识别

- [ ] **Step 5: Commit**

```bash
git add flink-etl-connector/connector-mqtt/pom.xml flink-etl-connector/pom.xml flink-etl-client/pom.xml
git commit -m "feat(mqtt): 新增 MQTT connector Maven 模块配置

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: 创建启动模式枚举

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/StartupMode.java`

- [ ] **Step 1: 创建 StartupMode 枚举**

```java
package com.etl.connector.mqtt.source;

/**
 * MQTT Source 启动模式枚举
 */
public enum StartupMode {
    /** 从 broker 保留的最早消息开始（retained message） */
    EARLIEST("earliest"),
    /** 从最新消息开始（订阅后发布的新消息） */
    LATEST("latest");

    private final String configValue;

    StartupMode(String configValue) {
        this.configValue = configValue;
    }

    /**
     * 从配置字符串解析启动模式
     *
     * @param value 配置值（不区分大小写）
     * @return 对应的启动模式
     */
    public static StartupMode fromConfigValue(String value) {
        for (StartupMode mode : values()) {
            if (mode.configValue.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "startupMode 必须是 earliest 或 latest，当前值: " + value);
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/StartupMode.java
git commit -m "feat(mqtt): 新增 MQTT Source 启动模式枚举

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 3: 创建配置封装类

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSourceConfig.java`

- [ ] **Step 1: 创建 MqttSourceConfig**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.UUID;

/**
 * MQTT Source 配置
 * 用于传递所有参数到 Enumerator 和 SplitReader
 */
@Getter
@Builder
public class MqttSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** MQTT broker 地址，如 tcp://localhost:1883 */
    private final String broker;
    /** 订阅的 topic 名称 */
    private final String topic;
    /** 客户端 ID（可选，默认自动生成 UUID） */
    private final String clientId;
    /** 认证用户名（可选） */
    private final String username;
    /** 认证密码（可选） */
    private final String password;
    /** 启动模式 */
    private final StartupMode startupMode;
    /** Schema 定义 */
    private final EtlSchema schema;

    /**
     * 从 SourceConfig 解析配置
     *
     * @param config Source 配置
     * @return MQTT 配置对象
     */
    public static MqttSourceConfig fromSourceConfig(SourceConfig config) {
        // 校验必填参数
        String broker = config.getString("broker");
        Preconditions.checkArgument(broker != null && !broker.isEmpty(),
                "broker 不能为空");

        String topic = config.getString("topic");
        Preconditions.checkArgument(topic != null && !topic.isEmpty(),
                "topic 不能为空");

        // clientId（可选，默认自动生成）
        String clientId = config.getString("clientId");
        if (clientId == null || clientId.isEmpty()) {
            clientId = "mqtt-source-" + UUID.randomUUID().toString().substring(0, 8);
        }

        // username/password（可选）
        String username = config.getString("username");
        String password = config.getString("password");

        // startupMode（可选，默认 latest）
        String startupModeValue = config.getString("startupMode", "latest");
        StartupMode startupMode = StartupMode.fromConfigValue(startupModeValue);

        // schema（必填）
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema 不能为空");

        return MqttSourceConfig.builder()
                .broker(broker)
                .topic(topic)
                .clientId(clientId)
                .username(username)
                .password(password)
                .startupMode(startupMode)
                .schema(schema)
                .build();
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSourceConfig.java
git commit -m "feat(mqtt): 新增 MQTT Source 配置封装类

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 4: 创建分片和状态类

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplit.java`
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitState.java`
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttEnumCheckpoint.java`

- [ ] **Step 1: 创建 MqttSplit**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.Getter;

/**
 * MQTT 分片
 * 单分片模式，包含完整的 MQTT 配置
 */
@Getter
public class MqttSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    /** 分片 ID */
    private final String splitId;

    /** MQTT 配置 */
    private final MqttSourceConfig config;

    /**
     * 构造函数
     *
     * @param splitId 分片 ID
     * @param config  MQTT 配置
     */
    public MqttSplit(String splitId, MqttSourceConfig config) {
        this.splitId = splitId;
        this.config = config;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "MqttSplit{" +
                "splitId='" + splitId + '\'' +
                ", topic='" + config.getTopic() + '\'' +
                ", broker='" + config.getBroker() + '\'' +
                '}';
    }
}
```

- [ ] **Step 2: 创建 MqttSplitState**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;
import lombok.Setter;

/**
 * MQTT 分片状态
 */
@Getter
@Setter
public class MqttSplitState extends BaseSplitState<MqttSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param split MQTT 分片
     */
    public MqttSplitState(MqttSplit split) {
        super(split);
    }

    @Override
    public String toString() {
        return "MqttSplitState{" +
                "split=" + getSplit() +
                ", recordsRead=" + getRecordsRead() +
                '}';
    }
}
```

- [ ] **Step 3: 创建 MqttEnumCheckpoint**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;

import java.util.Collection;

/**
 * MQTT 分片枚举器检查点
 */
public class MqttEnumCheckpoint extends BaseEnumCheckpoint<MqttSplit> {

    private static final long serialVersionUID = DefaultCheckpointSerializer.VERSION;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public MqttEnumCheckpoint(Collection<MqttSplit> pendingSplits) {
        super(pendingSplits);
    }
}
```

- [ ] **Step 4: 验证编译**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplit.java \
        flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitState.java \
        flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttEnumCheckpoint.java
git commit -m "feat(mqtt): 新增 MQTT 分片、状态和检查点类

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 5: 创建分片枚举器

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitEnumerator.java`

- [ ] **Step 1: 创建 MqttSplitEnumerator**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseSplitEnumerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * MQTT 分片枚举器
 * 单分片模式，创建一个包含完整配置的分片
 */
@Slf4j
public class MqttSplitEnumerator extends BaseSplitEnumerator<MqttSplit, MqttEnumCheckpoint> {

    private final MqttSourceConfig mqttSourceConfig;

    /**
     * 构造函数
     *
     * @param context           枚举器上下文
     * @param mqttSourceConfig  MQTT 配置
     */
    public MqttSplitEnumerator(
            SplitEnumeratorContext<MqttSplit> context,
            MqttSourceConfig mqttSourceConfig) {
        super(context);
        this.mqttSourceConfig = mqttSourceConfig;
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context           枚举器上下文
     * @param checkpoint        检查点
     * @param mqttSourceConfig  MQTT 配置
     */
    public MqttSplitEnumerator(
            SplitEnumeratorContext<MqttSplit> context,
            MqttEnumCheckpoint checkpoint,
            MqttSourceConfig mqttSourceConfig) {
        super(context, checkpoint);
        this.mqttSourceConfig = mqttSourceConfig;
    }

    @Override
    public void start() {
        log.info("MqttSplitEnumerator 启动，broker: {}, topic: {}",
                mqttSourceConfig.getBroker(), mqttSourceConfig.getTopic());

        // 创建单分片
        MqttSplit split = new MqttSplit("mqtt-split-0", mqttSourceConfig);

        // 添加到待处理队列
        addPendingSplits(Collections.singletonList(split));
        log.info("创建 MQTT 分片: {}", split);
    }

    @Override
    public MqttEnumCheckpoint snapshotState(long checkpointId) {
        List<MqttSplit> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new MqttEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("MqttSplitEnumerator 关闭");
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitEnumerator.java
git commit -m "feat(mqtt): 新增 MQTT 分片枚举器

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 6: 创建 MQTT 分片读取器（核心消费逻辑）

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitReader.java`

- [ ] **Step 1: 创建 MqttSplitReader**

```java
package com.etl.connector.mqtt.source;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 分片读取器
 * 使用 Paho 客户端订阅 topic，阻塞读取消息
 */
@Slf4j
public class MqttSplitReader implements MqttCallback {

    private static final int QOS = 1;
    private static final int QUEUE_CAPACITY = 1000;
    private static final long FETCH_TIMEOUT_MS = 100;

    private final Queue<MqttSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    /** 消息阻塞队列 */
    private final BlockingQueue<MqttMessageRecord> messageQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    /** MQTT 客户端 */
    private MqttClient mqttClient;

    /** 当前订阅的 topic */
    private String currentTopic;

    /** 是否已连接 */
    private volatile boolean connected = false;

    @Override
    public RecordsWithSplitIds<MqttMessageRecord> fetch() throws IOException {
        RecordsBySplits.Builder<MqttMessageRecord> builder = new RecordsBySplits.Builder<>();

        // 尝试从队列获取消息
        MqttMessageRecord record;
        try {
            record = messageQueue.poll(FETCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return builder.build();
        }

        if (record != null) {
            // 有消息，添加到结果
            builder.add(record.getSplitId(), record);
        }

        // 检查是否有已完成的分片
        builder.addFinishedSplits(finishedSplits);

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MqttSplit> splitsChanges) {
        for (MqttSplit split : splitsChanges.splits()) {
            pendingSplits.add(split);
            log.debug("接收到 MQTT 分片: {}", split);
        }

        // 处理第一个分片（启动连接）
        if (!connected && !pendingSplits.isEmpty()) {
            MqttSplit split = pendingSplits.poll();
            connectAndSubscribe(split);
        }
    }

    /**
     * 连接 MQTT broker 并订阅 topic
     */
    private void connectAndSubscribe(MqttSplit split) {
        MqttSourceConfig config = split.getConfig();

        try {
            // 创建 MQTT 客户端
            mqttClient = new MqttClient(config.getBroker(), config.getClientId());

            // 设置连接选项
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            options.setAutomaticReconnect(true);
            options.setKeepAliveInterval(60);

            // 设置认证信息（可选）
            if (config.getUsername() != null && !config.getUsername().isEmpty()) {
                options.setUserName(config.getUsername());
            }
            if (config.getPassword() != null && !config.getPassword().isEmpty()) {
                options.setPassword(config.getPassword().toCharArray());
            }

            // 设置回调
            mqttClient.setCallback(this);

            // 连接
            mqttClient.connect(options);
            connected = true;
            currentTopic = config.getTopic();

            log.info("MQTT 客户端已连接: broker={}, clientId={}, topic={}",
                    config.getBroker(), config.getClientId(), currentTopic);

            // 订阅 topic
            mqttClient.subscribe(currentTopic, QOS);
            log.info("已订阅 topic: {}, QoS: {}", currentTopic, QOS);

        } catch (MqttException e) {
            log.error("MQTT 连接失败: {}", e.getMessage(), e);
            throw new RuntimeException("MQTT 连接失败: " + e.getMessage(), e);
        }
    }

    // ===== MqttCallback 实现 =====

    @Override
    public void connectionLost(Throwable cause) {
        log.warn("MQTT 连接丢失: {}", cause.getMessage());
        connected = false;
        // Paho 会自动重连（automaticReconnect=true）
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        try {
            byte[] payload = message.getPayload();
            if (payload == null || payload.length == 0) {
                log.warn("收到空消息，topic: {}", topic);
                return;
            }

            String jsonContent = new String(payload, StandardCharsets.UTF_8);
            log.debug("收到 MQTT 消息: topic={}, payload={}", topic, jsonContent);

            // 射入队列
            MqttMessageRecord record = new MqttMessageRecord("mqtt-split-0", jsonContent);
            messageQueue.put(record);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("消息入队被中断");
        } catch (Exception e) {
            log.error("处理 MQTT 消息失败: {}", e.getMessage(), e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // 不处理发布确认
    }

    @Override
    public void wakeUp() {
        // 唤醒阻塞的 fetch 操作
    }

    @Override
    public void close() throws Exception {
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                mqttClient.unsubscribe(currentTopic);
                mqttClient.disconnect();
                mqttClient.close();
                log.info("MQTT 客户端已关闭");
            } catch (MqttException e) {
                log.warn("关闭 MQTT 客户端异常: {}", e.getMessage());
            }
        }
        connected = false;
    }
}
```

- [ ] **Step 2: 创建 MqttMessageRecord（消息记录包装类）**

```java
package com.etl.connector.mqtt.source;

import lombok.Getter;

import java.io.Serializable;

/**
 * MQTT 消息记录
 * 包装 MQTT 消息内容，用于传递到 RecordEmitter
 */
@Getter
public class MqttMessageRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 分片 ID */
    private final String splitId;

    /** JSON 消息内容 */
    private final String jsonContent;

    /**
     * 构造函数
     *
     * @param splitId     分片 ID
     * @param jsonContent JSON 消息内容
     */
    public MqttMessageRecord(String splitId, String jsonContent) {
        this.splitId = splitId;
        this.jsonContent = jsonContent;
    }

    @Override
    public String toString() {
        return "MqttMessageRecord{" +
                "splitId='" + splitId + '\'' +
                ", jsonContent='" + jsonContent + '\'' +
                '}';
    }
}
```

- [ ] **Step 3: 验证编译**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitReader.java \
        flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttMessageRecord.java
git commit -m "feat(mqtt): 新增 MQTT 分片读取器和消息记录类

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 7: 创建记录发射器

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttRecordEmitter.java`

- [ ] **Step 1: 创建 MqttRecordEmitter**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

/**
 * MQTT 记录发射器
 * 将 JSON 消息转换为 Row 并发射到下游
 */
@Slf4j
public class MqttRecordEmitter implements RecordEmitter<MqttMessageRecord, Row, MqttSplitState> {

    @Override
    public void emitRecord(MqttMessageRecord record, SourceOutput<Row> output, MqttSplitState splitState) throws Exception {
        try {
            // 解析 JSON
            JsonNode jsonNode = JsonUtils.parseJson(record.getJsonContent());

            if (jsonNode == null || !jsonNode.isObject()) {
                log.warn("JSON 解析失败或不是对象类型: {}", record.getJsonContent());
                return;
            }

            // 转换为 Row
            EtlSchema schema = splitState.getSplit().getConfig().getSchema();
            Row row = JsonToRowConverter.convertJsonToRow(jsonNode, schema);

            // 发射到下游
            output.collect(row);

            // 更新状态
            splitState.addRecordsRead(1);

        } catch (Exception e) {
            log.error("JSON 转 Row 失败: {}, 原始消息: {}", e.getMessage(), record.getJsonContent());
            // 跳过该消息，继续处理后续消息
        }
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttRecordEmitter.java
git commit -m "feat(mqtt): 新增 MQTT 记录发射器

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 8: 创建 Source Reader

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSourceReader.java`

- [ ] **Step 1: 创建 MqttSourceReader**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseSourceReader;
import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;

import java.util.function.Supplier;

/**
 * MQTT Source Reader
 * 继承 BaseSourceReader，自动处理线程模型和状态管理
 */
@Slf4j
public class MqttSourceReader extends BaseSourceReader<MqttMessageRecord, Row, MqttSplit, MqttSplitState> {

    public MqttSourceReader(
            Supplier<BaseSplitReader<MqttMessageRecord, MqttSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new MqttRecordEmitter(), context);
    }

    @Override
    public MqttSplitState initializedState(MqttSplit split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new MqttSplitState(split);
    }

    @Override
    protected MqttSplit toSplitType(String splitId, MqttSplitState splitState) {
        return splitState.getSplit();
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSourceReader.java
git commit -m "feat(mqtt): 新增 MQTT Source Reader

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 9: 创建 MQTT Source 主类

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSource.java`

- [ ] **Step 1: 创建 MqttSource**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * MQTT Source 实现
 * 使用 Paho 客户端订阅 MQTT topic，消费 JSON 消息
 */
@Slf4j
public class MqttSource extends AbstractSplitSource<MqttSplit, MqttEnumCheckpoint> {

    private final MqttSourceConfig mqttSourceConfig;

    public MqttSource(SourceConfig config) {
        super(config);
        this.mqttSourceConfig = MqttSourceConfig.fromSourceConfig(config);
        log.info("创建 MqttSource: broker={}, topic={}, clientId={}",
                mqttSourceConfig.getBroker(),
                mqttSourceConfig.getTopic(),
                mqttSourceConfig.getClientId());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MqttSplit, MqttEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<MqttSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new MqttSplitEnumerator(enumContext, mqttSourceConfig);
    }

    @Override
    public SplitEnumerator<MqttSplit, MqttEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<MqttSplit> enumContext,
            MqttEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new MqttSplitEnumerator(enumContext, checkpoint, mqttSourceConfig);
    }

    @Override
    public SourceReader<Row, MqttSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<BaseSplitReader<MqttMessageRecord, MqttSplit>> splitReaderSupplier =
                () -> new MqttSplitReader();
        return new MqttSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<MqttSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<MqttEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSource.java
git commit -m "feat(mqtt): 新增 MQTT Source 主类

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 10: 创建 SPI 插件入口

**Files:**
- Create: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSourcePlugin.java`

- [ ] **Step 1: 创建 MqttSourcePlugin**

```java
package com.etl.connector.mqtt.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.types.Row;

/**
 * MQTT Source 插件
 * 使用 Eclipse Paho 客户端订阅 MQTT topic
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class MqttSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "mqtt";
    }

    @Override
    public Source<Row, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 MQTT Source");
        return new MqttSource(config);
    }
}
```

- [ ] **Step 2: 验证编译并生成 SPI 配置**

Run: `mvn clean compile -pl flink-etl-connector/connector-mqtt -am`
Expected: BUILD SUCCESS，META-INF/services/com.etl.core.spi.SourcePlugin 文件自动生成

- [ ] **Step 3: 验证 SPI 文件内容**

Run: `cat flink-etl-connector/connector-mqtt/target/classes/META-INF/services/com.etl.core.spi.SourcePlugin`
Expected: `com.etl.connector.mqtt.source.MqttSourcePlugin`

- [ ] **Step 4: Commit**

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSourcePlugin.java
git commit -m "feat(mqtt): 新增 MQTT Source SPI 插件入口

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 11: 完整编译验证

**Files:**
- 无新增文件

- [ ] **Step 1: 编译整个项目**

Run: `mvn clean compile`
Expected: BUILD SUCCESS

- [ ] **Step 2: 打包验证**

Run: `mvn clean package -DskipTests`
Expected: BUILD SUCCESS，flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar 包含 MQTT 插件

- [ ] **Step 3: 验证 JAR 包包含 MQTT 插件**

Run: `jar tf flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar | grep -i mqtt`
Expected: 显示 MQTT 相关类文件

- [ ] **Step 4: Commit（如有变更）**

无变更，跳过提交。

---

### Task 12: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 在 PLUGINS.md 目录中添加 MQTT Source**

在 Source 插件目录部分添加 MQTT Source 条目：

```markdown
- [MQTT Source](#mqtt-source)
```

- [ ] **Step 2: 添加 MQTT Source 完整章节**

在 Source 插件章节末尾（Mock Source 之后）添加：

```markdown
---

### MQTT Source

从 MQTT broker 订阅 topic，消费 JSON 格式消息，使用 Eclipse Paho 客户端。

#### 配置参数

| 参数          |  必填  | 默认值         | 说明                                         |
|-------------|:----:|-------------|--------------------------------------------|
| `broker`    |  是   | -           | MQTT broker 地址，如 `tcp://localhost:1883`    |
| `topic`     |  是   | -           | 订阅的 topic 名称                               |
| `clientId`  |  否   | 自动生成 UUID   | 客户端 ID，多任务建议手动指定避免冲突                       |
| `username`  |  否   | -           | 认证用户名                                      |
| `password`  |  否   | -           | 认证密码                                       |
| `startupMode` |  否  | `latest`    | 启动模式：`earliest`（接收 retained 消息）或 `latest`（新消息） |
| `schema`    |  是   | -           | 消息体字段定义                                    |

#### 配置示例

**基础配置：**

```json
{
  "source": {
    "type": "mqtt",
    "outputTable": "sensor_data",
    "config": {
      "broker": "tcp://localhost:1883",
      "topic": "sensor/temperature",
      "schema": {
        "deviceId": "STRING",
        "value": "DOUBLE",
        "timestamp": "TIMESTAMP"
      }
    }
  }
}
```

**带认证配置：**

```json
{
  "source": {
    "type": "mqtt",
    "outputTable": "mqtt_events",
    "config": {
      "broker": "tcp://broker.example.com:1883",
      "topic": "events/log",
      "clientId": "etl-consumer-001",
      "username": "admin",
      "password": "secret",
      "startupMode": "earliest",
      "schema": {
        "eventId": "STRING",
        "type": "STRING",
        "data": {
          "key": "STRING",
          "value": "DOUBLE"
        },
        "tags": ["STRING"],
        "timestamp": "TIMESTAMP"
      }
    }
  }
}
```

#### 运行模式

- 流式消费，持续运行（`mode: "streaming"`）
- QoS 1（至少一次送达）
- 支持 checkpoint 时保存分片状态
- 自动重连机制（连接断开后自动重新连接）

#### 启动模式说明

- `earliest`：订阅时会尝试接收 broker 保留的 last retained message（如果有）
- `latest`：只接收订阅后新发布的消息

#### 错误处理

- JSON 解析失败：记录 ERROR 日志，跳过该消息，继续消费
- 连接失败：抛出异常，触发 Flink 重试
- 认证失败：启动时抛出 IllegalArgumentException
```

- [ ] **Step 3: 验证文档完整性**

Run: `grep -c "MQTT Source" PLUGINS.md`
Expected: 至少 2 次出现（目录和章节标题）

- [ ] **Step 4: Commit**

```bash
git add PLUGINS.md
git commit -m "docs: 新增 MQTT Source 插件文档

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 13: 创建示例配置文件

**Files:**
- Create: `docs/examples/streaming-mqtt2console.json`

- [ ] **Step 1: 创建示例配置**

```json
{
  "job": {
    "name": "mqtt-to-console",
    "mode": "streaming",
    "parallelism": 1
  },
  "sources": [
    {
      "type": "mqtt",
      "outputTable": "mqtt_messages",
      "config": {
        "broker": "tcp://localhost:1883",
        "topic": "sensor/data",
        "clientId": "etl-mqtt-consumer",
        "schema": {
          "deviceId": "STRING",
          "temperature": "DOUBLE",
          "humidity": "DOUBLE",
          "timestamp": "TIMESTAMP"
        }
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "mqtt_messages"
    }
  ]
}
```

- [ ] **Step 2: Commit**

```bash
git add docs/examples/stream-mqtt2console.json
git commit -m "docs: 新增 MQTT Source 示例配置

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Self-Review Checklist

**1. Spec Coverage:**
- ✅ Task 1-3: Maven 模块、启动模式、配置封装 → 连接配置需求
- ✅ Task 4-5: 分片、状态、枚举器 → 抽象层架构
- ✅ Task 6-7: SplitReader、RecordEmitter → MQTT 消费和 JSON 转换
- ✅ Task 8-10: SourceReader、Source、Plugin → SPI 入口和主类
- ✅ Task 12: PLUGINS.md → 文档更新需求
- ✅ Task 13: 示例配置 → 示例文件

**2. Placeholder Scan:**
- ✅ 无 TBD/TODO
- ✅ 所有代码块完整
- ✅ 所有命令有预期输出

**3. Type Consistency:**
- ✅ `MqttMessageRecord` 在 Task 6 和 Task 7-9 中使用一致
- ✅ `MqttSplit` 作为分片类型在所有类中一致
- ✅ `MqttSplitState` 作为状态类型在 SourceReader 和 RecordEmitter 中一致
- ✅ `MqttEnumCheckpoint` 作为检查点类型一致

---

**Plan complete and saved to `docs/superpowers/plans/2026-04-24-mqtt-source-plan.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**