# MQTT Source 插件设计文档

## 概述

新增 MQTT Source 插件，使用 Eclipse Paho MQTT 客户端库订阅 MQTT topic，消费 JSON 格式消息并转换为 Flink Row 数据。

## 技术选型

- **MQTT 客户端**：Eclipse Paho `org.eclipse.paho.client.mqttv3` 1.2.5
- **QoS 级别**：QoS 1（至少一次送达）
- **架构**：使用项目抽象层（AbstractSplitSource + BaseSplitEnumerator + BaseSourceReader）

## 需求规格

### 功能需求

| 需求项 | 规格 |
|--------|------|
| Topic 配置 | 单 topic |
| 消息格式 | JSON（必须） |
| Schema 支持 | 所有 PLUGINS.md 定义的类型 |
| 启动模式 | `earliest` / `latest` 可配置 |
| 运行模式 | 流式（streaming） |
| QoS | QoS 1（固定） |

### 连接配置

| 参数 | 必填 | 默认值 | 说明 |
|------|:--:|--------|------|
| `broker` | 是 | - | MQTT broker 地址，如 `tcp://localhost:1883` |
| `topic` | 是 | - | 订阅的 topic 名称 |
| `clientId` | 否 | 自动生成 UUID | 客户端 ID，多任务建议手动指定避免冲突 |
| `username` | 否 | - | 认证用户名 |
| `password` | 否 | - | 认证密码 |
| `startupMode` | 否 | `latest` | `earliest`（保留消息）或 `latest`（新消息） |
| `schema` | 是 | - | 消息体字段定义 |

## 模块结构

```
flink-etl-connector/connector-mqtt/
├── pom.xml
└── src/main/java/com/etl/connector/mqtt/source/
    ├── MqttSourcePlugin.java        # SPI 入口，@AutoService(SourcePlugin.class)
    ├── MqttSource.java              # AbstractSplitSource 实现
    ├── MqttSourceConfig.java        # 配置封装类（Serializable）
    ├── MqttSplit.java               # 分片定义，继承 BaseSourceSplit
    ├── MqttSplitEnumerator.java     # 分片枚举器，继承 BaseSplitEnumerator
    ├── MqttSourceReader.java        # Source Reader，继承 BaseSourceReader
    ├── MqttSplitReader.java         # MQTT 消费逻辑，继承 BaseSplitReader
    ├── MqttRecordEmitter.java       # JSON → Row 转换，实现 RecordEmitter
    ├── MqttSplitState.java          # 分片状态，继承 BaseSplitState
    └── MqttEnumCheckpoint.java      # 检查点，继承 BaseEnumCheckpoint
```

## 类设计

### MqttSourcePlugin

SPI 入口，实现 `SourcePlugin` 接口。

```java
@AutoService(SourcePlugin.class)
public class MqttSourcePlugin implements SourcePlugin {
    @Override
    public String identifier() {
        return "mqtt";
    }

    @Override
    public Source<Row, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        return new MqttSource(config);
    }
}
```

### MqttSource

继承 `AbstractSplitSource<MqttSplit, MqttEnumCheckpoint>`，负责：
- 解析配置并构建 `MqttSourceConfig`
- 创建 Enumerator 和 Reader
- 提供 `Boundedness.CONTINUOUS_UNBOUNDED`
- 提供 TypeInformation（基于 Schema）

### MqttSourceConfig

配置封装类，包含所有参数：

```java
@Getter
@Builder
public class MqttSourceConfig implements Serializable {
    private final String broker;
    private final String topic;
    private final String clientId;
    private final String username;
    private final String password;
    private final StartupMode startupMode;  // EARLIEST / LATEST
    private final EtlSchema schema;
}
```

### MqttSplitEnumerator

继承 `BaseSplitEnumerator`，单分片模式：
- `start()` 创建包含 topic 和配置的分片
- `snapshotState()` 保存待处理分片状态

### MqttSplitReader

继承 `BaseSplitReader<MqttMessage, MqttSplit>`，核心消费逻辑：
- 创建 Paho `MqttClient` 并连接 broker
- 根据 `startupMode` 决定是否接收 retained messages
- 使用阻塞队列接收消息
- `fetch()` 方法从队列取出消息放入 `ElementsQueue`

关键实现点：
- 连接时设置 `MqttConnectOptions.setCleanSession(false)` 以持久化订阅
- `earliest` 模式：订阅后接收 broker 保留的 last retained message
- `latest` 模式：只接收订阅后发布的新消息

### MqttRecordEmitter

实现 `RecordEmitter<MqttMessage, Row, MqttSplitState>`：
- 解析 JSON 消息体
- 使用 `JsonToRowConverter` 转换为 Row
- 处理解析异常（记录日志并跳过）

### MqttSplit

继承 `BaseSourceSplit`：
- 包含 `topic` 和完整的 `MqttSourceConfig`

### MqttSplitState / MqttEnumCheckpoint

状态追踪和检查点，继承对应基类。

## 数据流程

```
MQTT Broker
    ↓ (Paho MqttClient.subscribe, QoS 1)
MqttSplitReader (消息放入阻塞队列)
    ↓ (fetch() 取出 MqttMessage)
ElementsQueue (Flink 内部队列)
    ↓
MqttRecordEmitter (JSON → Row，使用 JsonToRowConverter)
    ↓
BaseSourceReader (输出到下游)
    ↓
DataStream<Row> → Table API
```

## 配置示例

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

**完整配置（带认证和启动模式）：**

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

## 错误处理

| 异常场景 | 处理方式 |
|----------|----------|
| JSON 解析失败 | 记录 ERROR 日志，跳过该消息，继续消费 |
| 连接失败 | 抛出 IOException，触发 Flink 重试 |
| 认证失败 | 启动时抛出 IllegalArgumentException |
| broker 参数缺失 | 启动时抛出 IllegalArgumentException |
| topic 参数缺失 | 启动时抛出 IllegalArgumentException |
| schema 参数缺失 | 启动时抛出 IllegalArgumentException |

## 测试策略

- 使用 HiveMQ 或 Mosquitto 作为测试 broker
- Mock 测试：配置解析、JSON 转换逻辑
- 集成测试：完整消费流程（需 broker 环境）

## 文档更新

完成后需更新 `PLUGINS.md`，添加 MQTT Source 章节。

## 依赖添加

**flink-etl-connector/connector-mqtt/pom.xml：**

```xml
<dependency>
    <groupId>org.eclipse.paho</groupId>
    <artifactId>org.eclipse.paho.client.mqttv3</artifactId>
    <version>1.2.5</version>
</dependency>
```

**flink-etl-client/pom.xml：**

```xml
<dependency>
    <groupId>com.etl</groupId>
    <artifactId>connector-mqtt</artifactId>
    <version>${project.version}</version>
</dependency>
```