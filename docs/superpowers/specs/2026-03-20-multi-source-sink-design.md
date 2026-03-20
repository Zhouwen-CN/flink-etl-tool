# 多 Source 和多 Sink 支持设计

## 概述

将 ETL 框架从单 source、单 sink 扩展为支持多个 source 和多个 sink，通过 `outputTable` / `inputTable` 机制灵活关联数据流。

## 配置格式变更

### JobConfig 结构

```java
@Data
public class JobConfig {
    private JobMeta job;
    private List<SourceConfig> sources;      // source 改为 sources 数组
    private List<TransformConfig> transforms;
    private List<SinkConfig> sinks;          // sink 改为 sinks 数组
}
```

### 示例配置

**完整示例（含 Join 和多个 Sink）：**

```json
{
  "job": {
    "name": "multi-source-demo",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "mysql",
      "outputTable": "users",
      "config": {
        "url": "jdbc:mysql://localhost:3306/db",
        "table": "users",
        "username": "root",
        "password": "xxx",
        "splitColumn": "id"
      }
    },
    {
      "type": "mysql",
      "outputTable": "orders",
      "config": {
        "url": "jdbc:mysql://localhost:3306/db",
        "table": "orders",
        "username": "root",
        "password": "xxx",
        "splitColumn": "id"
      }
    }
  ],
  "transforms": [
    {
      "type": "sql",
      "outputTable": "user_orders",
      "config": {
        "sql": "SELECT u.id, u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100"
      }
    },
    {
      "type": "sql",
      "outputTable": "high_value_users",
      "config": {
        "sql": "SELECT id, name, SUM(amount) as total FROM user_orders GROUP BY id, name"
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "user_orders",
      "config": {}
    },
    {
      "type": "mysql",
      "inputTable": "high_value_users",
      "config": {
        "url": "jdbc:mysql://localhost:3306/target_db",
        "table": "high_value_users",
        "username": "root",
        "password": "xxx"
      }
    }
  ]
}
```

**最简示例（无 Transform，Source 直连 Sink）：**

```json
{
  "job": {
    "name": "simple-multi-sink",
    "mode": "batch"
  },
  "sources": [
    {
      "type": "mysql",
      "outputTable": "users",
      "config": {
        "url": "jdbc:mysql://localhost:3306/db",
        "table": "users",
        "username": "root",
        "password": "xxx",
        "splitColumn": "id"
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "users",
      "config": {}
    }
  ]
}
```

## 执行流程

```
┌─────────────────────────────────────────────────────────────────┐
│                         Flink Job                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌──────────┐                                   │
│  │ Source 1 │───►│ Table    │───┐                               │
│  │ (users)  │    │ "users"  │   │                               │
│  └──────────┘    └──────────┘   │                               │
│                                 ▼                               │
│  ┌──────────┐    ┌──────────┐  ┌─────────────┐  ┌────────────┐  │
│  │ Source 2 │───►│ Table    │─►│ Transform 1 │─►│ Table      │  │
│  │ (orders) │    │ "orders" │  │ (SQL Join)  │  │"user_orders│  │
│  └──────────┘    └──────────┘  └─────────────┘  └────────────┘  │
│                                                      │          │
│                                        ┌─────────────┼───────┐  │
│                                        ▼             ▼       │  │
│                               ┌────────────┐  ┌───────────┐   │  │
│                               │ Transform 2│  │   Sink 1  │   │  │
│                               │ (SQL Group)│  │ (console) │   │  │
│                               └────────────┘  └───────────┘   │  │
│                                      │                        │  │
│                                      ▼                        │  │
│                               ┌────────────┐  ┌───────────┐   │  │
│                               │   Table    │─►│  Sink 2   │   │  │
│                               │"high_value"│  │  (mysql)  │   │  │
│                               └────────────┘  └───────────┘   │  │
│                                                              │  │
└─────────────────────────────────────────────────────────────────┘
```

### 执行步骤

1. **Source 阶段**：遍历 `sources` 数组，每个 source：
   - 创建 DataStream<Row>
   - 注册为 Table（表名为 outputTable）

2. **Transform 阶段**：按配置顺序遍历 `transforms` 数组：
   - 执行 SQL（引用已注册的表名）
   - 结果注册为中间表

3. **Sink 阶段**：遍历 `sinks` 数组，每个 sink：
   - 从 inputTable 获取 Table
   - Table → DataStream<Row>
   - 写入 Sink

## Transform inputTable 规则

- **SQL Transform**：
  - 可省略 `inputTable`，因为 SQL 语句中已明确引用表名
  - 如果用户提供了 `inputTable`，则忽略（不报错，保持向后兼容）
- **其他 Transform 类型**：保留 `inputTable` 字段（预留扩展）

## 代码变更

### 1. JobConfig.java

```java
@Data
public class JobConfig {
    private JobMeta job;
    private List<SourceConfig> sources;      // List 替代单对象
    private List<TransformConfig> transforms;
    private List<SinkConfig> sinks;          // List 替代单对象
}
```

### 2. ConfigParser.java

更新校验逻辑：
- 校验 `sources` 数组非空
- 校验每个 source 的 `type` 和 `outputTable`
- 校验 `sources` 中 `outputTable` 名称唯一（不允许重复）
- 校验 `sinks` 数组非空
- 校验每个 sink 的 `type` 和 `inputTable`
- 校验 `transforms` 中 `outputTable` 名称唯一（不允许与 source 或其他 transform 重复）

### 3. JobBuilder.java

```java
public static void build(StreamExecutionEnvironment env, JobConfig config) {
    StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

    // 1. 遍历所有 Source
    for (SourceConfig sourceConfig : config.getSources()) {
        SourcePlugin plugin = PluginLoader.loadSourcePlugin(sourceConfig.getType());
        Source source = plugin.createSource(sourceConfig);
        DataStream<Row> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceConfig.getType());
        stEnv.createTemporaryView(sourceConfig.getOutputTable(), stream);
    }

    // 2. 按顺序执行 Transform
    if (config.getTransforms() != null) {
        for (TransformConfig transformConfig : config.getTransforms()) {
            TransformPlugin plugin = PluginLoader.loadTransformPlugin(transformConfig.getType());
            Table result = plugin.transform(transformConfig, stEnv);
            stEnv.createTemporaryView(transformConfig.getOutputTable(), result);
        }
    }

    // 3. 遍历所有 Sink
    for (SinkConfig sinkConfig : config.getSinks()) {
        Table table = stEnv.from(sinkConfig.getInputTable());
        DataStream<Row> stream = stEnv.toDataStream(table);
        SinkPlugin plugin = PluginLoader.loadSinkPlugin(sinkConfig.getType());
        stream.addSink(plugin.createSink(sinkConfig));
    }
}
```

### 4. 示例配置更新

更新 `docs/examples/` 目录下的所有 JSON 配置文件，采用新的数组格式。

## 不兼容变更

此设计不兼容旧版配置格式：
- `source` 字段改为 `sources`
- `sink` 字段改为 `sinks`

用户需更新现有配置文件。