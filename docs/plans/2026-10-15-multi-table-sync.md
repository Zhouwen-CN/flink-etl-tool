# 多表同步（multiTable）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kafka 订阅 N 个 topic（CDC 数据）写入 Doris 的多表同步能力。一个作业、一个 source 流、一个 sink writer
完成整库同步，整合写入、减少 Doris 写并发与后台 compaction 压力。v1 覆盖 Doris sink；后续 JDBC sink 复用同一套路由/攒批骨架。

**Architecture:** envelope（信封）模式——业务数据不拆成 Row 字段，而是作为 payload（json string）放在 `_metadata` 中。source
反序列化时只提取路由键（tableId）+ RowKind，payload 原样透传；sink 端按 `tableMapping` 映射表（源表 -> 目标表）查表得到目标表，按表分
buffer 攒批、独立 Stream Load。多表流同样注册为 Flink Table（视图只有 `_metadata` 一个字段），SQL pipeline 结构完全统一，
`JobBuilder` 零改动。

**Tech Stack:** Java 1.8、Flink 1.15.2、flink-doris-connector-1.15:1.4.0、Jackson、Lombok、JUnit 5。

---

## 设计决策（讨论结论，不再重复论证）

1. **envelope 而非 union schema**：1000 表 × 100 字段的 union schema 不可扩展且同名冲突。管道类型与表数量解耦，schema 从"
   必填配置"降级为"不存在"。
2. **payload 放 `_metadata`**：复用现有 `MetadataManager`
   机制（[MetadataManager.java](file:///d:/work/idea/flink-etl-tool/flink-etl-core/src/main/java/com/etl/core/schema/metadata/MetadataManager.java)
   ），单表模式 payload 恒为 null，行为不变。
3. **`tableMapping` 配在 sink，不放 source**：一个多表流可能被多个 sink 消费，各自目标映射不同。source 的 `_metadata.source`
   只带源 tableId（`CdcJsonUtil` 已解析出 `db.schema.table`）。
4. **模式判定用配置字段，不用行内标志**：Sink 实例构建期定型。`table`（单表）与 `tableMapping`（多表）二选一互斥，由字段本身表达模式，不加布尔开关。
5. **多表流也转 Flink Table**：视图仅 `_metadata` 一个字段，用户可通过 SQL（字符串函数/后续 JSON UDF）修改 payload。changelog
   语义保留 RowKind，sink 端照常判断 DELETE。`JobBuilder` 无分支。
6. **未匹配映射直接丢弃 + 计数**：丢弃不影响 offset 推进（KafkaSource checkpoint 按 partition 存
   offset，与消息处理结果无关）。丢弃期间的历史增量靠独立回填任务补（新表 topic + earliest + 新映射，Doris unique key
   幂等），主任务不动。存量数据本来就不在 CDC 流里，走全量快照导入。
7. **自研 MultiTableWriter，不用官方 multiple sink**：1.4.0 的 `DorisRecordSerializer.serialize()` 只返回 `byte[]`（表固定在
   `DorisOptions.tableIdentifier`），官方 per-record 表路由 API（`DorisRecord.of(db, table, row)`）是 1.5.x 才有的。v1 不升级
   connector 版本。

## envelope 数据结构

```
Row (envelope)
 ├─ _metadata (Row)
 │    ├─ topic    : STRING   ← 已有
 │    ├─ source   : STRING   ← 已有, 即源表 tableId "db.schema.table"
 │    └─ payload  : STRING   ← 新增, 业务数据 json 原文; 单表模式恒为 null
 └─ RowKind                ← CDC 语义: INSERT / UPDATE_AFTER / DELETE
```

数据流：

```
Kafka topics (CDC json)
   │ CdcJsonDeserializationSchema (multiTable=true)
   │   解析 op -> RowKind; 解析 source -> tableId; data 节点原文 -> payload
   ▼
DataStream<Row> (envelope, 类型固定: ROW_NAMED(["_metadata"], ROW_NAMED([topic,source,payload])))
   │
   │ fromChangelogStream -> Table(仅 _metadata 一列) -> [可选 SQL transform] -> toChangelogStream
   ▼
MultiTableDorisWriter (SinkV2)
   ├─ write(row): extractMetadata -> tableMapping 解析 -> 未命中丢弃+计数
   │             payload -> 塞 __DORIS_DELETE_SIGN__(kind=DELETE 时) -> 按目标表分 buffer
   ├─ prepareCommit: 各表独立 Stream Load (label = prefix + table + ckId)
   └─ 全局并发槽位 maxConcurrentLoads, 控制同时 load 的表数量
```

## 配置示例

```json
{
  "sources": [
    {
      "type": "kafka",
      "outputTable": "cdc_source",
      "multiTable": true,
      "properties": {
        "bootstrap.servers": "...",
        "group.id": "..."
      },
      "topicPattern": "^cdc\\.mydb\\..*$",
      "format": {
        "type": "cdc-json"
      }
    }
  ],
  "sinks": [
    {
      "type": "doris",
      "inputTable": "cdc_source",
      "tableMapping": {
        "mydb.t_user": "ods_mydb.t_user",
        "mydb.t_order": "ods_mydb.t_order",
        "otherdb.t_order": "ods_order.all_order"
      },
      "fenodes": "...",
      "username": "...",
      "password": "...",
      "labelPrefix": "etl_multi",
      "maxConcurrentLoads": 10
    }
  ]
}
```

单表模式配置不变（`table` 字段）。`multiTable=true` 的 source 不配置 `schema`。

## 配置校验规则（ConfigParser.validate）

| 规则                                                  | 行为                             |
|-----------------------------------------------------|--------------------------------|
| source `multiTable=true` 且配置了 `schema`              | 报错                             |
| sink `table` 与 `tableMapping` 同时配置                  | 报错（互斥）                         |
| sink 配置了 `tableMapping` 但 `type != doris`           | 报错（v1 仅 doris 支持多表，显式拒绝优于静默忽略） |
| 上游 source `multiTable=true` 但 sink 无 `tableMapping` | 报错                             |
| 上游 source 非 multiTable 但 sink 配了 `tableMapping`     | 报错                             |

transform 引用多表流无需校验：多表 source 虽注册视图，SQL 引用 `_metadata` 之外的列会自然失败，属用户 SQL 错误。

---

## 已核对的 API（来自现有代码）

```java
// 1.4.0: org.apache.doris.flink.sink.writer.DorisRecordSerializer<Row>
byte[] serialize(Row row) throws IOException;   // 只返回字节, 无表路由能力 (v1 RowToJsonSerializer 已确认)

// MetadataManager (现有, flink-etl-core)
MetadataManager.

addMetadata(EtlSchema) /

addMetadata(RowTypeInfo)
MetadataManager.

extractMetadata(Row) ->Pair<Row, Metadata>   // 剥 _metadata
Metadata.

builder().

topic(..).

source(..).

build()

// CdcJsonUtil (现有): parseOggOp/parseCustomOggOp/parseDebeziumOp -> RowKind
//                    getOggSource/getCustomOggSource/getDebeziumSource -> tableId
```

## 实现前必须核对（反编译 flink-doris-connector-1.15-1.4.0.jar）

- `org.apache.doris.flink.sink.writer.DorisStreamLoad` 构造参数与 `write/load/close` 方法签名（Task 4 按表创建实例使用）
- `DorisWriter` 的 buffer/flush/checkpoint 状态管理方式（作为 MultiTableDorisWriter 的参考实现）
- `LoadConstants.DORIS_DELETE_SIGN` 常量名
- 若核对发现 1.4.0 存在可直接复用的多表 writer，优先复用并简化 Task 4

## 文件结构

```
flink-etl-core/
└── src/main/java/com/etl/core/schema/metadata/
    ├── Metadata.java                       # 修改: 加 payload 字段
    └── MetadataManager.java                # 修改: nested 类型加 payload; extractMetadata 读 payload; 新增 envelopeSchema()

flink-etl-connector/connector-kafka/
└── src/main/java/com/etl/connector/kafka/source/
    ├── config/KafkaSourceConfig.java       # 修改: 加 multiTable (默认 false)
    └── format/cdc/CdcJsonDeserializationSchema.java  # 修改: multiTable 分支产出 envelope Row

flink-etl-connector/connector-doris/
└── src/main/java/com/etl/connector/doris/sink/
    ├── config/DorisSinkConfig.java         # 修改: 加 tableMapping(简单映射)/maxConcurrentLoads
    ├── MultiTableDorisSink.java            # 新建: SinkV2 Sink, 创建 writer
    ├── MultiTableDorisWriter.java          # 新建: SinkWriter, 按表分 buffer + Stream Load
    ├── MultiTableRowSerializer.java        # 新建: payload -> 塞 delete sign -> byte[]
    └── v1/DorisSinkPlugin.java             # 修改: tableMapping 非空走 MultiTableDorisSink

flink-etl-client/
└── src/main/java/com/etl/client/parser/ConfigParser.java  # 修改: 校验规则
```

其余改动：

- `docs/examples/stream-kafka-multi-table-2-doris.json`：多表示例配置
- `PLUGINS.md`：multiTable 说明

---

### Task 1: core — Metadata 加 payload + envelopeSchema

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/metadata/Metadata.java`
- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/metadata/MetadataManager.java`

- [ ] **Step 1: Metadata 加 payload 字段**

`@Builder` 加 `private String payload;`（默认 null）。单表模式全链路不受影响。

- [ ] **Step 2: MetadataManager nested 类型加 payload**

`addMetadata(String[], TypeInformation[])` 中：

```java
newNames[length]=METADATA_FIELD;
newTypes[length]=Types.

ROW_NAMED(
    new String[] {
    TOPIC_FIELD, SOURCE_FIELD, "payload"
},
Types.STRING,Types.STRING,Types.STRING);
```

- [ ] **Step 3: extractMetadata 读取 payload**

`extractMetadata(Row)` 的 builder 链加 `.payload(metadataMap.get("payload"))`。

- [ ] **Step 4: 新增 envelopeSchema()**

```java
/** 多表模式 envelope schema: 唯一字段 _metadata(topic, source, payload) */
public static EtlSchema envelopeSchema() {
    return new EtlSchema(
            new String[]{METADATA_FIELD},
            new TypeInformation<?>[]{Types.ROW_NAMED(
                    new String[]{TOPIC_FIELD, SOURCE_FIELD, "payload"},
                    Types.STRING, Types.STRING, Types.STRING)});
}
```

- [ ] **Step 5: 单测**

`MetadataManager` 相关测试：envelopeSchema 字段结构正确；extractMetadata 能取回 payload；payload=null（单表）行为与现状一致。跑
`mvn -pl flink-etl-core test`。

---

### Task 2: kafka source — multiTable 配置与 envelope 反序列化

**Files:**

- Modify:
  `flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/source/config/KafkaSourceConfig.java`
- Modify:
  `flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/source/format/cdc/CdcJsonDeserializationSchema.java`

- [ ] **Step 1: KafkaSourceConfig 加 multiTable**

`private boolean multiTable = false;`，从 `config.get("multiTable", Boolean.class)` 读取。

- [ ] **Step 2: CdcJsonDeserializationSchema 支持双模式**

构造函数加 multiTable 参数（`KafkaFormatPlugin.createDeserializer` 调用链透传）。

multiTable=false：现有逻辑不变（`JsonToRowConverter.convertJsonToRow`，metadata.payload 不设置）。

multiTable=true：

1. 解析 op -> RowKind，解析失败 WARN + 跳过（同现状）
2. 解析 source -> tableId
3. `dataNode` -> `dataNode.toString()` 作为 payload
4. 产出 Row：`Row.withNames()` 只放 `_metadata`（nested Row：topic/source/payload），`row.setKind(rowKind)`
5. `getProducedType()` 返回 `ROW_NAMED(["_metadata"], ROW_NAMED([topic,source,payload]))`

- [ ] **Step 3: 校验拦截**

multiTable=true 且配置了 schema 时抛 `SourceConfigException`（插件层防御，与 ConfigParser 校验重复但独立生效）。

- [ ] **Step 4: 单测**

构造 ogg / debezium 两种消息样例断言：envelope Row 的 `_metadata` 三字段正确、RowKind 正确；payload 为 data 节点原文 json。跑
`mvn -pl flink-etl-connector/connector-kafka test`。

---

### Task 3: doris sink — tableMapping 配置

**Files:**

- Modify: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/config/DorisSinkConfig.java`

- [ ] **Step 1: DorisSinkConfig 加字段**

- `tableMapping`: `Map<String, String>`，key = 源表 tableId（`db.table` 或 `db.schema.table`），value = Doris 目标表（
  `db.table`）。精确匹配，`get(tableId)` 未命中返回 null（数据丢弃）
- `maxConcurrentLoads`: `int`，默认 10

`fromSinkConfig` 直接 `config.get("tableMapping", Map.class)` 反序列化，空 Map 视为未配置（null）。

- [ ] **Step 2: 单测**

配置解析（Map 结构正确）、table 与 tableMapping 互斥解析正常、空 Map 归一为 null。跑
`mvn -pl flink-etl-connector/connector-doris test`。

---

### Task 4: doris sink — MultiTableDorisWriter

**Files:**

- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/MultiTableDorisSink.java`
- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/MultiTableDorisWriter.java`
- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/MultiTableRowSerializer.java`
- Modify: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/v1/DorisSinkPlugin.java`

**前置:** 完成上文"实现前必须核对"的 API 反编译。

- [ ] **Step 1: MultiTableRowSerializer**

输入 envelope Row：`MetadataManager.extractMetadata` 取 payload -> Jackson `readTree` -> `kind=DELETE` 时
`put(LoadConstants.DORIS_DELETE_SIGN, 1)` -> `writeValueAsString` -> UTF-8 bytes。payload 为 null（不该出现）抛
IllegalStateException。

- [ ] **Step 2: MultiTableDorisWriter（SinkWriter<Row>）**

内部结构：

```java
Map<String /*target table*/, TableBuffer> buffers;   // TableBuffer: 攒批字节 + DorisStreamLoad 实例(懒创建)
Map<String /*source tableId*/, String /*target*/> tableMapping;
```

- `write(Row)`: extractMetadata -> `tableMapping.get(source)` -> null 则丢弃（counter + 采样 WARN，见 Step 4）；命中则序列化入
  buffer，达到单表行数阈值（复用 batchSize 语义）时触发该表 flush
- `prepareCommit(boolean flush)`: 所有非空 buffer 逐表 Stream Load；并发用固定大小线程池（`maxConcurrentLoads`），
  `invokeAll` 等待全部完成。任一表 load 失败抛异常使 checkpoint 失败（at-least-once）
- `snapshotState`: 返回空列表（prepareCommit 已清空 buffer）
- Stream Load 参数：`format=json`、`read_json_by_line=true`，label = `labelPrefix + "_" + tablehash + "_" + checkpointId`
  ，DorisOptions 按 `tableIdentifier=target` 构造
- `close`: 关闭全部 stream load 实例与线程池

**实现注意：** 官方 `DorisStreamLoad` 若不便按表实例化（内部状态耦合），可参考 `DorisWriter` 的 SinkBuffer 逻辑，用 Apache
HttpClient 自实现极简 stream load（PUT + 2 个 header：label、format），HTTP 语义简单可控。

- [ ] **Step 3: MultiTableDorisSink（SinkV2）**

实现 `org.apache.flink.api.connector.sink2.Sink<Row>`，`createWriter(InitContext)` 返回 `MultiTableDorisWriter`。
`DorisSinkPlugin.createSink` 分支：`cfg.getTableMapping() != null` -> `SinkV1Adapter.wrap(new MultiTableDorisSink(...))`
（与现有 v1 单表路径对称）。

- [ ] **Step 4: 丢弃可观测性**

`InitContext.metricGroup().counter("multiTableDropCount")`；丢弃时 counter++，并按 1/1000 采样打 WARN（含
topic/tableId）。连续丢弃（如 10000 条）打一条 ERROR。

- [ ] **Step 5: 单测**

tableMapping 未命中丢弃不写 buffer；payload 序列化含 delete sign（DELETE 与非 DELETE）；buffer 达阈值触发 flush 回调（mock
DorisStreamLoad）。跑 `mvn -pl flink-etl-connector/connector-doris test`。

---

### Task 5: client — ConfigParser 校验

**Files:**

- Modify: `flink-etl-client/src/main/java/com/etl/client/parser/ConfigParser.java`

- [ ] **Step 1: sources 循环**

- `sourceByTable` 改为 `Map<String, SourceConfig>`（现有 Set 逻辑并入）
- `multiTable=true` 且 `get("schema", Map.class) != null` -> 报错

- [ ] **Step 2: sinks 循环**

- `hasTableMapping = sink.get("tableMapping", Map.class) != null`
- `hasTable = sink.get("table", String.class) != null`
- 两者同时 -> 报错
- `hasTableMapping && !"doris".equals(sink.getType())` -> 报错
- 上游 source（`sourceByTable.get(sink.getInputTable())`）：
    - `source.multiTable=true` 且 `!hasTableMapping` -> 报错
    - `source.multiTable!=true` 且 `hasTableMapping` -> 报错

- [ ] **Step 3: 单测**

六条校验规则各一个用例（BaseConfigTest 风格）。跑 `mvn -pl flink-etl-client test`。

---

### Task 6: 端到端验证

**Files:**

- Create: `docs/examples/stream-kafka-multi-table-2-doris.json`
- Modify: `PLUGINS.md`

- [ ] **Step 1: 示例配置**

按"配置示例"章节写 json，`docs/examples/` 现有命名风格。

- [ ] **Step 2: SQL 修改 payload 的 changelog 往返验证**

本地 mini cluster（参考现有测试的 StreamExecutionEnvironment 用法）：

1. envelope source -> `fromChangelogStream` -> SQL `SELECT _metadata FROM t WHERE ...`（含 payload 字符串函数改写）->
   `toChangelogStream` -> console sink
2. 断言 RowKind 在往返后保留、`_metadata` 结构完整

- [ ] **Step 3: 全量构建**

`mvn clean test` 全绿。

- [ ] **Step 4: 手工验证清单（需真实环境）**

- Kafka 两个 topic 各发 ogg/debezium 格式消息 -> Doris 两张预建表数据落对
- 故意发一条不在 tableMapping 内的消息 -> 丢弃 + metric 计数 + 主任务 offset 正常推进
- kill -9 后从 checkpoint 恢复 -> 无数据丢失（unique key 幂等容忍重复）

---

## 后续扩展（本期不做）

1. **JDBC sink 多表写入**：复用 `tableMapping` 映射，按 target 缓存 PreparedStatement 模板（首见表时从 payload 字段集生成
   upsert SQL）
2. **正则/批量路由规则**：tableMapping 逐条配置在大规模场景偏冗长，将来可扩展 pattern -> target 规则层（配置结构向后兼容）
3. **JSON UDF**：给 SQL 修改 payload 提供结构化手段（JSON_SET/REMOVE 语义），替代字符串函数
4. **动态路由下发**：广播流推送路由表，免重启更新 tableMapping（当前语义：改配置 = savepoint 重启生效）
5. **新表自动建表**：从 payload 推断列类型建 Doris 表（依赖 schema 推断，复杂度高）
6. **评估升级 flink-doris-connector 1.5.x**：官方 `DorisRecord.of(db, table, row)` + multiple writer 可替换自研 Task 4
