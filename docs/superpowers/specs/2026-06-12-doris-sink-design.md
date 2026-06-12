# Doris Sink 插件设计

日期：2026-06-12
状态：已确认，待实现

## 1. 目标

新增 Doris Sink 连接器，基于 `flink-doris-connector-1.15`（版本 1.5.2），通过 Stream Load 将 `Row` 数据写入 Apache Doris。采用自定义序列化器方式支持 `Row` 类型写入，序列化格式通过 SPI 机制可扩展（本期仅实现 JSON）。

## 2. 关键决策

| 决策项 | 选择 | 理由 |
|--------|------|------|
| 集成模式 | 直接返回官方 `DorisSink`，不继承 `AbstractSink` | 复用 connector 自带 Stream Load 批量/缓冲/checkpoint，避免重复造轮子。与 `KafkaSinkPlugin` 同模式 |
| 写入格式 | JSON | 复用现有 `RowToJsonConverter`，字段映射清晰，null/嵌套处理完善 |
| 写入语义 | at-least-once（`disable2PC`） | 与项目其他 Sink 一致（CLAUDE.md 规定 Sink 统一 at-least-once） |
| 配置策略 | 核心必填 + 少量可选 | 必填 4 项 + 可选 `labelPrefix`/`batchSize`/`format`，其余 Stream Load 属性写死 |
| Format 扩展 | SPI（`ServiceLoader` + `@AutoService`） | 镜像 Kafka source 的 `KafkaFormatPlugin` 模式，后续加 `debezium-json` 零改动主流程 |

## 3. connector 版本与序列化器接口

`flink-doris-connector.version = 1.5.2`（已配置于根 `pom.xml`）。

1.5.x 序列化器接口：

```java
public interface DorisRecordSerializer<T> extends Serializable {
    DorisRecord serialize(T record) throws IOException;
}
```

`DorisRecord` 携带 db / table / 数据字节。构造方式 `DorisRecord.of(database, table, byte[])`。

## 4. 模块结构

新增模块 `flink-etl-connector/connector-doris/`：

```
connector-doris/
├── pom.xml
└── src/main/java/com/etl/connector/doris/sink/
    ├── DorisSinkPlugin.java               # @AutoService(SinkPlugin.class), identifier="doris"
    ├── config/
    │   └── DorisSinkConfig.java           # @Builder, Serializable, fromSinkConfig() 校验
    └── format/
        ├── DorisFormatPlugin.java         # SPI 接口
        ├── DorisFormatLoader.java         # ServiceLoader 加载 + 缓存
        ├── JsonFormatPlugin.java          # @AutoService, identifier="json"
        └── RowToDorisJsonSerializer.java  # DorisRecordSerializer<Row>，Row→JSON bytes
```

`pom.xml` 依赖：`flink-etl-core`、`flink-doris-connector-1.15`（provided，继承根 pom 版本）、`google-auto-service`、`lombok`。

## 5. 组件设计

### 5.1 DorisSinkConfig

`@Getter @Builder`，`implements Serializable`。

字段：

| 字段 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `fenodes` | 是 | — | Doris FE 节点 `host:port` |
| `tableIdentifier` | 是 | — | `db.table` |
| `username` | 是 | — | 用户名 |
| `password` | 是 | — | 密码 |
| `labelPrefix` | 否 | null | Stream Load label 前缀，不填则用 connector 默认 |
| `batchSize` | 否 | null | 批量缓冲条数，不填则用 connector 默认 |
| `format` | 否 | `"json"` | 序列化格式，SPI 加载 |

`static DorisSinkConfig fromSinkConfig(SinkConfig config)`：
- 校验 `fenodes`/`tableIdentifier`/`username`/`password` 非空（参考 `KafkaSinkConfig` 校验风格）。
- 校验 `tableIdentifier` 含 `.`，可拆为 db/table。
- `format` 缺省取 `"json"`。

### 5.2 DorisFormatPlugin（SPI 接口）

```java
public interface DorisFormatPlugin extends Serializable {
    /** Format 标识符，如 "json" */
    String identifier();

    /** 创建序列化器 */
    DorisRecordSerializer<Row> createSerializer(DorisSinkConfig config);

    /** 该 format 对应的 Stream Load 属性（如 json -> format=json, read_json_by_line=true） */
    Properties streamLoadProperties();
}
```

`streamLoadProperties()` 让 format 插件自带其 Stream Load 配置，避免主流程写死格式属性。

### 5.3 DorisFormatLoader

镜像 `KafkaFormatLoader`：静态 `ServiceLoader` 加载全部插件并按 `identifier()` 缓存到 `Map`。

- `static DorisFormatPlugin getFormatPlugin(String format)` — 未找到返回 null。
- `static List<String> supportedFormats()`。

### 5.4 JsonFormatPlugin

`@AutoService(DorisFormatPlugin.class)`，`identifier() = "json"`。

- `createSerializer(config)` → `new RowToDorisJsonSerializer(config)`。
- `streamLoadProperties()` → `format=json`、`read_json_by_line=true`。

### 5.5 RowToDorisJsonSerializer

`implements DorisRecordSerializer<Row>`，`Serializable`。

构造时从 `config.getTableIdentifier()` 拆出 `database`、`table` 缓存。

`serialize(Row row)`：
1. `JsonNode node = RowToJsonConverter.convertRowToJsonNode(row)`
2. `String json = JsonUtils.writeValueAsString(node)`
3. `byte[] bytes = json.getBytes(StandardCharsets.UTF_8)`
4. `return DorisRecord.of(database, table, bytes)`

### 5.6 DorisSinkPlugin

`@AutoService(SinkPlugin.class)`，`identifier() = "doris"`。

`createSink(SinkConfig config)`：
1. `DorisSinkConfig cfg = DorisSinkConfig.fromSinkConfig(config)`
2. SPI 取 format 插件：
   ```java
   DorisFormatPlugin fmt = DorisFormatLoader.getFormatPlugin(cfg.getFormat());
   if (fmt == null) throw new IllegalArgumentException(
       "不支持的 format: " + cfg.getFormat() + "，支持: " + DorisFormatLoader.supportedFormats());
   ```
3. 构建 `DorisOptions`：`setFenodes`、`setTableIdentifier`、`setUsername`、`setPassword`。
4. 构建 `DorisExecutionOptions.Builder`：
   - `setStreamLoadProp(fmt.streamLoadProperties())`
   - `disable2PC()`（at-least-once）
   - `labelPrefix` 非空 → `setLabelPrefix`
   - `batchSize` 非空 → `setBufferCount` / 对应批量 API
5. `DorisReadOptions.builder().build()`（Sink 需传，用默认）。
6. ```java
   return DorisSink.<Row>builder()
       .setDorisOptions(dorisOptions)
       .setDorisReadOptions(readOptions)
       .setDorisExecutionOptions(execOptions)
       .setSerializer(fmt.createSerializer(cfg))
       .build();
   ```

> 注：步骤 4/6 的精确 builder 方法名（`setBufferCount` vs `setBufferFlushMaxRows`、2PC 关闭 API）以 1.5.2 实际类签名为准，实现阶段核对依赖 jar。

## 6. 数据流

```
sinks[].inputTable → Table → DataStream<Row> → DorisSink
   → RowToDorisJsonSerializer.serialize(Row) → DorisRecord(db,table,jsonBytes)
   → connector Stream Load → Doris
```

## 7. 配置示例

`docs/examples/batch-mock2doris.json`（Mock Source → Doris Sink）：

```json
{
  "job": { "name": "mock2doris", "mode": "batch" },
  "sources": [
    {
      "type": "mock",
      "outputTable": "t_src",
      "...": "mock 字段配置"
    }
  ],
  "sinks": [
    {
      "type": "doris",
      "inputTable": "t_src",
      "fenodes": "127.0.0.1:8030",
      "tableIdentifier": "test_db.test_tbl",
      "username": "root",
      "password": "",
      "format": "json",
      "labelPrefix": "etl-doris",
      "batchSize": 10000
    }
  ]
}
```

## 8. 客户端接线

`flink-etl-client/pom.xml` 增加 `connector-doris` 模块依赖（参考其他 connector）。

## 9. 测试范围

单元测试（JUnit 5，遵循 CLAUDE.md：只测函数/工具类，不测整 job 流程）：

- `DorisSinkConfigTest` — `fromSinkConfig` 必填校验、`tableIdentifier` 拆分、`format` 默认值。
- `RowToDorisJsonSerializerTest` — Row → JSON bytes 内容正确，db/table 正确填入 `DorisRecord`。
- `DorisFormatLoaderTest` — `getFormatPlugin("json")` 非空、未知 format 返回 null、`supportedFormats()` 含 json。

## 10. 文档维护

实现完成后更新 `PLUGINS.md`，新增 Doris Sink 条目（类型 `doris`、配置项、format 说明）。

## 11. 本期范围（YAGNI）

- 仅实现 `json` format。`debezium-json` 不在本期，但 SPI 已就位，后续加 `@AutoService` 插件即可。
- 仅 at-least-once，不做 exactly-once / 2PC。
- Stream Load 其余高级属性写死，不暴露透传 map。
