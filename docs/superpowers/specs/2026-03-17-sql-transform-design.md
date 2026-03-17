# SQL Transform 设计文档

## 背景

当前项目已有类型系统（EtlSchema）和 Flink 类型转换器（FlinkTypeConverter），但 Transform 仅支持简单的字段映射。为了支持更复杂的数据转换逻辑，引入 SQL Transform，让用户可以通过 SQL 语句进行数据过滤、转换、聚合等操作。

## 目标

1. 强制 Source 配置 Schema（包含 tableName），将 Source 注册为 Flink Table
2. 支持 SQL Transform，用户可直接编写 SQL 处理数据
3. Sink 统一消费 Table，由 Sink 决定如何处理

## 设计

### 1. EtlSchema 增强

新增 `tableName` 字段，使 Schema 成为完整的表定义：

```java
@Data
public class EtlSchema implements Serializable {
    private String tableName;        // 新增：表名，用于注册 Flink Table
    private List<EtlField> fields;   // 现有字段列表
}
```

### 2. 配置格式

Schema 配置变为必填，包含 tableName：

```json
{
  "source": {
    "type": "mysql",
    "config": {
      "url": "jdbc:mysql://...",
      "table": "user_table",
      "schema": {
        "tableName": "users",
        "fields": [
          { "name": "id", "type": "LONG" },
          { "name": "name", "type": "STRING" }
        ]
      }
    }
  },
  "transform": {
    "type": "sql",
    "config": {
      "sql": "SELECT id, name FROM users WHERE id > 10"
    }
  },
  "sink": {
    "type": "console",
    "config": { "format": "json" }
  }
}
```

### 3. SchemaParser 更新

解析时读取 `tableName` 字段：

```java
public static EtlSchema parse(Object schemaConfig) {
    // ... 现有解析逻辑 ...

    EtlSchema schema = new EtlSchema();
    schema.setTableName((String) schemaMap.get("tableName"));
    schema.setFields(fields);
    return schema;
}
```

### 4. TransformPlugin 接口变更

输入输出改为 Table，新增 StreamTableEnvironment 参数：

```java
public interface TransformPlugin {
    String getType();

    /**
     * 执行转换
     * @param inputTable 输入表
     * @param config 转换配置
     * @param stEnv Table 环境
     * @return 转换后的表
     */
    Table transform(Table inputTable, TransformConfig config, StreamTableEnvironment stEnv);
}
```

### 5. SqlTransformPlugin 实现

```java
@AutoService(TransformPlugin.class)
public class SqlTransformPlugin implements TransformPlugin {

    @Override
    public String getType() {
        return "sql";
    }

    @Override
    public Table transform(Table inputTable, TransformConfig config, StreamTableEnvironment stEnv) {
        String sql = config.getString("sql");
        return stEnv.sqlQuery(sql);
    }
}
```

### 6. SinkPlugin 接口变更

统一消费 Table：

```java
public interface SinkPlugin {
    String getType();

    /**
     * 消费 Table
     * @param table 结果表
     * @param config Sink 配置
     * @param stEnv Table 环境
     */
    void sink(Table table, SinkConfig config, StreamTableEnvironment stEnv);
}
```

### 7. ConsoleSink 适配示例

```java
@Override
public void sink(Table table, SinkConfig config, StreamTableEnvironment stEnv) {
    String format = config.getString("format");
    DataStream<Row> stream = stEnv.toDataStream(table);

    if ("json".equals(format)) {
        stream.map(row -> {
            StringBuilder sb = new StringBuilder("{");
            List<String> fieldNames = row.getFieldNames(true);
            for (int i = 0; i < fieldNames.size(); i++) {
                if (i > 0) sb.append(", ");
                String name = fieldNames.get(i);
                sb.append("\"").append(name).append("\": ");
                Object value = row.getField(name);
                if (value instanceof String) {
                    sb.append("\"").append(value).append("\"");
                } else {
                    sb.append(value);
                }
            }
            return sb.append("}").toString();
        }).print();
    } else {
        stream.print();
    }
}
```

### 8. JobBuilder 改造

集成 Table API：

```java
public void build(StreamExecutionEnvironment env, JobConfig config) {
    // 创建 Table 环境
    StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

    // 1. Source -> DataStream -> 注册 Table
    SourcePlugin sourcePlugin = pluginLoader.loadSourcePlugin(config.getSource().getType());
    Source<?, ?, ?> source = sourcePlugin.createSource(config.getSource());
    DataStream<Row> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");

    // 强制校验 Schema
    EtlSchema schema = config.getSource().getSchema();
    if (schema == null || schema.getTableName() == null) {
        throw new IllegalArgumentException("Source 必须配置 schema.tableName");
    }

    // 注册为 Table
    stEnv.createTemporaryView(schema.getTableName(), sourceStream);

    // 2. Transform
    Table resultTable = stEnv.from(schema.getTableName());
    if (config.getTransform() != null) {
        TransformPlugin transformPlugin = pluginLoader.loadTransformPlugin(config.getTransform().getType());
        resultTable = transformPlugin.transform(resultTable, config.getTransform(), stEnv);
    }

    // 3. Sink 消费 Table
    SinkPlugin sinkPlugin = pluginLoader.loadSinkPlugin(config.getSink().getType());
    sinkPlugin.sink(resultTable, config.getSink(), stEnv);
}
```

## 执行流程

```
┌─────────┐    ┌──────────────┐    ┌───────────────┐    ┌─────────┐
│ Source  │───>│ DataStream   │───>│ 注册为 Table  │───>│  SQL    │
│ Plugin  │    │   <Row>      │    │  (tableName)  │    │Transform│
└─────────┘    └──────────────┘    └───────────────┘    └────┬────┘
                                                              │
                                                              ▼
                                                        ┌─────────┐
                                                        │  Table  │
                                                        └────┬────┘
                                                             │
                                                              ▼
                                                        ┌─────────┐
                                                        │  Sink   │
                                                        │ Plugin  │
                                                        └─────────┘
```

## 变更清单

### 修改文件

| 文件 | 变更内容 |
|------|----------|
| `EtlSchema.java` | 新增 `tableName` 字段 |
| `SchemaParser.java` | 解析 `tableName` |
| `SourceConfig.java` | `getSchema()` 返回带 tableName 的 EtlSchema |
| `TransformPlugin.java` | 接口方法改为 `transform(Table, TransformConfig, StreamTableEnvironment)` |
| `SinkPlugin.java` | 接口方法改为 `sink(Table, SinkConfig, StreamTableEnvironment)` |
| `JobBuilder.java` | 集成 Table API，注册表、执行 SQL、调用 Sink |
| `ConsoleSinkFunction.java` | 适配新 SinkPlugin 接口 |
| `MySQLSinkFunction.java` | 适配新 SinkPlugin 接口 |

### 新增文件

| 文件 | 说明 |
|------|------|
| `SqlTransformPlugin.java` | SQL Transform 实现 |

### 删除文件

| 文件 | 说明 |
|------|------|
| `FieldMappingTransformPlugin.java` | 被_SQL Transform 替代 |

### 依赖变更

| 模块 | 新增依赖 |
|------|----------|
| `flink-etl-core` | `flink-table-api-java-bridge` |

## 后续扩展

当前设计为单 Source 单 Sink，后续可扩展：

1. **多 Source**：配置改为数组，注册多个 Table，SQL 可 JOIN
2. **多 Sink**：配置改为数组，每个 Sink 消费同一 Table 或不同 SQL 结果
3. **多 Transform**：支持链式 SQL Transform

## 风险点

1. **类型兼容性**：Source 输出的 Row 需要与 Schema 定义一致，否则注册 Table 会失败
2. **SQL 语法限制**：用户需了解 Flink SQL 语法限制
3. **性能考量**：DataStream <-> Table 转换有一定开销