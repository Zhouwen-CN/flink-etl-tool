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
package com.etl.core.schema;  // 包路径

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

解析时读取 `tableName` 字段，并进行校验：

```java
package com.etl.core.schema;  // 包路径

public class SchemaParser {

    @SuppressWarnings("unchecked")
    public static EtlSchema parse(Object schemaConfig) {
        if (schemaConfig == null) {
            return null;
        }

        if (!(schemaConfig instanceof Map)) {
            throw new SchemaConfigException("schema 必须是一个对象");
        }

        Map<String, Object> schemaMap = (Map<String, Object>) schemaConfig;

        // 解析并校验 tableName
        String tableName = (String) schemaMap.get("tableName");
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new SchemaConfigException("schema.tableName 不能为空");
        }

        // 解析 fields（现有逻辑）
        Object fieldsObj = schemaMap.get("fields");
        // ... 字段解析逻辑 ...

        EtlSchema schema = new EtlSchema();
        schema.setTableName(tableName);
        schema.setFields(fields);
        return schema;
    }
}
```

### 4. TransformPlugin 接口变更

输入输出改为 Table，新增 StreamTableEnvironment 参数：

```java
package com.etl.core.spi;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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

**注意**：这是一个**破坏性变更**。现有 `FieldMappingTransformPlugin` 将被删除，不再维护旧接口。

### 5. TransformConfig 增强

新增 `getString()` 便捷方法：

```java
package com.etl.core.config;

@Data
public class TransformConfig {
    private String type;
    private Map<String, Object> config;

    /**
     * 获取字符串类型的配置值
     */
    public String getString(String key) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        return value != null ? String.valueOf(value) : null;
    }

    /**
     * 获取配置值
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }
}
```

### 6. SqlTransformPlugin 实现

包含完整的错误处理：

```java
package com.etl.transform;

import com.etl.core.config.TransformConfig;
import com.etl.core.spi.TransformPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Slf4j
@AutoService(TransformPlugin.class)
public class SqlTransformPlugin implements TransformPlugin {

    @Override
    public String getType() {
        return "sql";
    }

    @Override
    public Table transform(Table inputTable, TransformConfig config, StreamTableEnvironment stEnv) {
        String sql = config.getString("sql");

        // 参数校验
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL Transform 配置缺少 'sql' 字段");
        }

        log.info("执行 SQL: {}", sql);

        try {
            return stEnv.sqlQuery(sql);
        } catch (Exception e) {
            throw new RuntimeException("SQL 执行失败: " + e.getMessage(), e);
        }
    }
}
```

### 7. SinkPlugin 接口变更

统一消费 Table：

```java
package com.etl.core.spi;

import com.etl.core.config.SinkConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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

**注意**：这是一个**破坏性变更**。现有 `createSink()` 方法将被移除。

### 8. ConsoleSinkPlugin 适配

```java
package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

@Slf4j
@AutoService(SinkPlugin.class)
public class ConsoleSinkPlugin implements SinkPlugin {

    @Override
    public String getType() {
        return "console";
    }

    @Override
    public void sink(Table table, SinkConfig config, StreamTableEnvironment stEnv) {
        String format = config.getString("format");
        DataStream<Row> stream = stEnv.toDataStream(table);

        if ("json".equals(format)) {
            stream.map(this::toJsonString).print();
        } else {
            stream.print();
        }
    }

    /**
     * 将 Row 转换为 JSON 字符串
     * 处理 null 值和特殊字符
     */
    private String toJsonString(Row row) {
        List<String> fieldNames = row.getFieldNames(true);
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fieldNames.size(); i++) {
            if (i > 0) sb.append(", ");
            String name = fieldNames.get(i);
            Object value = row.getField(name);
            sb.append("\"").append(escapeJson(name)).append("\": ");
            if (value == null) {
                sb.append("null");
            } else if (value instanceof String) {
                sb.append("\"").append(escapeJson((String) value)).append("\"");
            } else {
                sb.append(value);
            }
        }
        return sb.append("}").toString();
    }

    /**
     * 转义 JSON 字符串中的特殊字符
     */
    private String escapeJson(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
```

### 9. JobBuilder 改造

集成 Table API，支持多 Transform 链式处理：

```java
package com.etl.core.job;

import com.etl.core.config.EtlSchema;
import com.etl.core.config.JobConfig;
import com.etl.core.config.TransformConfig;
import com.etl.core.spi.PluginLoader;
import com.etl.core.spi.SinkPlugin;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.TransformPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

@Slf4j
public class JobBuilder {

    private final PluginLoader pluginLoader;

    public JobBuilder(PluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    public void build(StreamExecutionEnvironment env, JobConfig config) {
        log.info("开始构建 Flink Job: {}", config.getJob().getName());

        // 创建 Table 环境
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

        // 1. Source -> DataStream -> 注册 Table
        SourcePlugin sourcePlugin = pluginLoader.loadSourcePlugin(config.getSource().getType());
        Source<?, ?, ?> source = sourcePlugin.createSource(config.getSource());
        DataStream<Row> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");

        // 强制校验 Schema
        EtlSchema schema = config.getSource().getSchema();
        if (schema == null) {
            throw new IllegalArgumentException("Source 必须配置 schema");
        }
        if (schema.getTableName() == null || schema.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Source 的 schema.tableName 不能为空");
        }

        // 注册为 Table
        stEnv.createTemporaryView(schema.getTableName(), sourceStream);
        log.info("注册 Table: {}", schema.getTableName());

        // 2. Transform 链式处理
        Table resultTable = stEnv.from(schema.getTableName());

        List<TransformConfig> transforms = config.getTransforms();
        if (transforms != null && !transforms.isEmpty()) {
            for (TransformConfig transformConfig : transforms) {
                TransformPlugin transformPlugin = pluginLoader.loadTransformPlugin(transformConfig.getType());
                resultTable = transformPlugin.transform(resultTable, transformConfig, stEnv);
                log.info("Transform 应用成功: {}", transformConfig.getType());
            }
        }

        // 3. Sink 消费 Table
        SinkPlugin sinkPlugin = pluginLoader.loadSinkPlugin(config.getSink().getType());
        sinkPlugin.sink(resultTable, config.getSink(), stEnv);
        log.info("Sink 创建成功");

        log.info("Flink Job 构建完成");
    }
}
```

## 执行流程

```
┌─────────┐    ┌──────────────┐    ┌───────────────┐    ┌─────────────┐
│ Source  │───>│ DataStream   │───>│ 注册为 Table  │───>│ Transform 1 │
│ Plugin  │    │   <Row>      │    │  (tableName)  │    │   (SQL)     │
└─────────┘    └──────────────┘    └───────────────┘    └──────┬──────┘
                                                              │
                    ┌─────────────────────────────────────────┘
                    ▼
             ┌─────────────┐         ┌─────────┐
             │ Transform N │────────>│  Table  │
             │   (SQL)     │         └────┬────┘
             └─────────────┘              │
                                          ▼
                                    ┌─────────┐
                                    │  Sink   │
                                    │ Plugin  │
                                    └─────────┘
```

## 变更清单

### 修改文件

| 文件 | 包路径 | 变更内容 |
|------|--------|----------|
| `EtlSchema.java` | `com.etl.core.schema` | 新增 `tableName` 字段 |
| `SchemaParser.java` | `com.etl.core.schema` | 解析并校验 `tableName` |
| `TransformConfig.java` | `com.etl.core.config` | 新增 `getString()` 方法 |
| `TransformPlugin.java` | `com.etl.core.spi` | 接口方法改为 `transform(Table, TransformConfig, StreamTableEnvironment)` |
| `SinkPlugin.java` | `com.etl.core.spi` | 接口方法改为 `sink(Table, SinkConfig, StreamTableEnvironment)` |
| `JobBuilder.java` | `com.etl.core.job` | 集成 Table API，注册表、执行 SQL、调用 Sink |
| `ConsoleSinkPlugin.java` | `com.etl.sink.console` | 适配新 SinkPlugin 接口 |
| `MySQLSinkPlugin.java` | `com.etl.sink.mysql` | 适配新 SinkPlugin 接口 |

### 新增文件

| 文件 | 包路径 | 说明 |
|------|--------|------|
| `SqlTransformPlugin.java` | `com.etl.transform` | SQL Transform 实现 |

### 删除文件

| 文件 | 说明 |
|------|------|
| `FieldMappingTransformPlugin.java` | 被 SQL Transform 替代 |

### 依赖变更

| 模块 | 新增依赖 |
|------|----------|
| `flink-etl-core` | `flink-table-api-java-bridge` |

## 迁移指南

### 配置迁移

**旧配置（不再支持）：**

```json
{
  "source": {
    "type": "mysql",
    "config": {
      "url": "jdbc:mysql://...",
      "table": "user_table"
      // 无 schema 配置
    }
  },
  "transforms": [
    {
      "type": "field-mapping",
      "config": {
        "mappings": [
          { "from": "id", "to": "user_id" }
        ]
      }
    }
  ]
}
```

**新配置：**

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
  "transforms": [
    {
      "type": "sql",
      "config": {
        "sql": "SELECT id AS user_id, name FROM users"
      }
    }
  ]
}
```

### 字段映射迁移

原 `field-mapping` 功能可通过 SQL 实现：

| field-mapping | SQL 等价 |
|---------------|----------|
| `{ "from": "id", "to": "user_id" }` | `SELECT id AS user_id, ... FROM table` |
| 过滤字段 | `SELECT field1, field2 FROM table` |

## 示例配置

### 简单过滤

```json
{
  "source": {
    "type": "mysql",
    "config": {
      "schema": { "tableName": "users", "fields": [...] }
    }
  },
  "transforms": [
    {
      "type": "sql",
      "config": {
        "sql": "SELECT * FROM users WHERE status = 'active'"
      }
    }
  ],
  "sink": { "type": "console" }
}
```

### 多 Transform 链式处理

```json
{
  "transforms": [
    {
      "type": "sql",
      "config": { "sql": "SELECT * FROM users WHERE id > 100" }
    },
    {
      "type": "sql",
      "config": { "sql": "SELECT id, UPPER(name) as name FROM filtered_users" }
    }
  ]
}
```

**注意**：多 Transform 时，SQL 中引用的表名需要是已注册的表名（第一个 SQL 的结果会自动注册为新表，表名需要后续定义）。

### 无 Transform（直接透传）

```json
{
  "source": {
    "type": "mysql",
    "config": {
      "schema": { "tableName": "users", "fields": [...] }
    }
  },
  "sink": { "type": "console" }
}
```

## 后续扩展

当前设计为单 Source 单 Sink，后续可扩展：

1. **多 Source**：配置改为数组，注册多个 Table，SQL 可 JOIN
2. **多 Sink**：配置改为数组，每个 Sink 消费同一 Table 或不同 SQL 结果

## 风险点

1. **类型兼容性**：Source 输出的 Row 需要与 Schema 定义一致，否则注册 Table 会失败
2. **SQL 语法限制**：用户需了解 Flink SQL 语法限制
3. **性能考量**：DataStream <-> Table 转换有一定开销
4. **破坏性变更**：此版本不兼容旧配置，用户需要迁移