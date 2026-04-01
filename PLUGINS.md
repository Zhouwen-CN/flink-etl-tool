# 插件配置文档

本文档介绍所有可用的 Source、Sink、Transform 插件及其配置参数。

## 目录

- [Source 插件](#source-插件)
  - [JDBC Source](#jdbc-source)
  - [LocalFile Source](#localfile-source)
  - [HTTP Source](#http-source)
  - [Kafka Source](#kafka-source)
- [Schema 配置](#schema-配置)
- [Sink 插件](#sink-插件)
  - [Console Sink](#console-sink)
  - [JDBC Sink](#jdbc-sink)
- [Transform 插件](#transform-插件)
  - [SQL Transform](#sql-transform)

---

## Source 插件

### JDBC Source

从 JDBC 兼容数据库读取数据，支持主键范围分片并行读取。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL，格式：`jdbc:mysql://host:port/database` |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `table` | 条件必填 | - | 表名。与 `sql` 二选一，优先 |
| `sql` | 条件必填 | - | 自定义查询 SQL。与 `table` 二选一 |
| `splitColumn` | 否 | - | 分片列名，支持数值类型（TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/DECIMAL）。不配置则使用单分片全表扫描 |
| `batchSize` | 否 | 100 | 批量读取大小 |
| `queryTimeout` | 否 | 无限制 | 查询超时时间（秒） |
| `schema` | 否 | 自动推断 | Schema 定义，不配置则从数据库元数据自动推断 |

#### 配置示例

**基础配置 - 读取整表：**

```json
{
  "source": {
    "type": "jdbc",
    "outputTable": "users",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users",
      "splitColumn": "id",
      "batchSize": 1000
    }
  }
}
```

**自定义 SQL 查询：**

```json
{
  "source": {
    "type": "jdbc",
    "outputTable": "active_users",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "sql": "SELECT id, name, email FROM users WHERE status = 1",
      "splitColumn": "id",
      "queryTimeout": 300
    }
  }
}
```

**无分片列配置（单线程全表扫描）：**

```json
{
  "source": {
    "type": "jdbc",
    "outputTable": "users",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users",
      "batchSize": 1000
    }
  }
}
```

> **注意：**
> - 未配置 `splitColumn` 时将使用单分片全表扫描模式，无法并行读取数据
> - 对于大数据量表，建议配置 `splitColumn` 以启用并行分片读取
> - `splitColumn` 仅支持数值类型（TINYINT, SMALLINT, INT, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC），配置非数值类型列会报错

#### 分片说明

- 分片数量由 Job 配置的 `parallelism` 决定
- 分片列应为数值型主键，支持范围查询
- 数据会根据主键范围均匀分配到各并行度

---

### LocalFile Source

从本地文件系统读取文件，支持通配符匹配和多种文件格式。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `path` | 是 | - | 文件路径，支持通配符 `*` |
| `format` | 是 | - | 文件格式，目前支持 `csv` |
| `encoding` | 否 | `UTF-8` | 文件编码 |
| `delimiter` | 否 | `,` | CSV 字段分隔符 |
| `skipHeader` | 否 | `true` | 是否跳过 CSV 文件头 |
| `recursive` | 否 | `false` | 是否递归匹配子目录 |
| `schema` | 是 | - | Schema 定义，包含字段名和类型 |

#### 配置示例

**CSV 文件读取：**

```json
{
  "source": {
    "type": "localfile",
    "outputTable": "csv_data",
    "config": {
      "path": "/data/input/*.csv",
      "format": "csv",
      "encoding": "UTF-8",
      "delimiter": ",",
      "skipHeader": true,
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "age": "INT",
        "email": "STRING"
      }
    }
  }
}
```

**递归匹配子目录：**

```json
{
  "source": {
    "type": "localfile",
    "outputTable": "csv_data",
    "config": {
      "path": "/data/**/*.csv",
      "format": "csv",
      "recursive": true,
      "skipHeader": true,
      "schema": {
        "id": "STRING",
        "value": "DOUBLE"
      }
    }
  }
}
```

#### 通配符说明

| 通配符 | 说明 | 示例 |
|--------|------|------|
| `*` | 匹配单个目录下的文件名 | `/data/*.csv` 匹配 `/data` 下所有 CSV 文件 |
| `**` | 跨目录层级匹配（需 `recursive=true`） | `/data/**/*.csv` 递归匹配所有子目录中的 CSV 文件 |

#### 分片说明

- 每个匹配的文件对应一个分片
- 分片数量等于匹配到的文件数量
- Schema 配置必须与文件列数一致
- CSV 数据会根据 Schema 中定义的类型自动转换

---

### HTTP Source

从 HTTP API 获取 JSON 数据，支持 GET 和 POST 请求。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | 请求 URL |
| `method` | 否 | `GET` | HTTP 方法，支持 `GET`、`POST` |
| `headers` | 否 | `{}` | 请求头，键值对形式 |
| `params` | 否 | `{}` | 查询参数，键值对形式 |
| `body` | 否 | `null` | 请求体，JSON 对象形式 |
| `dataPath` | 否 | `null` | JSONPath 表达式，提取数据 |
| `schema` | 是 | - | Schema 定义，描述单条记录结构 |

#### dataPath 结果处理

| 提取结果类型 | 处理方式 |
|-------------|---------|
| JSONArray | 遍历数组，每个元素作为一行数据发送 |
| JSONObject | 作为单行数据发送 |
| 其他类型 | 抛出异常，提示数据格式错误 |

#### 配置示例

**GET 请求，直接返回数组：**

```json
{
  "source": {
    "type": "http",
    "outputTable": "users",
    "config": {
      "url": "https://api.example.com/users",
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "email": "STRING"
      }
    }
  }
}
```

**POST 请求，带请求体和 JSONPath 提取：**

```json
{
  "source": {
    "type": "http",
    "outputTable": "users",
    "config": {
      "url": "https://api.example.com/users/query",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer token123"
      },
      "body": {
        "status": "active"
      },
      "dataPath": "$.data.items",
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "tags": ["STRING"],
        "address": {
          "city": "STRING",
          "zip": "STRING"
        }
      }
    }
  }
}
```

#### 数据解析说明

- 未配置 `dataPath` 时，使用整个响应体作为数据源
- 配置 `dataPath` 后，使用 JSONPath 提取结果作为数据源
- Schema 始终描述单条记录的结构
- 支持复杂类型：`["简单类型"]`（数组）和 `OBJECT`（嵌套结构）

---

### Kafka Source

从 Kafka 消费 JSON 格式消息，支持 Topic 列表和正则匹配两种订阅方式。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `bootstrapServers` | 是 | - | Kafka 集群地址，如 `localhost:9092` |
| `groupId` | 是 | - | 消费者组 ID |
| `topics` | 条件必填 | - | Topic 列表，与 `topicPattern` 二选一 |
| `topicPattern` | 条件必填 | - | Topic 正则表达式，与 `topics` 二选一 |
| `startupMode` | 否 | `earliest` | 启动模式：`earliest`（从最早开始）、`latest`（从最新开始）、`committed`（从已提交 offset 开始） |
| `properties` | 否 | `{}` | 额外的 Kafka consumer 配置 |
| `schema` | 是 | - | 消息体字段定义 |

**隐藏字段**：输出 Row 自动包含 `__topic__` 字段（STRING 类型），记录消息来源 Topic。

#### 配置示例

**Topic 列表模式：**

```json
{
  "source": {
    "type": "kafka",
    "outputTable": "user_events",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "etl-consumer",
      "topics": ["user-events", "order-events"],
      "startupMode": "earliest",
      "schema": {
        "userId": "LONG",
        "eventType": "STRING",
        "timestamp": "LONG"
      }
    }
  }
}
```

**正则匹配模式：**

```json
{
  "source": {
    "type": "kafka",
    "outputTable": "metrics",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "metrics-consumer",
      "topicPattern": "metrics-.*",
      "startupMode": "latest",
      "properties": {
        "fetch.max.bytes": "52428800"
      },
      "schema": {
        "metric": "STRING",
        "value": "DOUBLE",
        "tags": ["STRING"]
      }
    }
  }
}
```

#### 数据解析说明

- 支持 JSON 对象和 JSON 数组两种消息格式
- JSON 数组会展开为多条 Row 记录
- Schema 始终描述单条记录的结构
- 自动追加 `__topic__` 字段记录消息来源

#### 运行模式

- 流式消费，持续运行（`mode: "streaming"`）
- 支持 checkpoint 时自动提交 offset 到 Kafka

---

## Schema 配置

Schema 用于定义数据结构，支持简单类型和复杂类型（ARRAY、OBJECT）。

### 简单类型

支持的简单类型：`STRING`, `BOOLEAN`, `INT`, `LONG`, `DOUBLE`, `DECIMAL`, `TIMESTAMP`

```json
{
  "schema": {
    "id": "LONG",
    "name": "STRING",
    "age": "INT"
  }
}
```

### 基础类型数组

数组类型，元素为简单类型，使用 JSON 数组格式：

```json
{
  "schema": {
    "tags": ["STRING"],
    "scores": ["INT"]
  }
}
```

### OBJECT 类型

嵌套对象类型，内部定义子字段：

```json
{
  "schema": {
    "address": {
      "city": "STRING",
      "street": "STRING"
    }
  }
}
```

### OBJECT 嵌套 ARRAY

对象内部包含数组字段：

```json
{
  "schema": {
    "address": {
      "city": "STRING",
      "zipcodes": ["INT"]
    }
  }
}
```

### ARRAY<OBJECT>

对象数组类型，使用数组形式定义：

```json
{
  "schema": {
    "friends": [
      {"name": "STRING", "age": "INT"}
    ]
  }
}
```

### 完整嵌套示例

综合使用各种类型的完整示例：

```json
{
  "schema": {
    "id": "LONG",
    "name": "STRING",
    "hobby": ["STRING"],
    "address": {
      "city": "STRING",
      "zipcodes": ["INT"]
    },
    "friends": [
      {
        "name": "STRING",
        "age": "INT",
        "tags": ["STRING"]
      }
    ]
  }
}
```

**注意：** CSV 格式仅支持简单类型，复杂类型（ARRAY、OBJECT）用于 JDBC Source 或 JSON 文件格式。

---

## Sink 插件开发指南

### 使用新 Sink API

所有新 Sink 插件推荐使用 `AbstractSink` 和 `AbstractSinkWriter` 基类。

#### AbstractSinkWriter 特点

- **最小化抽象**：只提供 context 和 config 字段访问
- **子类完全自主**：自行实现 write()、flush()、close() 方法
- **InitContext 访问**：通过 `context` 字段直接获取运行时信息（subtaskId、并行度、metrics）

#### 开发步骤

1. 创建 Sink 类，继承 `AbstractSink`
2. 在构造函数中进行参数校验和配置对象构建
3. 创建 Writer 类，继承 `AbstractSinkWriter`
4. 实现 `write()` 方法：写入数据逻辑（自行决定是否批量）
5. 实现 `flush()` 方法：提交数据逻辑（如批量提交）
6. 实现 `close()` 方法：清理资源逻辑（如关闭连接）
7. 注册 SPI（使用 `@AutoService(SinkPlugin.class)`）

#### 批量管理

需要批量写入的 Sink（如 JDBC）自行管理：
- 维护 `batchSize` 和 `pendingCount` 字段
- 在 `write()` 中判断是否触发 flush
- 在 `flush()` 中执行批量提交
- 在 `close()` 中提交剩余数据

不需要批量的 Sink（如 Console）：
- `write()` 直接输出
- `flush()` 空实现
- `close()` 空实现或简单清理

### InitContext 使用

Writer 可以通过 `context` 字段访问：
- `context.getSubtaskId()` - 获取子任务 ID
- `context.getNumberOfParallelSubtasks()` - 获取总并行度
- `context.metricGroup()` - 获取度量组（用于上报指标）

### 异常处理

- `write()` 失败 → 抛出 IOException，Flink 从 checkpoint 重试
- `flush()` 失败 → 自行处理异常（如 rollback），然后抛出 IOException
- `close()` 时 flush 失败 → 抛出异常，任务失败

---

## Sink 插件

### Console Sink

将数据输出到控制台，主要用于调试和测试。默认显示 subtask 信息。

#### 配置参数

无配置参数。

#### 配置示例

```json
{
  "sink": {
    "type": "console",
    "inputTable": "output_data",
    "config": {}
  }
}
```

---

### JDBC Sink

将数据写入 JDBC 兼容数据库，支持 table 和 sql 两种配置模式。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `table` | 条件必填 | - | 目标表名。与 `sql` 二选一，优先 |
| `sql` | 条件必填 | - | 自定义 SQL，支持具名占位符 `:paramName` |
| `mode` | 否 | `insert` | 写入模式：`insert`（插入）或 `upsert`（存在则更新） |
| `keyFields` | upsert 必填 | - | Upsert 模式的主键/唯一键字段，数组格式 |
| `batchSize` | 否 | `100` | 批量写入大小 |

#### 两种模式

| 模式 | 说明 |
|------|------|
| `table` 模式 | 自动生成 `INSERT INTO table(col1, col2, ...) VALUES(?, ?...)`，列名从 Row 字段名获取 |
| `sql` 模式 | 自定义 SQL，使用具名占位符 `:paramName`，可实现 upsert 等复杂逻辑 |

#### 配置示例

**table 模式 - 自动生成 INSERT：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "target_table",
      "batchSize": 100
    }
  }
}
```

**sql 模式 - 实现 upsert（MySQL）：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "sql": "INSERT INTO user_table(id, name, email) VALUES(:id, :name, :email) ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email)",
      "batchSize": 100
    }
  }
}
```

**table 模式 upsert（MySQL）：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "user_table",
      "mode": "upsert",
      "keyFields": ["id"],
      "batchSize": 100
    }
  }
}
```

生成的 SQL：
```sql
INSERT INTO `user_table` (`id`, `name`, `email`) VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE `name`=VALUES(`name`), `email`=VALUES(`email`)
```

#### 具名占位符说明

- 格式：`:paramName`，如 `:id`、`:name`、`:email`
- 参数名必须与 Row 字段名严格匹配
- 支持字母、数字、下划线，以字母或下划线开头

#### 多数据库支持

JDBC Sink 自动识别数据库类型并使用对应的标识符转义：

| 数据库 | 标识符转义 | URL 示例 |
|--------|-----------|----------|
| MySQL | `` `name` `` | `jdbc:mysql://host:3306/db` |
| PostgreSQL | `"name"` | `jdbc:postgresql://host:5432/db` |
| SQLite | `"name"` | `jdbc:sqlite:/path/to/db` |
| SQL Server | `[name]` | `jdbc:sqlserver://host:1433;databaseName=db` |

---

## Transform 插件

### SQL Transform

通过 SQL 语句进行数据转换，支持 Flink Table API 的所有 SQL 语法。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `sql` | 是 | - | SQL 查询语句，引用的表名为上游的 `outputTable` |

#### 配置示例

**数据过滤：**

```json
{
  "transforms": [
    {
      "type": "sql",
      "outputTable": "filtered_users",
      "config": {
        "sql": "SELECT * FROM users WHERE id > 0"
      }
    }
  ]
}
```

**字段选择与重命名：**

```json
{
  "transforms": [
    {
      "type": "sql",
      "outputTable": "renamed_users",
      "config": {
        "sql": "SELECT id AS user_id, name AS user_name FROM users"
      }
    }
  ]
}
```

**多 Transform 链式处理：**

```json
{
  "transforms": [
    {
      "type": "sql",
      "outputTable": "filtered",
      "config": {
        "sql": "SELECT * FROM source_table WHERE status = 1"
      }
    },
    {
      "type": "sql",
      "outputTable": "final_output",
      "config": {
        "sql": "SELECT id, name, UPPER(email) AS email FROM filtered"
      }
    }
  ]
}
```

#### 数据流转机制

- `source.outputTable` → Source 输出注册为 Table
- `transform.inputTable`（SQL 中引用的表名）→ 从该 Table 读取数据
- `transform.outputTable` → Transform 结果注册为中间表
- `sink.inputTable` → 从该 Table 读取数据写入 Sink

---

## 完整配置示例

### MySQL 到 Console

```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "jdbc",
      "outputTable": "users",
      "config": {
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": "root",
        "password": "password",
        "table": "users",
        "splitColumn": "id",
        "batchSize": 1000
      }
    }
  ],
  "transforms": [
    {
      "type": "sql",
      "outputTable": "active_users",
      "config": {
        "sql": "SELECT id, name FROM users WHERE status = 1"
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "active_users",
      "config": {}
    }
  ]
}
```

### CSV 文件到 JDBC

```json
{
  "job": {
    "name": "csv-to-jdbc",
    "mode": "batch",
    "parallelism": 2
  },
  "sources": [
    {
      "type": "localfile",
      "outputTable": "csv_data",
      "config": {
        "path": "/data/input/*.csv",
        "format": "csv",
        "skipHeader": true,
        "schema": {
          "id": "LONG",
          "name": "STRING",
          "age": "INT",
          "email": "STRING"
        }
      }
    }
  ],
  "sinks": [
    {
      "type": "jdbc",
      "inputTable": "csv_data",
      "config": {
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": "root",
        "password": "password",
        "table": "import_data",
        "batchSize": 500
      }
    }
  ]
}
```

### MySQL 到 MySQL（数据同步 + upsert）

```json
{
  "job": {
    "name": "mysql-sync",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "jdbc",
      "outputTable": "source_data",
      "config": {
        "url": "jdbc:mysql://source-host:3306/source_db",
        "username": "root",
        "password": "password",
        "table": "source_table",
        "splitColumn": "id",
        "batchSize": 1000
      }
    }
  ],
  "sinks": [
    {
      "type": "jdbc",
      "inputTable": "source_data",
      "config": {
        "url": "jdbc:mysql://target-host:3306/target_db",
        "username": "root",
        "password": "password",
        "sql": "INSERT INTO target_table(id, name, email) VALUES(:id, :name, :email) ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email)",
        "batchSize": 500
      }
    }
  ]
}
```