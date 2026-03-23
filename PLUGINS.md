# 插件配置文档

本文档介绍所有可用的 Source、Sink、Transform 插件及其配置参数。

## 目录

- [Source 插件](#source-插件)
  - [JDBC Source](#jdbc-source)
  - [LocalFile Source](#localfile-source)
  - [HTTP Source](#http-source)
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
| `splitColumn` | 是 | - | 分片列名，通常为主键列 |
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
        "tags": "ARRAY<STRING>",
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
- 支持复杂类型：`ARRAY<简单类型>` 和 `OBJECT`（嵌套结构）

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

### ARRAY<简单类型>

数组类型，元素为简单类型：

```json
{
  "schema": {
    "tags": "ARRAY<STRING>",
    "scores": "ARRAY<INT>"
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
      "zipcodes": "ARRAY<INT>"
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
    "hobby": "ARRAY<STRING>",
    "address": {
      "city": "STRING",
      "zipcodes": "ARRAY<INT>"
    },
    "friends": [
      {
        "name": "STRING",
        "age": "INT",
        "tags": "ARRAY<STRING>"
      }
    ]
  }
}
```

**注意：** CSV 格式仅支持简单类型，复杂类型（ARRAY、OBJECT）用于 JDBC Source 或 JSON 文件格式。

---

## Sink 插件

### Console Sink

将数据输出到控制台，主要用于调试和测试。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `showSubtask` | 否 | `true` | 是否显示分片子任务编号 |

#### 配置示例

```json
{
  "sink": {
    "type": "console",
    "inputTable": "output_data",
    "config": {
      "showSubtask": true
    }
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