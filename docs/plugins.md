# 插件配置文档

本文档介绍所有可用的 Source、Sink、Transform 插件及其配置参数。

## 目录

- [Source 插件](#source-插件)
  - [JDBC Source](#jdbc-source)
  - [LocalFile Source](#localfile-source)
- [Sink 插件](#sink-插件)
  - [Console Sink](#console-sink)
  - [MySQL Sink](#mysql-sink)
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
| `table` | 条件必填 | - | 表名。与 `sql` 二选一 |
| `sql` | 条件必填 | - | 自定义查询 SQL。与 `table` 二选一 |
| `splitColumn` | 是 | - | 分片列名，通常为主键列 |
| `batchSize` | 否 | 100 | 批量读取大小 |
| `queryTimeout` | 否 | 无限制 | 查询超时时间（秒） |
| `schema` | 否 | 自动推断 | Schema 定义，不配置则从数据库元数据自动推断 |

#### schema 格式

Schema 为数组格式，每项包含 `name` 和 `type`：

```json
"schema": [
  { "name": "id", "type": "INT" },
  { "name": "name", "type": "STRING" }
]
```

支持的数据类型：
- `STRING` - 字符串类型
- `BOOLEAN` - 布尔类型
- `INT` - 32位整数
- `LONG` - 64位长整数
- `DOUBLE` - 双精度浮点数
- `DATE` - 日期
- `TIMESTAMP` - 时间戳

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

#### schema 格式

```json
"schema": [
  { "name": "id", "type": "LONG" },
  { "name": "name", "type": "STRING" },
  { "name": "age", "type": "INT" }
]
```

支持的数据类型：`STRING`、`BOOLEAN`、`INT`、`LONG`、`DOUBLE`、`DATE`、`TIMESTAMP`

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
      "schema": [
        { "name": "id", "type": "LONG" },
        { "name": "name", "type": "STRING" },
        { "name": "age", "type": "INT" },
        { "name": "email", "type": "STRING" }
      ]
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
      "schema": [
        { "name": "id", "type": "STRING" },
        { "name": "value", "type": "DOUBLE" }
      ]
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

### MySQL Sink

将数据写入 MySQL 数据库，支持批量写入和 upsert 模式。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `table` | 是 | - | 目标表名 |
| `batchSize` | 否 | `100` | 批量写入大小 |
| `writeMode` | 否 | `insert` | 写入模式：`insert` 或 `upsert` |

#### 配置示例

**INSERT 模式：**

```json
{
  "sink": {
    "type": "mysql",
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

**Upsert 模式：**

```json
{
  "sink": {
    "type": "mysql",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "target_table",
      "batchSize": 500,
      "writeMode": "upsert"
    }
  }
}
```

#### 写入模式说明

| 模式 | 说明 |
|------|------|
| `insert` | 直接插入数据，主键冲突会报错 |
| `upsert` | 使用 `INSERT ... ON DUPLICATE KEY UPDATE`，主键冲突时更新 |

#### 列名说明

- 列名从运行时 Row 的字段名中动态获取，无需在配置中指定
- Row 字段名应与目标表列名一致

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
  },
  "transforms": [
    {
      "type": "sql",
      "outputTable": "active_users",
      "config": {
        "sql": "SELECT id, name FROM users WHERE status = 1"
      }
    }
  ],
  "sink": {
    "type": "console",
    "inputTable": "active_users",
    "config": {}
  }
}
```

### CSV 文件到 MySQL

```json
{
  "job": {
    "name": "csv-to-mysql",
    "mode": "batch",
    "parallelism": 2
  },
  "source": {
    "type": "localfile",
    "outputTable": "csv_data",
    "config": {
      "path": "/data/input/*.csv",
      "format": "csv",
      "skipHeader": true,
      "schema": [
        { "name": "id", "type": "LONG" },
        { "name": "name", "type": "STRING" },
        { "name": "age", "type": "INT" },
        { "name": "email", "type": "STRING" }
      ]
    }
  },
  "sink": {
    "type": "mysql",
    "inputTable": "csv_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "import_data",
      "batchSize": 500,
      "writeMode": "insert"
    }
  }
}
```

### MySQL 到 MySQL（数据同步）

```json
{
  "job": {
    "name": "mysql-sync",
    "mode": "batch",
    "parallelism": 4
  },
  "source": {
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
  },
  "sink": {
    "type": "mysql",
    "inputTable": "source_data",
    "config": {
      "url": "jdbc:mysql://target-host:3306/target_db",
      "username": "root",
      "password": "password",
      "table": "target_table",
      "batchSize": 500,
      "writeMode": "upsert"
    }
  }
}
```