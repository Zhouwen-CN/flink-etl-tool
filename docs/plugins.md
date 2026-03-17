# 插件配置文档

本文档介绍所有可用的 Source、Sink、Transform 插件及其配置参数。

## 目录

- [Source 插件](#source-插件)
  - [MySQL Source](#mysql-source)
  - [LocalFile Source](#localfile-source)
- [Sink 插件](#sink-插件)
  - [Console Sink](#console-sink)
  - [MySQL Sink](#mysql-sink)
- [Transform 插件](#transform-插件)
  - [Field Mapping Transform](#field-mapping-transform)

---

## Source 插件

### MySQL Source

从 MySQL 数据库读取数据，支持主键范围分片并行读取。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL，格式：`jdbc:mysql://host:port/database` |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `table` | 条件必填 | - | 表名。与 `sql` 二选一 |
| `sql` | 条件必填 | - | 自定义查询 SQL。与 `table` 二选一 |
| `splitColumn` | 是 | - | 分片列名，通常为主键列 |
| `fetchSize` | 否 | 无限制 | JDBC fetch size，流式读取时建议设置 |
| `queryTimeout` | 否 | 无限制 | 查询超时时间（秒） |
| `schema` | 否 | 自动推断 | Schema 定义，不配置则从数据库元数据自动推断 |

#### schema 结构（可选）

| 字段 | 必填 | 说明 |
|------|:----:|------|
| `fields` | 是 | 字段列表，每项包含 `name` 和 `type` |

每个字段支持以下类型：
- `string` - 字符串类型
- `boolean` - 布尔类型
- `int` - 32位整数
- `long` - 64位长整数
- `double` - 双精度浮点数
- `decimal` - 高精度十进制数
- `timestamp` - 时间戳
- `bytes` - 字节数组

#### 配置示例

**基础配置 - 读取整表：**

```json
{
  "source": {
    "type": "mysql",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users",
      "splitColumn": "id",
      "fetchSize": 1000
    }
  }
}
```

**自定义 SQL 查询：**

```json
{
  "source": {
    "type": "mysql",
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
| `path` | 是 | - | 文件路径，支持通配符 `*` 和 `**` |
| `format` | 是 | - | 文件格式，目前支持 `csv` |
| `recursive` | 否 | `false` | 是否递归匹配子目录 |
| `encoding` | 否 | `UTF-8` | 文件编码 |
| `delimiter` | 否 | `,` | CSV 字段分隔符 |
| `skipHeader` | 否 | `false` | 是否跳过 CSV 文件头 |
| `schema` | 是 | - | Schema 定义，包含字段名和类型 |

#### schema 结构

| 字段 | 必填 | 说明 |
|------|:----:|------|
| `fields` | 是 | 字段列表，每项包含 `name` 和 `type` |

每个字段支持以下类型：
- `string` - 字符串类型
- `boolean` - 布尔类型
- `int` - 32位整数
- `long` - 64位长整数
- `double` - 双精度浮点数
- `decimal` - 高精度十进制数
- `timestamp` - 时间戳
- `bytes` - 字节数组

#### 配置示例

**CSV 文件读取（带类型转换）：**

```json
{
  "source": {
    "type": "localfile",
    "config": {
      "path": "/data/input/*.csv",
      "format": "csv",
      "encoding": "UTF-8",
      "delimiter": ",",
      "skipHeader": true,
      "schema": {
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "age", "type": "int"},
          {"name": "email", "type": "string"}
        ]
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
    "config": {
      "path": "/data/**/*.csv",
      "format": "csv",
      "recursive": true,
      "skipHeader": true,
      "schema": {
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "value", "type": "double"}
        ]
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

#### 类型转换规则

- 空字符串或 null 值转换为 null
- 数值类型解析失败会抛出 `TypeConversionException`
- 布尔类型支持 `true/false`、`1/0`、`yes/no` 等格式

---

## Sink 插件

### Console Sink

将数据输出到控制台，主要用于调试和测试。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `format` | 否 | `json` | 输出格式，支持 `json`、`text` |

#### 配置示例

```json
{
  "sink": {
    "type": "console",
    "config": {
      "format": "json"
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

**基础配置 - INSERT 模式：**

```json
{
  "sink": {
    "type": "mysql",
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

### Field Mapping Transform

字段映射转换，支持字段重命名和字段过滤。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `mappings` | 是 | - | 字段映射列表，每项包含 `from` 和 `to` |

#### mappings 结构

| 字段 | 必填 | 说明 |
|------|:----:|------|
| `from` | 是 | 源字段名 |
| `to` | 是 | 目标字段名 |

#### 配置示例

**字段重命名：**

```json
{
  "transforms": [
    {
      "type": "field-mapping",
      "config": {
        "mappings": [
          { "from": "id", "to": "user_id" },
          { "from": "name", "to": "user_name" }
        ]
      }
    }
  ]
}
```

**字段过滤（只保留映射的字段）：**

```json
{
  "transforms": [
    {
      "type": "field-mapping",
      "config": {
        "mappings": [
          { "from": "id", "to": "id" },
          { "from": "name", "to": "name" },
          { "from": "email", "to": "email" }
        ]
      }
    }
  ]
}
```

#### 行为说明

- 映射的字段会被重命名
- **未配置映射的字段会原样保留**（如需过滤，请只映射需要的字段）
- 多个 Transform 按配置顺序依次执行

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
    "type": "mysql",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users",
      "splitColumn": "id",
      "fetchSize": 1000
    }
  },
  "transforms": [
    {
      "type": "field-mapping",
      "config": {
        "mappings": [
          { "from": "id", "to": "user_id" },
          { "from": "name", "to": "user_name" }
        ]
      }
    }
  ],
  "sink": {
    "type": "console",
    "config": {
      "format": "json"
    }
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
    "config": {
      "path": "/data/input/*.csv",
      "format": "csv",
      "skipHeader": true,
      "schema": {
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "age", "type": "int"},
          {"name": "email", "type": "string"}
        ]
      }
    }
  },
  "sink": {
    "type": "mysql",
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
    "type": "mysql",
    "config": {
      "url": "jdbc:mysql://source-host:3306/source_db",
      "username": "root",
      "password": "password",
      "table": "source_table",
      "splitColumn": "id",
      "fetchSize": 1000
    }
  },
  "transforms": [
    {
      "type": "field-mapping",
      "config": {
        "mappings": [
          { "from": "id", "to": "id" },
          { "from": "name", "to": "name" },
          { "from": "created_at", "to": "created_at" }
        ]
      }
    }
  ],
  "sink": {
    "type": "mysql",
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