# 插件配置文档

本文档介绍所有可用的 Source、Sink、Transform 插件及其配置参数。

## 目录

- [配置文件格式](#配置文件格式)
- [配置变量替换](#配置变量替换)
- [Schema 配置](#schema-配置)
- [Source 插件](#source-插件)
    - [JDBC Source](#jdbc-source)
    - [LocalFile Source](#localfile-source)
    - [HTTP Source](#http-source)
    - [Kafka Source](#kafka-source)
    - [Mock Source](#mock-source)
- [Sink 插件](#sink-插件)
    - [Console Sink](#console-sink)
    - [JDBC Sink](#jdbc-sink)
    - [Kafka Sink](#kafka-sink)
- [Transform 插件](#transform-插件)
    - [SQL Transform](#sql-transform)
- [UDF 插件](#udf-插件)
    - [内置 UDF 函数](#内置-udf-函数)
    - [使用示例](#使用示例)
    - [扩展新 UDF](#扩展新-udf)

---

## 配置文件格式

配置采用 DataX 风格的 JSON 结构。

```json
{
  "job": {
    "name": "job-name",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "...",
      "outputTable": "...",
      "config": {
        ...
      }
    }
  ],
  "transforms": [
    {
      "type": "sql",
      "outputTable": "...",
      "config": {
        "sql": "..."
      }
    }
  ],
  "sinks": [
    {
      "type": "...",
      "inputTable": "...",
      "config": {
        ...
      }
    }
  ]
}
```

**数据流转机制：**

- `sources` → 每个 Source 的 `outputTable` 注册为 Table
- `transforms` → 链式处理，SQL 中引用上游的 `outputTable`
- `sinks` → 从 `inputTable` 读取数据写入目标

**job 配置项：**

- `name`: Job 名称
- `mode`: `batch` 或 `streaming`
- `parallelism`: 并行度（可选），分片数量等于并行度

---

## 配置变量替换

### 功能说明

支持在配置文件中使用变量占位符，运行时通过命令行参数动态传递值。适用于：

- 不同环境（开发/测试/生产）的配置切换
- 敏感信息（密码等）不暴露在配置文件中
- 参数化配置，无需维护多份配置文件

### 变量格式

- **`${variable}`** - 变量必须通过命令行参数定义，否则抛异常
- **`${variable:-default}`** - 变量未定义时使用默认值

### 使用方式

**命令行传参：**

```bash
java -jar app.jar --file config.json \
  --db_url jdbc:mysql://localhost:3306/test \
  --db_user root \
  --db_password secret
```

**配置文件示例：**

```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch"
  },
  "sources": [
    {
      "type": "jdbc",
      "outputTable": "users",
      "config": {
        "url": "${db_url}",
        "username": "${db_user:-root}",
        "password": "${db_password}",
        "table": "users"
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "users"
    }
  ]
}
```

### 变量参数规则

1. **所有非 `--file` 和 `--config` 的参数都会作为变量**
    - 拼写错误的参数也会被收集，需自行检查
    - 建议使用明确的参数名

2. **变量未定义时的行为**
    - 有默认值：使用默认值
    - 无默认值：抛异常 `"变量 'xxx' 未定义，请通过 --xxx 参数传递"`

3. **变量值为空字符串**
    - `--db_url ""` 会将变量设置为空字符串
    - 空字符串与未定义不同（空字符串不会触发异常）

### 错误处理示例

**错误：变量未定义**

```
配置变量替换失败：变量 'db_url' 未定义，请通过 --db_url 参数传递
```

**正确：提供参数**

```bash
--db_url jdbc:mysql://localhost:3306/test
```

### 注意事项

1. **变量替换在所有配置源生效**
    - `--file` 参数：文件内容先进行变量替换
    - `--config` 参数：JSON 字符串或 Base64 编码都支持变量替换

2. **JSON 必须是标准格式**
    - 不支持注释（`//` 或 `/* */`）
    - 变量替换后的内容必须能解析为有效 JSON

3. **特殊字符处理**
    - 变量值包含特殊字符（如 URL 参数 `&`）无需转义
    - ParameterTool 自动处理参数值

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
    "tags": [
      "STRING"
    ],
    "scores": [
      "INT"
    ]
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
      "zipcodes": [
        "INT"
      ]
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
      {
        "name": "STRING",
        "age": "INT"
      }
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
    "hobby": [
      "STRING"
    ],
    "address": {
      "city": "STRING",
      "zipcodes": [
        "INT"
      ]
    },
    "friends": [
      {
        "name": "STRING",
        "age": "INT",
        "tags": [
          "STRING"
        ]
      }
    ]
  }
}
```

**注意：** CSV 格式仅支持简单类型，复杂类型（ARRAY、OBJECT）用于 JDBC Source 或 JSON 文件格式。

---

## Source 插件

### JDBC Source

从 JDBC 兼容数据库读取数据，支持主键范围分片并行读取。

#### 配置参数

| 参数             |  必填  | 默认值  | 说明                                                      |
|----------------|:----:|------|---------------------------------------------------------|
| `url`          |  是   | -    | JDBC 连接 URL，格式：`jdbc:mysql://host:port/database`        |
| `username`     |  是   | -    | 数据库用户名                                                  |
| `password`     |  是   | -    | 数据库密码                                                   |
| `dialect`      |  否   | 自动识别 | 数据库方言，可选值：`mysql`、`postgresql`、`oracle`。不配置则根据 URL 自动识别 |
| `table`        | 条件必填 | -    | 表名。与 `sql` 二选一，优先                                       |
| `sql`          | 条件必填 | -    | 自定义查询 SQL。与 `table` 二选一                                 |
| `splitKey`     |  否   | 自动推断 | 分片列名，支持数值、字符串、日期类型。不配置时自动从主键推断                          |
| `batchSize`    |  否   | 100  | 批量读取大小                                                  |
| `queryTimeout` |  否   | 无限制  | 查询超时时间（秒）                                               |

#### 支持的分片策略

**NUMERIC（数值范围分片）**

- 支持类型：INT、BIGINT、DECIMAL、FLOAT 等
- 分片方式：查询 MIN/MAX → 均匀分割 → 开区间边界（`>= start AND < end`）
- 示例：`WHERE id >= 0 AND id < 100`
- 适用场景：数值主键、数值索引列

**DATE_RANGE（日期动态粒度分片）**

- 支持类型：DATE、TIMESTAMP
- 分片方式：查询 MIN/MAX 日期 → 计算总天数 → 动态决定每个分片天数
- 示例：`WHERE order_date >= '2020-01-01' AND order_date < '2020-02-01'`
- 适用场景：日期列、时间戳列
- 特点：自动根据数据天数调整分片粒度

**FULL_TABLE_SCAN（全表扫描）**

- 无主键或类型不支持时使用
- 无法并行读取
- 适用场景：小表、无主键表

**STRING_HASH（字符串 Hash Mod 分片）**

- 支持类型：VARCHAR、CHAR、NVARCHAR 等
- 分片方式：使用数据库 hash 函数（MD5、hashtext、ORA_HASH）→ 按 hash 值分片
- 示例（MySQL）：`WHERE ABS(CRC32(column) % 10) = 0`
- 适用场景：字符串主键、字符串索引列
- 注意：各数据库使用不同的 hash 函数

#### 自动推断逻辑

- 配置了 `splitKey` → 验证类型 → 选择对应策略
- 配置了 `table`，未配置 `splitKey` → 自动从主键推断 → 选择最优类型
- 配置了 `sql`，未配置 `splitKey` 和 `table` → 单分片全表扫描

#### 配置示例

**配置 table：**

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
      "splitKey": "id",
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
      "splitKey": "id",
      "queryTimeout": 300
    }
  }
}
```

**OceanBase Oracle 模式配置：**

```json
{
  "source": {
    "type": "jdbc",
    "outputTable": "users",
    "config": {
      "url": "jdbc:oceanbase://localhost:2883/test",
      "username": "admin",
      "password": "password",
      "table": "USERS",
      "dialect": "oracle",
      "splitKey": "ID",
      "batchSize": 1000
    }
  }
}
```

> **说明：** OceanBase 支持两种兼容模式：
> - MySQL 模式：URL 格式 `jdbc:oceanbase://host:2883/db`，自动识别为 MySQL 方言，无需显式配置 dialect
> - Oracle 模式：URL 格式 `jdbc:oceanbase://host:2883/db`，需显式配置 `dialect: "oracle"` 以使用 Oracle 方言

> **注意：**
> - 未配置 `splitKey` 时，系统会自动从表主键推断分片列
> - `splitKey` 支持数值类型：TINYINT, SMALLINT, INT, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC
> - `dialect` 参数用于显式指定数据库类型，适用于 URL 无法正确识别数据库类型的场景（如 OceanBase）

#### 分片说明

- 分片数量由 Job 配置的 `parallelism` 决定
- **自动推断：** 未配置 `splitKey` 时，自动从表主键推断分片列
    - 单主键：自动选择该主键列
    - 复合主键：优先选择数值类型范围最大的列（BIGINT > INT > SMALLINT > TINYINT > DECIMAL > FLOAT）
    - 无主键：任务失败，提示手动配置 `splitKey`
- **手动配置：** 配置 `splitKey` 时使用用户指定的列
- 数据会根据主键范围均匀分配到各并行度

---

### LocalFile Source

从本地文件系统读取文件，支持通配符匹配和多种文件格式。

#### 配置参数

| 参数           | 必填 | 默认值     | 说明                 |
|--------------|:--:|---------|--------------------|
| `path`       | 是  | -       | 文件路径，支持通配符 `*`     |
| `format`     | 是  | -       | 文件格式，目前支持 `csv`    |
| `encoding`   | 否  | `UTF-8` | 文件编码               |
| `delimiter`  | 否  | `,`     | CSV 字段分隔符          |
| `skipHeader` | 否  | `true`  | 是否跳过 CSV 文件头       |
| `recursive`  | 否  | `false` | 是否递归匹配子目录          |
| `schema`     | 是  | -       | Schema 定义，包含字段名和类型 |

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

| 通配符  | 说明                          | 示例                                  |
|------|-----------------------------|-------------------------------------|
| `*`  | 匹配单个目录下的文件名                 | `/data/*.csv` 匹配 `/data` 下所有 CSV 文件 |
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

| 参数         | 必填 | 默认值    | 说明                      |
|------------|:--:|--------|-------------------------|
| `url`      | 是  | -      | 请求 URL                  |
| `method`   | 否  | `GET`  | HTTP 方法，支持 `GET`、`POST` |
| `headers`  | 否  | `{}`   | 请求头，键值对形式               |
| `params`   | 否  | `{}`   | 查询参数，键值对形式              |
| `body`     | 否  | `null` | 请求体，JSON 对象形式           |
| `dataPath` | 否  | `null` | JSONPath 表达式，提取数据       |
| `schema`   | 是  | -      | Schema 定义，描述单条记录结构      |

#### dataPath 结果处理

| 提取结果类型     | 处理方式              |
|------------|-------------------|
| JSONArray  | 遍历数组，每个元素作为一行数据发送 |
| JSONObject | 作为单行数据发送          |
| 其他类型       | 抛出异常，提示数据格式错误     |

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
        "tags": [
          "STRING"
        ],
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

| 参数                 |  必填  | 默认值        | 说明                                                                 |
|--------------------|:----:|------------|--------------------------------------------------------------------|
| `bootstrapServers` |  是   | -          | Kafka 集群地址，如 `localhost:9092`                                      |
| `groupId`          |  是   | -          | 消费者组 ID                                                            |
| `topics`           | 条件必填 | -          | Topic 列表，与 `topicPattern` 二选一                                      |
| `topicPattern`     | 条件必填 | -          | Topic 正则表达式，与 `topics` 二选一                                         |
| `startupMode`      |  否   | `earliest` | 启动模式：`earliest`（从最早开始）、`latest`（从最新开始）、`committed`（从已提交 offset 开始） |
| `format`           |  否   | `json`     | 消息格式：`json`（标准 JSON）、`debezium-json`（Debezium CDC JSON）            |
| `properties`       |  否   | `{}`       | 额外的 Kafka consumer 配置                                              |
| `schema`           |  是   | -          | 消息体字段定义                                                            |

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
      "topics": [
        "user-events",
        "order-events"
      ],
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
        "tags": [
          "STRING"
        ]
      }
    }
  }
}
```

#### Debezium CDC 格式配置示例

**Kafka Source Debezium 配置:**

```json
{
  "source": {
    "type": "kafka",
    "outputTable": "users_cdc",
    "config": {
      "bootstrapServers": "localhost:9092",
      "groupId": "cdc-consumer",
      "topics": [
        "dbserver1.inventory.users"
      ],
      "startupMode": "earliest",
      "format": "debezium-json",
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "email": "STRING",
        "updated_at": "TIMESTAMP"
      }
    }
  }
}
```

**说明:**

- `format: "debezium-json"` 启用 Debezium CDC 数据解析
- `schema` 只需配置业务数据的字段结构（after/before 的字段），无需配置 Debezium 元数据
- 解析后的 Row 会自动设置 RowKind：
    - `op='c'/'r'` → INSERT
    - `op='u'` → UPDATE_AFTER
    - `op='d'` → DELETE

#### 数据解析说明

- 支持 JSON 对象和 JSON 数组两种消息格式
- JSON 数组会展开为多条 Row 记录
- Schema 始终描述单条记录的结构

#### 运行模式

- 流式消费，持续运行（`mode: "streaming"`）
- 支持 checkpoint 时自动提交 offset 到 Kafka

---

### MySQL CDC Source

实时捕获 MySQL 数据库变更事件,输出带 RowKind 标记的 Row 数据,支持 INSERT、UPDATE、DELETE 操作识别。

#### 配置参数

| 参数               | 必填 | 默认值      | 说明                                                      |
|------------------|:--:|----------|---------------------------------------------------------|
| `url`            | 是  | -        | JDBC 连接 URL,格式:`jdbc:mysql://host:port/database`        |
| `table`          | 是  | -        | 监听的表名(单表)                                               |
| `username`       | 是  | -        | 数据库用户名                                                  |
| `password`       | 是  | -        | 数据库密码                                                   |
| `startupMode`    | 否  | `latest` | 启动模式:`earliest`(从头开始)、`latest`(从最新位置)、`initial`(先快照再增量) |
| `serverId`       | 否  | 自动生成     | MySQL binlog client ID,范围 5400-15400。多任务需配置不同值避免冲突      |
| `serverTimeZone` | 否  | -        | 数据库服务器时区,如 `Asia/Shanghai`、`UTC`。不配置时使用 connector 默认值   |
| `splitKey`       | 否  | 自动获取     | 并行读取分片键,用于快照阶段并行读取。不配置时自动从数据库获取主键第一列                    |

#### 配置示例

**基础配置(从最新位置开始):**

```json
{
  "source": {
    "type": "mysql-cdc",
    "outputTable": "users_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users"
    }
  }
}
```

**从头开始捕获历史变更:**

```json
{
  "source": {
    "type": "mysql-cdc",
    "outputTable": "orders_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "orders",
      "startupMode": "earliest"
    }
  }
}
```

**先快照再增量(推荐生产使用):**

```json
{
  "source": {
    "type": "mysql-cdc",
    "outputTable": "inventory_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "inventory",
      "startupMode": "initial",
      "serverId": 5401,
      "serverTimeZone": "Asia/Shanghai",
      "splitKey": "id"
    }
  }
}
```

#### CDC 数据说明

**输出 Row 结构:**

- Row 字段自动从数据库表 Schema 获取(无需配置 schema 参数)
- Row 设置 RowKind 标记:
    - INSERT 操作 → `RowKind.INSERT`
    - UPDATE 操作 → `RowKind.UPDATE_AFTER`
    - DELETE 操作 → `RowKind.DELETE`

**与 JDBC Sink CDC 模式配合:**

MySQL CDC Source 输出的数据可直接写入 JDBC Sink CDC 模式,实现增量数据实时同步:

```json
{
  "job": {
    "name": "mysql-realtime-sync",
    "mode": "streaming",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "mysql-cdc",
      "outputTable": "source_cdc",
      "config": {
        "url": "jdbc:mysql://source-host:3306/source_db",
        "username": "root",
        "password": "password",
        "table": "users",
        "startupMode": "initial"
      }
    }
  ],
  "sinks": [
    {
      "type": "jdbc",
      "inputTable": "source_cdc",
      "config": {
        "url": "jdbc:mysql://target-host:3306/target_db",
        "username": "root",
        "password": "password",
        "table": "users",
        "mode": "cdc",
        "batchSize": 100
      }
    }
  ]
}
```

> **说明:**
> - MySQL CDC Source 使用 Ververica CDC Connector 实现
> - 监听 MySQL binlog 实时捕获变更事件
> - 支持单表监听,表 Schema 自动从数据库获取
> - 输出带 RowKind 的 Row,配合 JDBC Sink CDC 模式实现增量同步

#### 运行模式

- 流式运行,持续监听 binlog 事件(`mode: "streaming"`)
- 支持 checkpoint 时保存 binlog position,故障恢复从上次位置继续
- at-least-once 语义,故障恢复时可能产生重复事件

#### MySQL 前置要求

注意：不支持 8.4+ 版本以上的 mysql

MySQL 数据库需满足以下条件:

1. **开启 binlog**: MySQL 配置文件添加:
   ```
   [mysqld]
   log-bin=mysql-bin
   binlog-format=ROW
   server-id=1
   ```

2. **用户权限**: CDC 用户需有以下权限:
   ```sql
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
   FLUSH PRIVILEGES;
   ```

3. **表要求**: 表必须有主键(用于识别 DELETE 操作的 before 字段)

---

### Mock Source

用于测试和开发场景的数据模拟插件，支持固定数据和随机数据生成两种模式。

#### 配置参数

| 参数           | 必填 | 默认值    | 说明                                           |
|--------------|:--:|--------|----------------------------------------------|
| `schema`     | 是  | -      | Schema 定义，定义输出数据结构                           |
| `data`       | 否  | -      | 固定数据配置（JSON 数组），与 `numRows` 二选一。batch 模式优先使用 |
| `numRows`    | 否  | `10`   | 随机生成行数，与 `data` 二选一。batch 模式使用               |
| `intervalMs` | 否  | `1000` | 流式生成间隔（毫秒），streaming 模式使用                    |

**配置规则说明：**

- **Batch 模式**：
    - 优先使用 `data` 配置（固定数据）
    - 未配置 `data` 时使用 `numRows`（随机生成，默认 10 行）
    - `intervalMs` 配置会被忽略

- **Streaming 模式**：
    - 使用 `intervalMs` 控制生成间隔（默认 1000ms）
    - `data` 和 `numRows` 配置会被忽略
    - 持续生成随机数据流

#### data 配置格式

`data` 是一个 JSON 数组，每个元素是一行数据，字段名必须与 `schema` 定义一致。字段值根据 schema 类型自动转换：

- `LONG`、`INT`、`DOUBLE` → 数值类型
- `STRING` → 字符串类型
- `BOOLEAN` → 布尔类型
- `TIMESTAMP` → 时间戳（支持毫秒值或 ISO 格式）

#### 配置示例

**Batch 模式 - 固定数据：**

```json
{
  "source": {
    "type": "mock",
    "outputTable": "users",
    "config": {
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "age": "INT",
        "active": "BOOLEAN"
      },
      "data": [
        {
          "id": 1,
          "name": "Alice",
          "age": 25,
          "active": true
        },
        {
          "id": 2,
          "name": "Bob",
          "age": 30,
          "active": false
        },
        {
          "id": 1,
          "name": "Alice Updated",
          "age": 26,
          "active": true
        }
      ]
    }
  }
}
```

**Batch 模式 - 随机生成：**

```json
{
  "source": {
    "type": "mock",
    "outputTable": "orders",
    "config": {
      "schema": {
        "orderId": "LONG",
        "amount": "DOUBLE",
        "status": "STRING",
        "created_at": "TIMESTAMP"
      },
      "numRows": 50
    }
  }
}
```

**Streaming 模式 - 持续生成：**

```json
{
  "source": {
    "type": "mock",
    "outputTable": "events",
    "config": {
      "schema": {
        "eventId": "LONG",
        "eventType": "STRING",
        "timestamp": "TIMESTAMP"
      },
      "intervalMs": 500
    }
  }
}
```

#### 使用场景

- **功能测试**：使用固定数据验证业务逻辑，无需依赖外部数据源
- **性能测试**：使用随机生成大量数据测试系统吞吐量
- **开发调试**：快速生成测试数据，无需启动数据库或 Kafka
- **流式演示**：演示流处理功能，持续生成随机数据流

#### 数据生成规则

**随机数据生成规则（numRows 或 streaming 模式）：**

- `LONG`：随机长整数
- `INT`：随机整数
- `DOUBLE`：随机浮点数
- `STRING`：随机小写字母字符串（长度 1-10）
- `BOOLEAN`：随机布尔值
- `DECIMAL`：随机 Decimal 值（0-10000，保留 2 位小数）
- `TIMESTAMP`：当前时间附近的 LocalDateTime

**固定数据（data 模式）：**

- 完全按照配置的 JSON 数据生成 Row
- 数据类型严格匹配 schema 定义

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
    "inputTable": "output_data"
  }
}
```

---

### JDBC Sink

将数据写入 JDBC 兼容数据库，支持 table 和 sql 两种配置模式。

#### 配置参数

| 参数                |  必填  | 默认值      | 说明                                                      |
|-------------------|:----:|----------|---------------------------------------------------------|
| `url`             |  是   | -        | JDBC 连接 URL                                             |
| `username`        |  是   | -        | 数据库用户名                                                  |
| `password`        |  是   | -        | 数据库密码                                                   |
| `dialect`         |  否   | 自动识别     | 数据库方言，可选值：`mysql`、`postgresql`、`oracle`。不配置则根据 URL 自动识别 |
| `table`           | 条件必填 | -        | 目标表名。INSERT/UPSERT/CDC 模式必填，CUSTOM 模式忽略                 |
| `sql`             | 条件必填 | -        | 自定义 SQL，支持具名占位符 `:paramName`。CUSTOM 模式必填，其他模式忽略         |
| `mode`            |  否   | `upsert` | 写入模式：`insert`、`upsert`、`cdc`、`custom`                   |
| `keyFields`       | 条件必填 | 自动获取     | UPSERT/CDC 模式可选（自动获取主键），INSERT/CUSTOM 模式忽略              |
| `batchSize`       |  否   | `100`    | 批量写入大小，达到此数量触发刷写                                        |
| `batchIntervalMs` |  否   | `0`      | 批量刷写间隔（毫秒），`0` 表示禁用时间触发。与 `batchSize` 配合使用，任意条件满足即触发刷写  |

#### CDC 模式说明

**CDC 模式配置：**

CDC 模式用于处理带有 RowKind 标记的数据（如 Debezium CDC 数据），根据 RowKind 自动执行 INSERT/UPDATE/DELETE 操作。

**配置要求：**

- `mode: "cdc"` - 启用 CDC 模式
- `keyFields` - 指定主键字段列表，如未配置尝试从数据库获取

**配置示例：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "users_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/target_db",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "cdc",
      "batchSize": 100,
      "batchIntervalMs": 1000
    }
  }
}
```

**CDC 模式行为：**

- 根据 Row 的 RowKind 执行对应操作：
    - INSERT 和 UPDATE_AFTER → 执行 UPSERT SQL（原子操作，存在则更新，不存在则插入）
    - DELETE → 执行 DELETE SQL
- `keyFields` 用于 UPSERT 的主键匹配和 DELETE 的 WHERE 条件
- 适用于 Kafka Source 使用 `format: "debezium-json"` 的场景

**使用 upsert SQL 的优势：**

- 原子性：一条 SQL 完成插入或更新，避免并发问题
- 简化逻辑：统一处理 INSERT 和 UPDATE，参数设置相同
- 性能优化：数据库层面的原子操作更高效

**支持的数据库：**

- MySQL：使用 `INSERT ... ON DUPLICATE KEY UPDATE` 语法
- PostgreSQL：使用 `INSERT ... ON CONFLICT DO UPDATE` 语法
- Oracle：使用 `MERGE INTO` 语法
- H2：完整支持

---

#### CUSTOM 模式说明

**CUSTOM 模式配置：**

CUSTOM 模式用于执行用户自定义 SQL，实现复杂写入逻辑（如多表插入、自定义 upsert、批量更新等）。

**配置要求：**

- `mode: "custom"` - 启用 CUSTOM 模式
- `sql` - **必须配置**，支持具名占位符 `:paramName`
- `table` 和 `keyFields` - **会被忽略**，即使配置也不会使用

**配置示例：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "user_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "mode": "custom",
      "sql": "INSERT INTO user_stats(user_id, total_orders, last_update) VALUES(:userId, :orderCount, NOW()) ON DUPLICATE KEY UPDATE total_orders=VALUES(total_orders), last_update=NOW()",
      "batchSize": 100
    }
  }
}
```

**适用场景：**

- 自定义 upsert 逻辑（复杂条件判断）
- 跨表更新或插入
- 使用数据库函数或特殊语法
- 不依赖自动生成 SQL 的场景

---

#### UPSERT 模式说明

**主键配置机制（两种方式）：**

1. **自动获取主键（推荐）**：
    - 不配置 `keyFields` 参数，系统自动从数据库获取主键信息
    - 复合主键表会使用所有主键列作为条件字段
    - 表必须有主键，否则抛出异常

2. **手动指定主键（可选）**：
    - 配置 `keyFields` 参数，显式指定主键/唯一键字段列表
    - 适用于表无主键但有唯一索引、或需使用部分字段作为条件的场景
    - 格式：`["field1", "field2"]`

**配置优先级：**

- 用户配置 `keyFields` → 使用配置的字段列表
- 未配置 `keyFields` → 自动从数据库获取主键

**约束条件：**

- **必须配置 table，不能配置 sql**：主键信息只存在于物理表（自动获取时）
- **表必须有主键（自动获取时）**：无主键表需手动配置 keyFields

**错误处理：**

- 配置 sql 时抛异常：`"UPSERT 模式必须配置 table，因为需要主键信息"`
- 未配置 keyFields 且表无主键时抛异常：`"表 'xxx' 没有主键，无法使用 UPSERT 模式。请使用 INSERT 模式、手动配置 keyFields 或为表添加主键"`

---

#### 模式与配置要求

JDBC Sink 支持 4 种写入模式，每种模式有明确的配置要求：

| 模式     | 必需配置    | 忽略配置                | keyFields 处理 |
|--------|---------|---------------------|--------------|
| INSERT | `table` | `sql`、`keyFields`   | 不需要          |
| UPSERT | `table` | `sql`               | 可选（自动获取主键）   |
| CDC    | `table` | `sql`               | 可选（自动获取主键）   |
| CUSTOM | `sql`   | `table`、`keyFields` | 不需要          |

**配置规则说明：**

- 每个模式都有明确的必需配置，配置错误的参数会被忽略
- UPSERT/CDC 模式的 `keyFields` 不配置时自动从数据库获取主键
- 不建议配置不符合模式的参数，虽然会被忽略但容易造成混淆

---

#### 批量写入控制

JDBC Sink 支持两种批量刷写触发机制：

**触发条件：**

1. **数据量触发**：累计数据达到 `batchSize` 时触发刷写
2. **时间触发**：距离上次刷写超过 `batchIntervalMs` 时触发刷写（仅当 `batchIntervalMs > 0`）

**两种触发机制配合使用，任意条件满足即触发刷写：**

| 配置组合                                  | 适用场景   | 行为说明                                   |
|---------------------------------------|--------|----------------------------------------|
| `batchSize=100, batchIntervalMs=0`    | 高吞吐量场景 | 纯数据量触发，满 100 条刷写，数据不足时等待下一个 checkpoint |
| `batchSize=100, batchIntervalMs=1000` | 流式实时场景 | 双重触发，满 100 条或间隔 1 秒都会刷写，平衡吞吐和延迟        |
| `batchSize=1, batchIntervalMs=1000`   | 低延迟场景  | 每条数据立即刷写或最多等待 1 秒，适合实时性要求高的场景          |

**推荐配置：**

- **批处理任务**：建议使用 `batchIntervalMs=0`，最大化吞吐量
- **流处理任务**：建议配置 `batchIntervalMs=1000-5000`，避免长时间未刷写导致数据延迟

#### 配置示例

**UPSERT 模式（默认值） - 存在则更新，不存在则插入：**

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
      "batchSize": 100,
      "batchIntervalMs": 1000
    }
  }
}
```

**INSERT 模式 - 强制插入所有记录：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "mode": "INSERT",
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "target_table",
      "batchSize": 100
    }
  }
}
```

**CUSTOM 模式 - 自定义 SQL：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "mode": "CUSTOM",
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "sql": "INSERT INTO user_table(id, name, email) VALUES(:id, :name, :email) ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email)",
      "batchSize": 100
    }
  }
}
```

**table 模式 - UPSERT（自动获取主键）：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "mode": "UPSERT",
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "target_table",
      "batchSize": 100
    }
  }
}
```

> **说明：** UPSERT 模式会自动从数据库获取 `target_table` 的主键信息，无需配置 keyFields。

**table 模式 - UPSERT 手动指定主键：**

适用场景：表无主键但有唯一索引，或需使用部分字段作为匹配条件。

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "upsert",
      "keyFields": [
        "email"
      ],
      "batchSize": 100
    }
  }
}
```

> - **说明：** 该配置使用 `email` 字段作为 UPSERT 的匹配条件，即使表有其他主键也不会使用。
> - `dialect` 参数用于显式指定数据库类型，适用于 URL 无法正确识别数据库类型的场景（如 OceanBase）
> - 不同数据库的 upsert 语法不同：MySQL 使用 `ON DUPLICATE KEY UPDATE`，PostgreSQL 使用 `ON CONFLICT`，Oracle 使用
    `MERGE INTO`

#### 具名占位符说明

- 格式：`:paramName`，如 `:id`、`:name`、`:email`
- 参数名必须与 Row 字段名严格匹配
- 支持字母、数字、下划线，以字母或下划线开头

#### 多数据库支持

JDBC Sink 自动识别数据库类型并使用对应的标识符转义：

| 数据库                   | 标识符转义        | URL 示例                                       |
|-----------------------|--------------|----------------------------------------------|
| MySQL                 | `` `name` `` | `jdbc:mysql://host:3306/db`                  |
| PostgreSQL            | `"name"`     | `jdbc:postgresql://host:5432/db`             |
| Oracle                | `"name"`     | `jdbc:oracle:thin:@host:1521:SID`            |
| OceanBase (Oracle 模式) | `"name"`     | `jdbc:oceanbase://host:2883/db`              |
| SQLite                | `"name"`     | `jdbc:sqlite:/path/to/db`                    |
| SQL Server            | `[name]`     | `jdbc:sqlserver://host:1433;databaseName=db` |

#### JDBC 驱动依赖配置

不同数据库需要对应的 JDBC 驱动依赖：

**MySQL 驱动（已包含）：**

```xml

<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
    <version>8.0.33</version>
</dependency>
```

**OceanBase 驱动（已包含）：**

```xml

<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>oceanbase-client</artifactId>
    <version>2.4.3</version>
</dependency>
```

**Oracle 驱动（需要手动添加）：**

```xml

<dependency>
    <groupId>com.oracle.database.jdbc</groupId>
    <artifactId>ojdbc8</artifactId>
    <version>21.11.0.0</version>
</dependency>
```

> **注意：** Oracle JDBC 驱动可能需要从 Oracle 官网下载或使用第三方 Maven 仓库

---

### Kafka Sink

将数据写入 Kafka Topic，消息格式为 JSON。

#### 配置参数

| 参数                 | 必填 | 默认值  | 说明                            |
|--------------------|:--:|------|-------------------------------|
| `bootstrapServers` | 是  | -    | Kafka 集群地址，如 `localhost:9092` |
| `topic`            | 是  | -    | 目标 Topic 名称                   |
| `keyField`         | 否  | -    | Key 字段名，从 Row 中提取该字段值作为消息 Key |
| `properties`       | 否  | `{}` | 额外的 Kafka Producer 配置         |

#### 配置示例

**基础配置：**

```json
{
  "sink": {
    "type": "kafka",
    "inputTable": "output_data",
    "config": {
      "bootstrapServers": "localhost:9092",
      "topic": "output-topic"
    }
  }
}
```

**配置消息 Key：**

```json
{
  "sink": {
    "type": "kafka",
    "inputTable": "user_events",
    "config": {
      "bootstrapServers": "localhost:9092",
      "topic": "user-events",
      "keyField": "userId",
      "properties": {
        "compression.type": "gzip",
        "batch.size": "16384"
      }
    }
  }
}
```

#### 数据格式说明

- **消息体格式**：JSON 对象，字段名来自 Row 的字段名
- **消息 Key**：可选，根据 `keyField` 配置从 Row 中提取指定字段值
- **消息分区**：如果配置了 `keyField`，消息会根据 Key 哈希分配到分区；否则轮询分配

#### 与 Kafka Source 配合使用

Kafka Sink 与 Kafka Source 可以形成完整的数据流转链路：

```json
{
  "job": {
    "name": "kafka-transform",
    "mode": "streaming",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "kafka",
      "outputTable": "source_events",
      "config": {
        "bootstrapServers": "localhost:9092",
        "groupId": "transform-consumer",
        "topics": [
          "input-topic"
        ],
        "schema": {
          "userId": "LONG",
          "eventType": "STRING",
          "timestamp": "LONG"
        }
      }
    }
  ],
  "transforms": [
    {
      "type": "sql",
      "outputTable": "processed_events",
      "config": {
        "sql": "SELECT userId, eventType, timestamp FROM source_events WHERE userId > 0"
      }
    }
  ],
  "sinks": [
    {
      "type": "kafka",
      "inputTable": "processed_events",
      "config": {
        "bootstrapServers": "localhost:9092",
        "topic": "output-topic",
        "keyField": "userId"
      }
    }
  ]
}
```

#### 运行模式

- 流式写入，数据实时发送到 Kafka（`mode: "streaming"`）
- 支持 checkpoint 时确认消息写入成功
- at-least-once 语义，故障恢复时可能产生重复消息

---

## Transform 插件

### SQL Transform

通过 SQL 语句进行数据转换，支持 Flink Table API 的所有 SQL 语法。

#### 配置参数

| 参数    | 必填 | 默认值 | 说明                               |
|-------|:--:|-----|----------------------------------|
| `sql` | 是  | -   | SQL 查询语句，引用的表名为上游的 `outputTable` |

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

## UDF 插件

项目支持自定义 UDF（User Defined Function），可在 SQL Transform 中直接使用。

### 内置 UDF 函数

项目提供以下内置 UDF 函数，可在 SQL Transform 中直接使用：

#### 标量函数（ScalarFunction）

| 函数名         | 说明                    | 示例                                  |
|-------------|-----------------------|-------------------------------------|
| `hash_code` | 返回输入值的哈希码，null 输入返回 0 | `SELECT hash_code(name) FROM users` |

### 使用示例

**在 SQL Transform 中使用 UDF：**

```json
{
  "transforms": [
    {
      "type": "sql",
      "outputTable": "result_table",
      "config": {
        "sql": "SELECT id, name, hash_code(id) AS id_hash FROM source_table"
      }
    }
  ]
}
```

**UDF 与内置函数组合使用：**

```json
{
  "transforms": [
    {
      "type": "sql",
      "outputTable": "processed_users",
      "config": {
        "sql": "SELECT id, UPPER(name) AS name_upper, hash_code(email) AS email_hash FROM users WHERE id > 0"
      }
    }
  ]
}
```

### 扩展新 UDF

如需添加新的 UDF 函数，请参考设计文档：`docs/superpowers/specs/2026-04-07-flink-udf-design.md`

**开发流程：**

1. 在对应的文件夹下创建新 UDF 类：
    - 标量函数：`flink-etl-core/src/main/java/com/etl/core/udf/scalar/`
    - 表值函数：`flink-etl-core/src/main/java/com/etl/core/udf/table/`
    - 聚合函数：`flink-etl-core/src/main/java/com/etl/core/udf/agg/`
    - 表值聚合函数：`flink-etl-core/src/main/java/com/etl/core/udf/tagg/`

2. 实现 `UdfPlugin` 接口并添加 `@AutoService(UdfPlugin.class)` 注解

3. 编译项目：`mvn clean install -DskipTests`

4. UDF 会自动加载并在 SQL 中可用

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
        "splitKey": "id",
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
      "inputTable": "active_users"
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
        "splitKey": "id",
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