# HTTP Source 设计文档

## 概述

新增 HTTP Source 插件，支持通过 HTTP 请求获取 JSON 数据，转换为 Flink Row 类型输出到下游。

## 使用场景

- 批处理模式下执行一次性 HTTP 请求
- 从 REST API 获取数据，支持复杂 JSON 响应结构
- 支持简单类型和复杂类型（ARRAY、OBJECT）Schema 定义

## 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | 请求 URL |
| `method` | 否 | `GET` | HTTP 方法，支持 `GET`、`POST` |
| `headers` | 否 | `{}` | 请求头，键值对形式 |
| `params` | 否 | `{}` | 查询参数，键值对形式 |
| `body` | 否 | `null` | 请求体，JSON 对象形式 |
| `dataPath` | 否 | `null` | JSONPath 表达式，提取数据 |
| `schema` | 是 | - | 响应数据的 Schema 定义，描述单条记录结构 |

## 配置示例

### GET 请求，直接返回数组

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

### POST 请求，带请求体和 JSONPath 提取

```json
{
  "source": {
    "type": "http",
    "outputTable": "users",
    "config": {
      "url": "https://api.example.com/users/query",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer token123",
        "Content-Type": "application/json"
      },
      "body": {
        "status": "active",
        "page": 1
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

## 数据解析逻辑

### 处理流程

1. **确定 root**：
   - 未配置 `dataPath`：root = 整个响应体
   - 配置了 `dataPath`：root = JSONPath 提取结果

2. **root 类型处理**：
   - **JSONObject**：作为单条记录，按 Schema 转换为 1 个 Row
   - **JSONArray**：遍历数组，每个元素按 Schema 转换为 Row

3. **Schema 定义**：始终描述单条记录的结构

### 示例对照

| 响应 | dataPath | root | Schema | 输出 |
|------|----------|------|--------|------|
| `{"id": 1, "name": "张三"}` | 无 | JSONObject | `{"id": "LONG", "name": "STRING"}` | 1 行 |
| `[{"id": 1}, {"id": 2}]` | 无 | JSONArray | `{"id": "LONG"}` | 2 行 |
| `{"data": [{"id": 1}]}` | `$.data` | JSONArray | `{"id": "LONG"}` | 1 行 |
| `{"code": 0, "data": {"id": 1}}` | `$.data` | JSONObject | `{"id": "LONG"}` | 1 行 |

## 模块结构

```
flink-etl-source-http/
├── pom.xml
└── src/main/java/com/etl/source/http/
    ├── HttpSourcePlugin.java          # SPI 插件入口
    ├── HttpSource.java                # Source 主类
    ├── HttpSourceConfig.java          # 配置封装类
    ├── HttpSplit.java                 # 分片定义
    ├── HttpSplitEnumerator.java       # 分片枚举器
    ├── HttpSourceReader.java          # 源阅读器
    ├── HttpSplitReader.java           # 分片读取器
    ├── HttpEnumCheckpoint.java        # 枚举器检查点
    └── HttpRecordEmitter.java         # 记录发射器
```

## 核心组件

### HttpSource

继承 `AbstractSplitSource<HttpSplit, HttpEnumCheckpoint>`，职责：
- 构造函数中完成参数校验和配置封装
- 返回 `Boundedness.BOUNDED`（批处理）
- 创建 Enumerator 和 Reader

### HttpSourceConfig

配置封装类，包含：
- url、method、headers、params、body、dataPath
- schema（EtlSchema 类型）
- 实现 Serializable

### HttpSplitEnumerator

单分片模式：
- `start()` 时创建一个 HttpSplit 分片
- 分片包含完整的请求配置信息

### HttpSplitReader

核心读取逻辑：
1. 构建 HTTP 请求（URL + method + headers + params + body）
2. 执行请求，获取响应
3. 使用 JSONPath 从响应中提取 root（如果配置了 dataPath）
4. 根据 root 类型处理：
   - JSONObject → 转换为单个 Row
   - JSONArray → 遍历数组，逐条转换为 Row
5. 通过 ResultQueue 发送到下游

### Row 转换

根据 Schema 定义，将 JSON 数据转换为 Flink Row：
- 支持简单类型：STRING、INT、LONG、DOUBLE、BOOLEAN、DECIMAL、TIMESTAMP
- 支持复杂类型：ARRAY<简单类型>、OBJECT（嵌套结构）
- 复用现有 SchemaParser 的类型转换能力

## 错误处理

| 场景 | 处理方式 |
|------|---------|
| HTTP 请求失败 | 抛出异常，任务终止 |
| 响应非 JSON | 抛出异常，提示响应格式错误 |
| JSONPath 提取失败 | 抛出异常，提示路径无效 |
| JSONPath 提取结果为 null | 抛出异常，提示数据为空 |
| root 既非 JSONObject 也非 JSONArray | 抛出异常，提示数据格式错误 |
| 字段类型转换失败 | 抛出异常，提示字段名和期望类型 |
| Schema 必填字段缺失 | 抛出异常，提示缺失字段名 |

## 依赖

- `flink-etl-core`：核心框架
- `Jackson`：JSON 解析（项目已有）
- `JsonPath`：JSONPath 解析（需新增依赖，推荐 `com.jayway.jsonpath:json-path`）

## 文档更新

实现完成后需更新 `PLUGINS.md`，添加 HTTP Source 配置说明。