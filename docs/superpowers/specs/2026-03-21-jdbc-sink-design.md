# JDBC Sink 设计文档

## 背景

当前 MySQL Sink 仅支持 MySQL 数据库，且 upsert 语法 `ON DUPLICATE KEY UPDATE` 是 MySQL 特有的。需要将其重构为通用 JDBC Sink，支持多种数据库，并提供更灵活的 SQL 配置方式。

## 需求

1. **模块重命名**：`flink-etl-sink-mysql` → `flink-etl-sink-jdbc`
2. **plugin type**：`mysql` → `jdbc`
3. **删除 writeMode**：不再支持自动 upsert
4. **table/sql 优先级**：table 优先，同时配置时忽略 sql
5. **table 模式**：标准 INSERT INTO，从 Row 字段名自动生成 SQL
6. **sql 模式**：支持具名占位符 `:paramName`，严格匹配 Row 字段名

## 设计

### 核心组件

#### 1. JdbcSinkConfig 配置类

```java
@Getter
@Builder
public class JdbcSinkConfig implements Serializable {
    private final String url;
    private final String username;
    private final String password;
    private final String table;      // table 优先
    private final String sql;        // 具名占位符 SQL
    private final int batchSize;
}
```

#### 2. JdbcSinkPlugin 入口

- 实现 `SinkPlugin` 接口
- type 返回 `jdbc`
- 校验必要配置（url, username, password）
- table/sql 优先级处理：table 优先
- 创建 `JdbcSinkFunction`

#### 3. JdbcSinkFunction 执行器

继承 `RichSinkFunction<Row>`，支持两种模式：

**table 模式：**
- 首条记录到达时从 Row 字段名生成 INSERT SQL
- 标准 `INSERT INTO table(col1, col2, ...) VALUES(?, ?, ...)`

**sql 模式：**
- 解析具名占位符 `:paramName`
- 生成 PreparedStatement
- 按 Row 字段名严格匹配填充参数

#### 4. NamedParameterSqlParser 解析器

负责解析具名占位符 SQL：

```java
public class NamedParameterSqlParser {
    // 输入: "INSERT INTO t(a, b) VALUES(:x, :y)"
    // 输出:
    //   - preparedSql: "INSERT INTO t(a, b) VALUES(?, ?)"
    //   - paramNames: ["x", "y"]
}
```

### 数据流

```
配置 JSON
    ↓
JdbcSinkPlugin.createSink()
    ↓ 校验必要配置
    ↓ table 优先处理
JdbcSinkConfig
    ↓
JdbcSinkFunction.open()
    ↓ 首条记录到达时
┌─────────────────────────────────────┐
│ table 模式                           │
│   从 Row 字段名生成 INSERT SQL        │
├─────────────────────────────────────┤
│ sql 模式                             │
│   解析具名占位符 → PreparedStatement  │
│   按字段名填充参数                    │
└─────────────────────────────────────┘
    ↓
批量写入 (batchSize)
```

### 配置项

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| url | 是 | - | JDBC 连接 URL |
| username | 是 | - | 用户名 |
| password | 是 | - | 密码 |
| table | 否 | - | 目标表名（与 sql 二选一，优先） |
| sql | 否 | - | 自定义 SQL，支持具名占位符 |
| batchSize | 否 | 100 | 批量写入大小 |

### 配置示例

**table 模式：**
```json
{
  "type": "jdbc",
  "inputTable": "source_data",
  "config": {
    "url": "jdbc:mysql://localhost:3306/db",
    "username": "root",
    "password": "pwd",
    "table": "target_table",
    "batchSize": 100
  }
}
```

**sql 模式（具名占位符）：**
```json
{
  "type": "jdbc",
  "inputTable": "source_data",
  "config": {
    "url": "jdbc:mysql://localhost:3306/db",
    "username": "root",
    "password": "pwd",
    "sql": "INSERT INTO employee(last_name, email) VALUES(:lastName, :email)",
    "batchSize": 100
  }
}
```

**复杂 SQL 示例（upsert）：**
```json
{
  "config": {
    "sql": "INSERT INTO employee(id, name) VALUES(:id, :name) ON DUPLICATE KEY UPDATE name = VALUES(name)"
  }
}
```

## 实现要点

### 具名占位符解析

使用正则表达式 `:([a-zA-Z_][a-zA-Z0-9_]*)` 匹配占位符：

```java
private static final Pattern NAMED_PARAM_PATTERN = Pattern.compile(":([a-zA-Z_][a-zA-Z0-9_]*)");

public static ParsedSql parse(String sql) {
    List<String> paramNames = new ArrayList<>();
    Matcher matcher = NAMED_PARAM_PATTERN.matcher(sql);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
        paramNames.add(matcher.group(1));
        matcher.appendReplacement(sb, "?");
    }
    matcher.appendTail(sb);
    return new ParsedSql(sb.toString(), paramNames);
}
```

### 参数严格匹配

```java
for (int i = 0; i < paramNames.size(); i++) {
    String paramName = paramNames.get(i);
    Object value = row.getField(paramName);
    if (value == null && !row.getFieldNames(true).contains(paramName)) {
        throw new IllegalArgumentException("Row 中不存在字段: " + paramName);
    }
    statement.setObject(i + 1, value);
}
```

## 迁移影响

1. **模块名变更**：需要更新 `flink-etl-client/pom.xml` 中的依赖
2. **配置变更**：
   - `type: mysql` → `type: jdbc`
   - 删除 `writeMode` 配置
   - 需要 upsert 时使用 sql 自定义
3. **向后兼容**：不兼容，需要修改配置文件