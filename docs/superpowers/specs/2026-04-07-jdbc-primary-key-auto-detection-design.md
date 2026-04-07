# JDBC Sink 自动获取主键设计

**日期**: 2026-04-07
**状态**: 已批准

## 背景

当前 JdbcSink 在 UPSERT 模式下需要用户手动配置 `keyFields` 参数，用户体验较差：
- 用户需要额外查询数据库主键信息
- 配置容易出错（列名拼写错误、复合主键遗漏等）
- 配置文件冗长

**目标**: JdbcSink 在 UPSERT 模式下自动从数据库获取主键信息，删除 `keyFields` 配置项。

## 设计方案

### 1. SqlUtils 新增方法

**方法签名**:
```java
public static LinkedHashMap<String, Integer> getPrimaryKey(
    String url, String table, String username, String password)
```

**返回值**: `LinkedHashMap<String, Integer>`
- Key: 主键列名
- Value: JDBC 类型常量（来自 `java.sql.Types`)
- 顺序: 按 KEY_SEQ 排序（复合主键的定义顺序）

**参数说明**:
- `url`: 数据库连接 URL
- `table`: 表名（物理表，不支持 SQL 查询结果）
- `username`: 用户名（可为 null）
- `password`: 密码（可为 null）

**实现逻辑**:

1. 建立数据库连接
2. 自动获取 catalog 和 schema（适配不同数据库）:
   - MySQL: `catalog = db_name`, `schema = null`
   - PostgreSQL: `catalog = db_name`, `schema = "public"`
   - Oracle: `catalog = null`, `schema = username`
   - SQL Server: `catalog = db_name`, `schema = "dbo"`
3. 调用 `DatabaseMetaData.getPrimaryKeys(catalog, schema, table)`
4. 按 KEY_SEQ 排序并收集主键列名
5. 对每个主键列调用现有 `getColumnType()` 方法获取 JDBC 类型
6. 构建 `LinkedHashMap` 保证顺序
7. 主键不存在时抛 `RuntimeException`

**异常处理**:
- 连接失败：抛 `RuntimeException("从数据库获取主键失败: ...")`
- 主键不存在：抛 `RuntimeException("表 'xxx' 没有主键，无法使用 UPSERT 模式")`

### 2. JdbcSink 构造函数改动

**原有逻辑**:
```java
if (mode == WriteMode.UPSERT) {
    List<String> keyFieldsConfig = config.getList("keyFields");
    Preconditions.checkNotNull(keyFieldsConfig, "UPSERT 模式必须配置 keyFields");
    keyFields = keyFieldsConfig;
}
```

**新逻辑**:
```java
if (mode == WriteMode.UPSERT) {
    // UPSERT 模式必须配置 table，不能配置 sql
    Preconditions.checkNotNull(table,
        "UPSERT 模式必须配置 table（不能使用 sql），因为需要从数据库获取主键信息");

    // 自动获取主键
    LinkedHashMap<String, Integer> pkInfo =
        SqlUtils.getPrimaryKey(url, table, username, password);
    keyFields = new ArrayList<>(pkInfo.keySet());

    log.info("UPSERT 模式自动获取主键: table={}, keyFields={}", table, keyFields);
}
```

### 3. 配置项变更

**删除的配置项**:
- `keyFields`: 不再支持手动配置

**新的约束条件**:
- UPSERT 模式必须配置 `table`，不能配置 `sql`
- UPSERT 模式的表必须有主键（否则抛异常）

**INSERT 模式不受影响**: 仍然支持 `table` 或 `sql` 配置。

### 4. 数据流转

```
用户配置 (UPSERT 模式)
  ↓
配置项: url, table, username, password, mode=UPSERT
  ↓
JdbcSink 构造函数
  ↓
调用 SqlUtils.getPrimaryKey(url, table, username, password)
  ↓
Connection.getCatalog() + Connection.getSchema()
  ↓
DatabaseMetaData.getPrimaryKeys(catalog, schema, table)
  ↓
ResultSet (COLUMN_NAME, KEY_SEQ)
  ↓
按 KEY_SEQ 排序 → LinkedHashMap<列名, JDBC类型>
  ↓
keyFields = ArrayList(pkInfo.keySet())
  ↓
JdbcSinkWriter 使用 keyFields 构建 UPSERT SQL
```

### 5. 复合主键处理

**场景**: 表有多个主键列（例如 `PRIMARY KEY (id, name)`）

**处理逻辑**:
- 返回 `LinkedHashMap` 包含所有主键列，顺序为 KEY_SEQ 定义顺序
- `keyFields` 包含所有主键列名
- JdbcSinkWriter 生成的 UPSERT SQL 使用所有主键列作为条件：
  ```sql
  INSERT INTO table (...) VALUES (...) ON DUPLICATE KEY UPDATE ...
  -- 或
  MERGE INTO table USING ... ON (id=? AND name=?)
  ```

### 6. 错误场景处理

| 场景 | 异常类型 | 异常信息 |
|------|---------|---------|
| UPSERT 模式配置 sql | IllegalArgumentException | "UPSERT 模式必须配置 table（不能使用 sql），因为需要从数据库获取主键信息" |
| 表没有主键 | RuntimeException | "表 'xxx' 没有主键，无法使用 UPSERT 模式。请使用 INSERT 模式或为表添加主键" |
| 连接失败 | RuntimeException | "从数据库获取主键失败: [具体错误]" |
| 主键列类型获取失败 | RuntimeException | "获取列 'xxx' 的类型失败: [具体错误]"（来自 `getColumnType()`） |

### 7. 文档更新

**PLUGINS.md 变更**:
- 删除 `keyFields` 配置项说明
- 新增 UPSERT 模式约束说明：必须配置 table、表必须有主键
- 更新示例配置（删除 keyFields）

**示例配置变更**:
```json
{
  "type": "jdbc",
  "inputTable": "result_table",
  "config": {
    "url": "jdbc:mysql://localhost:3306/target_db",
    "username": "root",
    "password": "secret",
    "table": "target_table",
    "mode": "UPSERT",
    "batchSize": 100
  }
}
```

**删除的配置**:
```json
// 旧配置（已废弃）
"keyFields": ["id"]  // 不再需要
```

## 实现范围

### 新增文件
- 无

### 修改文件
1. `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java`
   - 新增 `getPrimaryKey()` 方法

2. `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java`
   - 修改构造函数，自动获取主键
   - 新增 UPSERT 模式 table/sql 校验

3. `PLUGINS.md`
   - 删除 `keyFields` 配置说明
   - 新增 UPSERT 模式约束说明

### 不涉及文件
- `JdbcSinkConfig.java`: `keyFields` 字段保留（SinkWriter 需要）
- `JdbcSinkWriter.java`: 无改动（仍使用 `keyFields` 字段）
- `JdbcSource.java`: 不涉及本次改动（后续扩展）

## 测试策略

### 单元测试

**SqlUtils.getPrimaryKey() 测试**:
- 正常场景：单主键表
- 正常场景：复合主键表
- 异常场景：表没有主键
- 异常场景：表不存在
- 异常场景：连接失败
- 测试 KEY_SEQ 顺序正确性

**JdbcSink 构造函数测试**:
- UPSERT 模式 + table 配置：成功获取主键
- UPSERT 模式 + sql 配置：抛异常
- INSERT 模式 + sql 配置：不受影响

### 集成测试

使用 H2 内存数据库：
- 创建带主键的表，验证 UPSERT 成功
- 创建无主键的表，验证 UPSERT 抛异常

## 后续扩展（不在本次范围）

**JdbcSource 自动 splitColumn**:
- 未配置 splitColumn 时，自动使用主键第一列
- 主键不存在时，要求用户手动配置 splitColumn
- 需要额外处理复合主键场景（选择哪一列）

**扩展方向**:
1. 新增 `SqlUtils.getPrimaryKey()` 方法（本次实现）
2. JdbcSource 构造函数增加自动 splitColumn 逻辑
3. 复合主键处理策略（使用第一列或组合分片）

## 设计决策记录

### Q1: 为什么不支持手动配置 keyFields？

**决策**: 完全删除 `keyFields` 配置项。

**理由**:
- 减少用户配置负担
- 避免配置错误（列名错误、遗漏列）
- 主键信息是确定的，无需用户干预
- 简化配置逻辑，减少出错可能

### Q2: 为什么 UPSERT 不支持 sql 配置？

**决策**: UPSERT 模式必须配置 table，不支持 sql。

**理由**:
- 主键信息只存在于物理表，SQL 查询结果没有主键概念
- 如果支持 sql，无法获取主键信息
- UPSERT 本质上是表的更新操作，应该针对物理表

**替代方案**: 用户需要自定义 SQL 时，使用 INSERT 模式 + 自定义 SQL。

### Q3: 为什么使用 LinkedHashMap？

**决策**: 返回 `LinkedHashMap<String, Integer>`。

**理由**:
- 复合主键需要保持 KEY_SEQ 顺序
- 不同数据库的主键列顺序可能影响 UPSERT SQL 生成
- HashMap 不保证顺序，不适合复合主键场景

### Q4: 为什么不通过 dialect 参数？

**决策**: 方法签名不包含 `JdbcDialect` 参数。

**理由**:
- DatabaseMetaData API 已经处理不同数据库的兼容性
- Connection.getCatalog() 和 Connection.getSchema() 自动适配
- 方法签名与现有 `inferRowType()` 风格一致
- 简化调用方代码

## 风险评估

### 低风险
- DatabaseMetaData.getPrimaryKeys() 是标准 JDBC API，所有数据库驱动都支持
- Connection.getCatalog() 和 Connection.getSchema() 是 Java 1.7+ 标准方法
- 现有 `getColumnType()` 方法已验证可靠性

### 需关注
- Oracle 数据库的 schema 获取（依赖 Connection.getSchema()，需测试）
- 复合主键的 UPSERT SQL 生成（由 dialect 处理，无需改动）

### 无风险
- INSERT 模式不受影响（向后兼容）
- JdbcSinkWriter 无改动（使用现有 keyFields 字段）