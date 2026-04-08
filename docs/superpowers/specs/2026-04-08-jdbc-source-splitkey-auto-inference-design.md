# JDBC Source splitKey 自动推断设计文档

## 需求概述

将 JDBC Source 的 `splitColumn` 参数改名为 `splitKey`，并实现可选配置的自动推断机制，提升用户体验。

**变更内容：**

1. 参数名称：`splitColumn` → `splitKey`（不向后兼容）
2. splitKey 改为可选配置
3. 未配置 splitKey 时，自动从数据库主键推断合适的分片列

**自动推断逻辑：**

- 配置 table：自动从表主键推断 splitKey
- 配置 sql：splitKey 必须用户手动配置，否则单分片模式

---

## 配置参数变更

### 参数定义

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `splitKey` | 否 | 自动推断 | 分片列名，支持数值类型。不配置时自动从主键推断 |

**参数语义：**
- 支持数值类型：BIGINT, INTEGER, SMALLINT, TINYINT, DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE
- 未配置时触发自动推断机制
- 不向后兼容，旧参数 `splitColumn` 被静默忽略

### 配置示例

**手动配置 splitKey：**

```json
{
  "sources": [{
    "type": "jdbc",
    "outputTable": "users",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users",
      "splitKey": "id"
    }
  }]
}
```

**自动推断 splitKey：**

```json
{
  "sources": [{
    "type": "jdbc",
    "outputTable": "users",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users"
      // 未配置 splitKey，自动从主键推断
    }
  }]
}
```

---

## 自动推断逻辑设计

### 推断流程

```
1. 用户手动配置了 splitKey？
   ├─ 是 → 验证类型，使用用户配置的 splitKey
   │   ├─ 类型支持 → 使用该列切分
   │   └─ 类型不支持 → 抛出 IllegalArgumentException
   │
   └─ 否 → 进入自动推断流程

2. 配置了 table？
   ├─ 是 → 自动从数据库获取表的主键
   │   ├─ 有主键 → 遍历主键 Map，选择最优类型
   │   │   ├─ 找到可用类型 → 选择最优类型（BIGINT > INT > ...）
   │   │   └─ 未找到可用类型 → 警告 + 单分片全表扫描
   │   │
   │   └─ 表无主键 → 抛出异常，提示手动配置 splitKey
   │
   └─ 否 → 进入 sql 模式

3. 配置了 sql（且未配置 table）？
   ├─ 是 → 警告 + 单分片全表扫描模式
   │
   └─ 否 → 抛出 IllegalArgumentException（table 和 sql 至少配置一个）
```

### 数值类型优先级

当表有多个数值类型的主键列时，按以下优先级选择（从高到低）：

1. **BIGINT** - 范围最大（-2^63 到 2^63-1），最适合大数据量分片
2. **INTEGER (INT)** - 范围适中（-2^31 到 2^31-1），常用主键类型
3. **SMALLINT** - 范围较小（-32768 到 32767）
4. **TINYINT** - 范围最小（-128 到 127）
5. **DECIMAL/NUMERIC** - 精确数值类型
6. **FLOAT/REAL/DOUBLE** - 浮点类型

**示例场景：**
- 主键是 `(user_id: BIGINT, order_seq: INT)` → 选择 `user_id` (BIGINT)
- 主键是 `(seq: SMALLINT, id: INT)` → 选择 `id` (INT)

### 警告信息设计

| 场景 | 警告信息 |
|------|---------|
| 配置了 sql 但未配置 splitKey | `"配置了自定义 SQL 但未指定 splitKey，将使用单分片全表扫描模式。建议配置 splitKey 以启用并行分片读取。"` |
| 配置了 table 但表无主键 | `"表 '{table}' 没有主键，无法自动推断 splitKey。请手动配置 splitKey 参数或为表添加主键"` |
| 配置了 table，有主键但无可用类型 | `"表 '{table}' 的主键列均不支持分片类型（支持的类型: BIGINT, INT, ...）。将使用单分片全表扫描模式。建议手动配置 splitKey 参数指定数值类型的列。"` |

---

## 实现方案

### 方案选择：在 JdbcSource 构造函数中集中处理

**理由：**
- 符合现有设计模式（JDBC Sink 主键推断也在构造函数处理）
- 逻辑集中，易于理解和维护
- 构造时确定策略，避免运行时重复推断
- 测试简单，只需测试构造函数行为

---

## 代码实现设计

### 新增自定义异常类

**文件位置：** `flink-etl-core/src/main/java/com/etl/core/exception/NoPrimaryKeyException.java`

```java
package com.etl.core.exception;

/**
 * 表无主键异常
 * 用于 JDBC Source 自动推断 splitKey 和 JDBC Sink UPSERT 模式
 */
public class NoPrimaryKeyException extends RuntimeException {

    private final String tableName;

    public NoPrimaryKeyException(String tableName) {
        super(String.format("表 '%s' 没有主键", tableName));
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
```

### SqlUtils.getPrimaryKey() 修改

**修改内容：**
- 抛出 `NoPrimaryKeyException` 而非通用 RuntimeException
- 错误信息通用化：`"表 '{table}' 没有主键"`

```java
public static Map<String, Integer> getPrimaryKey(
        String url, String table, String username, String password) {

    try (Connection conn = DriverManager.getConnection(url, username, password)) {
        // ... 现有的获取主键逻辑 ...

        if (result.isEmpty()) {
            throw new NoPrimaryKeyException(table);  // 使用自定义异常
        }

        return result;
    } catch (SQLException e) {
        throw new RuntimeException("从数据库获取主键失败: " + e.getMessage(), e);
    }
}
```

### JdbcSourceConfig.java 修改

**变更内容：**
- 字段名：`splitColumn` → `splitKey`
- 字段注释更新："分片列名（可选），不配置则自动从主键推断"

```java
@Getter
@Builder
public class JdbcSourceConfig implements Serializable {
    // ... 其他字段 ...

    /** 分片列名（可选），不配置则自动从主键推断 */
    private final String splitKey;
    /** 分片策略，根据 splitKey 是否配置决定 */
    private final SplitStrategy splitStrategy;
}
```

### JdbcSource.java 实现

**完整构造函数流程：**

```java
import org.apache.commons.lang3.tuple.Pair;

public JdbcSource(SourceConfig config) {
    super(config);

    // 1. 解析基础配置参数
    String url = Preconditions.checkNotNull(config.getString("url"), "url is null");
    String dialectName = config.getString("dialect");
    JdbcDialect dialect = JdbcDialectLoader.get(dialectName, url);
    url = dialect.wrapUrl(url);

    String username = config.getString("username");
    String password = config.getString("password");
    String table = config.getString("table");
    String sql = config.getString("sql");
    Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
    Integer queryTimeout = config.getInteger("queryTimeout");

    // 2. 推断 splitKey 和 splitStrategy
    String userSplitKey = config.getString("splitKey");
    Pair<String, SplitStrategy> splitKeyResult = inferSplitKey(
        url, table, sql, userSplitKey, dialect, username, password);

    String splitKey = splitKeyResult.getLeft();
    SplitStrategy splitStrategy = splitKeyResult.getRight();

    // 3. 构建 JdbcSourceConfig
    this.jdbcSourceConfig = JdbcSourceConfig.builder()
        .url(url)
        .username(username)
        .password(password)
        .table(table)
        .sql(sql)
        .splitKey(splitKey)
        .splitStrategy(splitStrategy)
        .batchSize(batchSize)
        .queryTimeout(queryTimeout)
        .dialect(dialect)
        .build();

    log.info("创建 JdbcSource: {}", this.jdbcSourceConfig);
}
```

**新增私有方法：**

```java
/**
 * 推断 splitKey 和 SplitStrategy
 */
private Pair<String, SplitStrategy> inferSplitKey(
    String url, String table, String sql,
    String userSplitKey, JdbcDialect dialect,
    String username, String password) {

    // 1. 用户手动配置了 splitKey
    if (userSplitKey != null) {
        int jdbcType = dialect.getColumnType(url, table, sql, userSplitKey, username, password);
        SplitStrategy strategy = SplitStrategy.fromJdbcType(jdbcType);
        if (strategy == null) {
            throw new IllegalArgumentException(
                String.format("分片列 '%s' 的 JDBC 类型(%d)不支持分片。支持的类型: %s",
                    userSplitKey, jdbcType, SplitStrategy.NUMERIC.getSupportedTypeNames()));
        }
        log.info("使用用户配置的 splitKey '{}'，策略: {}", userSplitKey, strategy.getDescription());
        return Pair.of(userSplitKey, strategy);
    }

    // 2. 配置了 table，未配置 splitKey → 自动从主键推断
    if (table != null) {
        try {
            Map<String, Integer> primaryKeys = SqlUtils.getPrimaryKey(url, table, username, password);
            String optimalKey = selectOptimalSplitKey(primaryKeys);

            if (optimalKey == null) {
                log.warn("表 '{}' 的主键列均不支持分片类型（支持的类型: {}）。将使用单分片全表扫描模式。建议手动配置 splitKey 参数指定数值类型的列。",
                    table, SplitStrategy.NUMERIC.getSupportedTypeNames());
                return Pair.of(null, SplitStrategy.FULL_TABLE_SCAN);
            }

            int jdbcType = primaryKeys.get(optimalKey);
            SplitStrategy strategy = SplitStrategy.fromJdbcType(jdbcType);
            log.info("自动推断 splitKey '{}'（主键列），策略: {}", optimalKey, strategy.getDescription());
            return Pair.of(optimalKey, strategy);

        } catch (NoPrimaryKeyException e) {
            throw new RuntimeException(
                String.format("表 '%s' 没有主键，无法自动推断 splitKey。请手动配置 splitKey 参数或为表添加主键", e.getTableName()));
        }
    }

    // 3. 配置了 sql（且未配置 table），未配置 splitKey → 单分片模式
    if (sql != null) {
        log.warn("配置了自定义 SQL 但未指定 splitKey，将使用单分片全表扫描模式。建议配置 splitKey 以启用并行分片读取。");
        return Pair.of(null, SplitStrategy.FULL_TABLE_SCAN);
    }

    // 4. table 和 sql 都未配置 → 报错
    throw new IllegalArgumentException("table 和 sql 至少配置一个");
}

/**
 * 从主键列中选择最优的可用分片列
 * 优先级：BIGINT > INTEGER > SMALLINT > TINYINT > DECIMAL/NUMERIC > FLOAT/REAL/DOUBLE
 */
private String selectOptimalSplitKey(Map<String, Integer> primaryKeys) {
    // 定义类型优先级
    int[] typePriority = {
        Types.BIGINT,
        Types.INTEGER,
        Types.SMALLINT,
        Types.TINYINT,
        Types.DECIMAL,
        Types.NUMERIC,
        Types.FLOAT,
        Types.REAL,
        Types.DOUBLE
    };

    // 按优先级遍历，找到第一个匹配的类型
    for (int preferredType : typePriority) {
        for (Map.Entry<String, Integer> entry : primaryKeys.entrySet()) {
            if (entry.getValue() == preferredType) {
                return entry.getKey();
            }
        }
    }

    return null; // 无可用类型
}
```

### JdbcSink.java 异常处理修改

**修改内容：** 捕获 `NoPrimaryKeyException` 并补充 UPSERT 模式的具体提示

```java
try {
    primaryKeyMap = SqlUtils.getPrimaryKey(url, table, username, password);
} catch (NoPrimaryKeyException e) {
    throw new RuntimeException(
        String.format("表 '%s' 没有主键，无法使用 UPSERT 模式。请使用 INSERT 模式、手动配置 keyFields 或为表添加主键", e.getTableName()));
}
```

---

## 测试设计

### 测试文件位置

`flink-etl-source-jdbc/src/test/java/com/etl/source/jdbc/JdbcSourceSplitKeyTest.java`

### 测试场景覆盖

| 场景 | 输入 | 预期结果 |
|------|------|---------|
| 用户手动配置 splitKey | `splitKey: "id"` (BIGINT) | 使用 "id"，策略为 NUMERIC |
| 用户配置 splitKey 但类型不支持 | `splitKey: "name"` (VARCHAR) | 抛出 `IllegalArgumentException` |
| 配置 table + 有主键 (BIGINT) | `table: "users"` (主键: id BIGINT) | 自动选择 "id"，策略为 NUMERIC |
| 配置 table + 复合主键 | `table: "orders"` (主键: id INT, seq BIGINT) | 自动选择 "seq" (BIGINT优先) |
| 配置 table + 主键类型不支持 | `table: "logs"` (主键: created_at TIMESTAMP) | 警告 + 单分片模式 |
| 配置 table + 表无主键 | `table: "temp"` (无主键) | 抛出异常，提示手动配置 |
| 配置 sql + 未配置 splitKey | `sql: "SELECT..."` | 警告 + 单分片模式 |
| 配置 sql + 配置 splitKey | `sql + splitKey: "id"` | 使用 "id"，策略为 NUMERIC |
| table 和 sql 都未配置 | 无 | 抛出 `IllegalArgumentException` |

### 测试类设计

```java
class JdbcSourceSplitKeyTest {

    @Test
    void testUserConfiguredSplitKey_BigIntType() {
        // 用户配置 splitKey 为 BIGINT 类型
        // 预期：使用用户配置的 splitKey
    }

    @Test
    void testUserConfiguredSplitKey_UnsupportedType() {
        // 用户配置 splitKey 为 VARCHAR 类型
        // 预期：抛出 IllegalArgumentException
    }

    @Test
    void testAutoInferFromPrimaryKey_SingleBigIntKey() {
        // 配置 table，表有单主键 BIGINT
        // 预期：自动选择该主键列
    }

    @Test
    void testAutoInferFromPrimaryKey_CompositeKey() {
        // 配置 table，表有复合主键（INT + BIGINT）
        // 预期：优先选择 BIGINT 类型的列
    }

    @Test
    void testAutoInferFromPrimaryKey_NoSupportedType() {
        // 配置 table，主键列类型都不支持
        // 预期：警告 + 单分片模式
    }

    @Test
    void testAutoInferFromPrimaryKey_NoPrimaryKey() {
        // 配置 table，表无主键
        // 预期：抛出异常
    }

    @Test
    void testSqlWithoutSplitKey() {
        // 配置 sql，未配置 splitKey
        // 预期：警告 + 单分片模式
    }

    @Test
    void testSqlWithUserSplitKey() {
        // 配置 sql + splitKey
        // 预期：使用用户配置的 splitKey
    }

    @Test
    void testNoTableOrSql() {
        // table 和 sql 都未配置
        // 预期：抛出 IllegalArgumentException
    }
}
```

### Mock 策略

- Mock `JdbcDialect.getColumnType()` 返回模拟的 JDBC 类型
- Mock `SqlUtils.getPrimaryKey()` 返回模拟的主键 Map
- 使用 H2 内存数据库测试真实场景

---

## 文档更新和影响范围

### PLUGINS.md 更新内容

1. **JDBC Source 配置参数表格更新：**
   - 参数名：`splitColumn` → `splitKey`
   - 说明："不配置则自动从主键推断"

2. **配置示例更新：**
   - 添加自动推断示例
   - 移除所有 `splitColumn` 引用

3. **说明文档新增：**
   - 自动推断机制说明
   - 复合主键选择优先级说明
   - 数值类型支持列表

### 影响文件清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `JdbcSourceConfig.java` | 重命名 | `splitColumn` → `splitKey` |
| `JdbcSource.java` | 新增逻辑 | 推断逻辑 + 2个私有方法 |
| `JdbcSplitEnumerator.java` | 无变更 | 使用 config 中的 splitKey |
| `JdbcSplitHelper.java` | 无变更 | 参数名已通用化 |
| `SplitStrategy.java` | 无变更 | 策略逻辑不变 |
| `SqlUtils.java` | 异常修改 | 使用 NoPrimaryKeyException |
| `JdbcSink.java` | 异常处理修改 | 捕获 NoPrimaryKeyException |
| `NoPrimaryKeyException.java` | 新增 | 自定义异常类 |
| `PLUGINS.md` | 文档更新 | 参数说明 + 示例 |
| `JdbcSourceSplitKeyTest.java` | 新增 | 测试类 |

---

## 关键决策记录

1. **参数名称变更：** `splitColumn` → `splitKey`，不向后兼容
2. **复合主键选择：** 优先选择数值类型范围最大的列（BIGINT > INT > ...）
3. **异常处理：** 自定义 NoPrimaryKeyException，通用化错误信息
4. **实现方案：** 在 JdbcSource 构造函数中集中处理，符合现有设计模式
5. **推断优先级：**
   - 用户配置 splitKey（最高）
   - 配置 table → 自动推断
   - 配置 sql → 单分片模式
   - 都未配置 → 报错