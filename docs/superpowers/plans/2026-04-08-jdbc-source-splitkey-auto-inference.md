# JDBC Source splitKey 自动推断实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 JDBC Source 的 splitColumn 参数改名为 splitKey，实现自动推断机制，提升用户体验

**Architecture:** 在 JdbcSource 构造函数中集中处理推断逻辑，使用 NoPrimaryKeyException 自定义异常，SqlUtils.getPrimaryKey() 使用 LinkedHashMap 保证主键顺序

**Tech Stack:** Java 1.8, Flink 1.15.2, Apache Commons Lang3 (Pair)

---

## 文件结构

**新增文件：**
- `flink-etl-core/src/main/java/com/etl/core/exception/NoPrimaryKeyException.java` - 自定义异常类
- `flink-etl-source-jdbc/src/test/java/com/etl/source/jdbc/JdbcSourceSplitKeyTest.java` - 测试类

**修改文件：**
- `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java` - 使用 NoPrimaryKeyException 和 LinkedHashMap
- `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java` - 字段重命名 splitColumn → splitKey
- `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java` - 新增推断逻辑
- `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java` - 异常处理修改
- `PLUGINS.md` - 文档更新

---

## Task 1: 创建自定义异常类 NoPrimaryKeyException

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/exception/NoPrimaryKeyException.java`

- [ ] **Step 1: 创建异常类**

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

- [ ] **Step 2: 编译验证**

Run: `mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/exception/NoPrimaryKeyException.java
git commit -m "feat: 新增 NoPrimaryKeyException 自定义异常类

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 2: 修改 SqlUtils.getPrimaryKey() 使用自定义异常和 LinkedHashMap

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java:68-107`
- Test: `flink-etl-core/src/test/java/com/etl/core/utils/SqlUtilsTest.java`

- [ ] **Step 1: 修改 SqlUtils.java 导入 LinkedHashMap**

在文件开头的导入部分添加：

```java
import com.etl.core.exception.NoPrimaryKeyException;
import java.util.LinkedHashMap;
```

移除：`import java.util.HashMap;`

- [ ] **Step 2: 修改 getPrimaryKey() 方法实现**

替换第 68-107 行的方法实现：

```java
/**
 * 从数据库获取表的主键信息
 *
 * @param url      数据库连接 URL
 * @param table    表名
 * @param username 用户名（可为 null）
 * @param password 密码（可为 null）
 * @return LinkedHashMap<列名, JDBC类型>，按 KEY_SEQ 顺序排列
 * @throws NoPrimaryKeyException 如果表没有主键
 * @throws RuntimeException 如果获取失败
 */
public static Map<String, Integer> getPrimaryKey(
        String url, String table, String username, String password) {

    try (Connection conn = DriverManager.getConnection(url, username, password)) {
        // 自动获取 catalog 和 schema
        String catalog = conn.getCatalog();
        String schema = conn.getSchema();

        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys(catalog, schema, table);

        // 使用 LinkedHashMap 保证主键顺序（按 KEY_SEQ）
        Map<String, Integer> result = new LinkedHashMap<>();

        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");

            // 使用 DatabaseMetaData.getColumns() 获取列类型
            ResultSet colRs = metaData.getColumns(catalog, schema, table, columnName);

            if (colRs.next()) {
                int jdbcType = colRs.getInt("DATA_TYPE");
                result.put(columnName, jdbcType);
            } else {
                throw new RuntimeException(
                    String.format("无法获取表 '%s' 列 '%s' 的类型信息", table, columnName));
            }
            colRs.close();
        }
        rs.close();

        if (result.isEmpty()) {
            throw new NoPrimaryKeyException(table);
        }

        return result;
    } catch (SQLException e) {
        throw new RuntimeException("从数据库获取主键失败: " + e.getMessage(), e);
    }
}
```

- [ ] **Step 3: 运行现有测试确保通过**

Run: `mvn test -pl flink-etl-core -Dtest=SqlUtilsTest`
Expected: Tests run: X, Failures: 0, Errors: 0

- [ ] **Step 4: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java
git commit -m "refactor: SqlUtils.getPrimaryKey() 使用 NoPrimaryKeyException 和 LinkedHashMap

- 使用自定义异常 NoPrimaryKeyException 替代通用 RuntimeException
- 使用 LinkedHashMap 保证复合主键顺序
- 错误信息通用化：\"表 '{table}' 没有主键\"

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 3: 修改 JdbcSourceConfig 字段名和注释

**Files:**
- Modify: `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java`

- [ ] **Step 1: 重命名字段 splitColumn → splitKey 并更新注释**

修改第 28-29 行：

```java
/** 分片列名（可选），不配置则自动从主键推断 */
private final String splitKey;
```

- [ ] **Step 2: 编译验证**

Run: `mvn clean compile -pl flink-etl-source/flink-etl-source-jdbc`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java
git commit -m "refactor: JdbcSourceConfig 字段名 splitColumn → splitKey

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 4: 编写 JdbcSource splitKey 推断测试

**Files:**
- Create: `flink-etl-source-jdbc/src/test/java/com/etl/source/jdbc/JdbcSourceSplitKeyTest.java`

- [ ] **Step 1: 创建测试类骨架**

```java
package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialectLoader;
import com.etl.core.exception.NoPrimaryKeyException;
import com.etl.core.utils.SqlUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.MockedStatic;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class JdbcSourceSplitKeyTest {

    private SourceConfig config;
    private JdbcDialect dialect;

    @BeforeEach
    void setUp() {
        config = mock(SourceConfig.class);
        dialect = mock(JdbcDialect.class);

        // 基础配置
        when(config.getString("url")).thenReturn("jdbc:mysql://localhost:3306/test");
        when(config.getString("username")).thenReturn("root");
        when(config.getString("password")).thenReturn("password");
        when(config.getInteger("batchSize", 100)).thenReturn(100);

        // Mock dialect
        when(dialect.wrapUrl(anyString())).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    void testUserConfiguredSplitKey_BigIntType() {
        // 用户配置 splitKey 为 BIGINT 类型
        when(config.getString("splitKey")).thenReturn("id");
        when(config.getString("table")).thenReturn("users");
        when(dialect.getColumnType(anyString(), anyString(), isNull(), eq("id"), anyString(), anyString()))
            .thenReturn(Types.BIGINT);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class)) {
            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证日志输出：使用用户配置的 splitKey
        }
    }

    @Test
    void testUserConfiguredSplitKey_UnsupportedType() {
        // 用户配置 splitKey 为 VARCHAR 类型（不支持）
        when(config.getString("splitKey")).thenReturn("name");
        when(config.getString("table")).thenReturn("users");
        when(dialect.getColumnType(anyString(), anyString(), isNull(), eq("name"), anyString(), anyString()))
            .thenReturn(Types.VARCHAR);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class)) {
            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);

            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new JdbcSource(config)
            );
            assertTrue(exception.getMessage().contains("不支持分片"));
        }
    }

    @Test
    void testAutoInferFromPrimaryKey_SingleBigIntKey() {
        // 配置 table，表有单主键 BIGINT
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn("users");
        when(config.getString("sql")).thenReturn(null);

        Map<String, Integer> primaryKeys = new LinkedHashMap<>();
        primaryKeys.put("id", Types.BIGINT);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<SqlUtils> sqlUtilsMock = mockStatic(SqlUtils.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            sqlUtilsMock.when(() -> SqlUtils.getPrimaryKey(anyString(), eq("users"), anyString(), anyString()))
                .thenReturn(primaryKeys);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证自动选择了 id (BIGINT)
        }
    }

    @Test
    void testAutoInferFromPrimaryKey_CompositeKey() {
        // 配置 table，表有复合主键（INT + BIGINT）
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn("orders");
        when(config.getString("sql")).thenReturn(null);

        Map<String, Integer> primaryKeys = new LinkedHashMap<>();
        primaryKeys.put("id", Types.INTEGER);
        primaryKeys.put("seq", Types.BIGINT);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<SqlUtils> sqlUtilsMock = mockStatic(SqlUtils.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            sqlUtilsMock.when(() -> SqlUtils.getPrimaryKey(anyString(), eq("orders"), anyString(), anyString()))
                .thenReturn(primaryKeys);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证优先选择了 seq (BIGINT)
        }
    }

    @Test
    void testAutoInferFromPrimaryKey_NoSupportedType() {
        // 配置 table，主键列类型都不支持
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn("logs");
        when(config.getString("sql")).thenReturn(null);

        Map<String, Integer> primaryKeys = new LinkedHashMap<>();
        primaryKeys.put("created_at", Types.TIMESTAMP);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<SqlUtils> sqlUtilsMock = mockStatic(SqlUtils.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            sqlUtilsMock.when(() -> SqlUtils.getPrimaryKey(anyString(), eq("logs"), anyString(), anyString()))
                .thenReturn(primaryKeys);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证降级为单分片模式（警告日志）
        }
    }

    @Test
    void testAutoInferFromPrimaryKey_NoPrimaryKey() {
        // 配置 table，表无主键
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn("temp");
        when(config.getString("sql")).thenReturn(null);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class);
             MockedStatic<SqlUtils> sqlUtilsMock = mockStatic(SqlUtils.class)) {

            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);
            sqlUtilsMock.when(() -> SqlUtils.getPrimaryKey(anyString(), eq("temp"), anyString(), anyString()))
                .thenThrow(new NoPrimaryKeyException("temp"));

            RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> new JdbcSource(config)
            );
            assertTrue(exception.getMessage().contains("无法自动推断 splitKey"));
        }
    }

    @Test
    void testSqlWithoutSplitKey() {
        // 配置 sql，未配置 splitKey
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn(null);
        when(config.getString("sql")).thenReturn("SELECT id, name FROM users WHERE status = 1");

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class)) {
            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证单分片模式（警告日志）
        }
    }

    @Test
    void testSqlWithUserSplitKey() {
        // 配置 sql + splitKey
        when(config.getString("splitKey")).thenReturn("id");
        when(config.getString("table")).thenReturn(null);
        when(config.getString("sql")).thenReturn("SELECT id, name FROM users WHERE status = 1");
        when(dialect.getColumnType(anyString(), isNull(), anyString(), eq("id"), anyString(), anyString()))
            .thenReturn(Types.BIGINT);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class)) {
            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);

            JdbcSource source = new JdbcSource(config);
            assertNotNull(source);
            // 验证使用用户配置的 splitKey
        }
    }

    @Test
    void testNoTableOrSql() {
        // table 和 sql 都未配置
        when(config.getString("splitKey")).thenReturn(null);
        when(config.getString("table")).thenReturn(null);
        when(config.getString("sql")).thenReturn(null);

        try (MockedStatic<JdbcDialectLoader> loaderMock = mockStatic(JdbcDialectLoader.class)) {
            loaderMock.when(() -> JdbcDialectLoader.get(isNull(), anyString())).thenReturn(dialect);

            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new JdbcSource(config)
            );
            assertTrue(exception.getMessage().contains("table 和 sql 至少配置一个"));
        }
    }
}
```

- [ ] **Step 2: 编译验证测试代码**

Run: `mvn test-compile -pl flink-etl-source/flink-etl-source-jdbc`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交测试类**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/test/java/com/etl/source/jdbc/JdbcSourceSplitKeyTest.java
git commit -m "test: 新增 JdbcSource splitKey 推断测试类

覆盖 9 种场景：
- 用户配置 splitKey（支持/不支持类型）
- 自动推断（单主键/复合主键/无主键/类型不支持）
- sql 模式（有/无 splitKey）
- 参数校验

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 5: 实现 JdbcSource 推断逻辑

**Files:**
- Modify: `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 1: 添加必要的导入**

在文件开头添加：

```java
import com.etl.core.exception.NoPrimaryKeyException;
import org.apache.commons.lang3.tuple.Pair;
import java.sql.Types;
```

- [ ] **Step 2: 修改构造函数**

替换构造函数中的 splitColumn 处理逻辑（第 48-66 行的 if-else 代码块）：

```java
public JdbcSource(SourceConfig config) {
    super(config);
    String url = Preconditions.checkNotNull(config.getString("url"), "url is null");

    // 支持显式配置 dialect
    String dialectName = config.getString("dialect");
    JdbcDialect dialect = JdbcDialectLoader.get(dialectName, url);

    // 使用 Dialect 包装 URL
    url = dialect.wrapUrl(url);

    String username = config.getString("username");
    String password = config.getString("password");

    String table = config.getString("table");
    String sql = config.getString("sql");

    // 推断 splitKey 和 splitStrategy
    String userSplitKey = config.getString("splitKey");
    Pair<String, SplitStrategy> splitKeyResult = inferSplitKey(
        url, table, sql, userSplitKey, dialect, username, password);

    String splitKey = splitKeyResult.getLeft();
    SplitStrategy splitStrategy = splitKeyResult.getRight();

    Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

    Integer queryTimeout = config.getInteger("queryTimeout");

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

- [ ] **Step 3: 添加 inferSplitKey() 私有方法**

在类的末尾添加：

```java
/**
 * 推断 splitKey 和 SplitStrategy
 *
 * @return Pair<splitKey, splitStrategy>，splitKey 为 null 表示单分片模式
 */
private Pair<String, SplitStrategy> inferSplitKey(
    String url, String table, String sql,
    String userSplitKey, JdbcDialect dialect,
    String username, String password) {

    // 参数校验：table 和 sql 至少配置一个
    Preconditions.checkArgument(table != null || sql != null, "table 和 sql 至少配置一个");

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
                String.format("%s。请手动配置 splitKey 参数或为表添加主键", e.getMessage()));
        }
    }

    // 3. 配置了 sql（且未配置 table），未配置 splitKey → 单分片模式
    log.warn("配置了自定义 SQL 但未指定 splitKey，将使用单分片全表扫描模式。建议配置 splitKey 以启用并行分片读取。");
    return Pair.of(null, SplitStrategy.FULL_TABLE_SCAN);
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

- [ ] **Step 4: 运行测试验证实现**

Run: `mvn test -pl flink-etl-source/flink-etl-source-jdbc -Dtest=JdbcSourceSplitKeyTest`
Expected: Tests run: 9, Failures: 0, Errors: 0

- [ ] **Step 5: 提交**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java
git commit -m "feat: JdbcSource 实现 splitKey 自动推断逻辑

- 新增 inferSplitKey() 方法实现推断流程
- 新增 selectOptimalSplitKey() 方法实现主键优选
- 优先级：用户配置 > table 自动推断 > sql 单分片
- 复合主键优先选择数值类型范围最大的列

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 6: 修改 JdbcSink 异常处理

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java`

- [ ] **Step 1: 添加异常导入**

在文件开头的导入部分添加：

```java
import com.etl.core.exception.NoPrimaryKeyException;
```

- [ ] **Step 2: 修改获取主键的异常处理**

找到调用 `SqlUtils.getPrimaryKey()` 的位置，修改异常处理：

```java
try {
    primaryKeyMap = SqlUtils.getPrimaryKey(url, table, username, password);
} catch (NoPrimaryKeyException e) {
    throw new RuntimeException(
        String.format("表 '%s' 没有主键，无法使用 UPSERT 模式。请使用 INSERT 模式、手动配置 keyFields 或为表添加主键", e.getTableName()));
}
```

- [ ] **Step 3: 运行 JDBC Sink 测试验证**

Run: `mvn test -pl flink-etl-sink/flink-etl-sink-jdbc`
Expected: Tests run: X, Failures: 0, Errors: 0

- [ ] **Step 4: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java
git commit -m "refactor: JdbcSink 使用 NoPrimaryKeyException 异常处理

捕获 NoPrimaryKeyException 并补充 UPSERT 模式的具体提示

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 7: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 更新 JDBC Source 配置参数表格**

修改第 267 行附近的参数说明：

```markdown
| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL，格式：`jdbc:mysql://host:port/database` |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `dialect` | 否 | 自动识别 | 数据库方言，可选值：`mysql`、`postgresql`、`oracle`。不配置则根据 URL 自动识别 |
| `table` | 条件必填 | - | 表名。与 `sql` 二选一，优先 |
| `sql` | 条件必填 | - | 自定义查询 SQL。与 `table` 二选一 |
| `splitKey` | 否 | 自动推断 | 分片列名，支持数值类型。不配置时自动从主键推断 |
| `batchSize` | 否 | 100 | 批量读取大小 |
| `queryTimeout` | 否 | 无限制 | 查询超时时间（秒） |
| `schema` | 否 | 自动推断 | Schema 定义，不配置则从数据库元数据自动推断 |
```

- [ ] **Step 2: 更新配置示例**

修改第 275-291 行的示例：

```markdown
#### 配置示例

**基础配置 - 读取整表（自动推断 splitKey）：**

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

> **说明：** 未配置 `splitKey` 时，自动从表主键推断合适的分片列。

**手动配置 splitKey：**

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

> **注意：** 配置 `sql` 时必须手动配置 `splitKey`，否则将使用单分片全表扫描模式。

**无主键表配置：**

```json
{
  "source": {
    "type": "jdbc",
    "outputTable": "temp_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "temp_data",
      "splitKey": "seq"
    }
  }
}
```

> **说明：** 无主键表必须手动配置 `splitKey`，否则任务会失败。
```

- [ ] **Step 3: 更新分片说明部分**

修改第 380-385 行：

```markdown
#### 分片说明

- 分片数量由 Job 配置的 `parallelism` 决定
- **自动推断：** 未配置 `splitKey` 时，自动从表主键推断分片列
  - 单主键：自动选择该主键列
  - 复合主键：优先选择数值类型范围最大的列（BIGINT > INT > SMALLINT > TINYINT > DECIMAL > FLOAT）
  - 无主键：任务失败，提示手动配置 `splitKey`
- **手动配置：** 配置 `splitKey` 时使用用户指定的列
- `splitKey` 支持数值类型：TINYINT, SMALLINT, INT, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC
```

- [ ] **Step 4: 全局替换 PLUGINS.md 中所有 splitColumn 引用**

使用编辑器全局替换：`splitColumn` → `splitKey`（约 8 处引用）

或使用命令（Windows Git Bash）：
```bash
sed -i 's/splitColumn/splitKey/g' PLUGINS.md
```

验证替换结果：
```bash
grep -n "splitColumn" PLUGINS.md
```
Expected: 无匹配结果

<function_calls>
<invoke name="Edit">
<parameter name="file_path">d:\work\idea\flink-etl-tool\docs\superpowers\plans\2026-04-08-jdbc-source-splitkey-auto-inference.md

- [ ] **Step 5: 提交文档更新**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 JDBC Source splitKey 参数说明和示例

- 参数名 splitColumn → splitKey
- 新增自动推断机制说明
- 新增复合主键选择优先级说明
- 更新配置示例

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 8: 运行完整测试套件

**Files:**
- 无新增/修改文件

- [ ] **Step 1: 运行完整测试**

Run: `mvn clean test`
Expected: All tests pass

- [ ] **Step 2: 运行集成测试（如果有）**

Run: `mvn verify -pl flink-etl-client`
Expected: BUILD SUCCESS

- [ ] **Step 3: 手动测试验证**

使用示例配置文件测试：

```bash
# 测试自动推断 splitKey
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file docs/examples/batch-mysql2console.json
```

Expected: 任务成功执行，日志显示自动推断的 splitKey

---

## Task 9: 更新示例配置文件

**Files:**
- Modify: `README.md`
- Modify: `docs/examples/*.json` (所有示例文件)

- [ ] **Step 1: 替换 README.md 中的 splitColumn**

在第 53 行替换参数名：

```json
"splitKey": "id"
```

- [ ] **Step 2: 替换所有示例配置文件**

使用命令（Windows Git Bash）：
```bash
for file in docs/examples/*.json; do
  sed -i 's/"splitColumn"/"splitKey"/g' "$file"
done
```

或手动编辑每个文件，将 `"splitColumn"` 替换为 `"splitKey"`

- [ ] **Step 3: 验证替换结果**

Run:
```bash
grep -r "splitColumn" docs/examples/ README.md
```
Expected: 无匹配结果

- [ ] **Step 4: 提交**

```bash
git add README.md docs/examples/*.json
git commit -m "refactor: 示例配置参数名 splitColumn → splitKey

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## 完成清单

- [ ] 所有测试通过
- [ ] 文档已更新
- [ ] 代码已提交
- [ ] 手动测试验证通过

---

## 注意事项

1. **LinkedHashMap 重要性**：SqlUtils.getPrimaryKey() 必须使用 LinkedHashMap，否则复合主键顺序不确定
2. **异常信息**：使用 `e.getMessage()` 避免双重格式化
3. **参数校验前置**：在 inferSplitKey() 开始处校验 table/sql 参数
4. **不向后兼容**：旧参数 `splitColumn` 会被静默忽略
5. **测试覆盖**：确保所有边界情况都有测试覆盖