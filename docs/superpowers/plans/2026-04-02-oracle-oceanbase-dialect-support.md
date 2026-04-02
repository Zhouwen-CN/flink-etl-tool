# Oracle/OceanBase Dialect 支持实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 JDBC Source 和 JDBC Sink 新增 dialect 配置支持，允许显式指定数据库方言，并新增 Oracle/OceanBase dialect 实现。

**Architecture:** 通过扩展 `JdbcDialects` 工厂类，新增按名称查找 Dialect 的能力。在 JDBC Source 和 Sink 的配置解析逻辑中，优先使用显式配置的 dialect 名称查找，未配置时继续使用 URL 自动匹配机制。新增 OracleDialect 类实现 Oracle/OceanBase 数据库的 SQL 语法特性。

**Tech Stack:** Java SPI（ServiceLoader）、@AutoService 注解、JUnit 5

---

## 文件结构

### 新增文件

1. **`flink-etl-core/src/main/java/com/etl/core/dialect/OracleDialect.java`**
   - 实现 Oracle/OceanBase 数据库方言
   - 使用双引号转义标识符
   - 实现 MERGE INTO 语法用于 upsert

2. **`flink-etl-core/src/test/java/com/etl/core/dialect/OracleDialectTest.java`**
   - OracleDialect 的单元测试
   - 测试 URL 匹配、标识符转义、SQL 生成

3. **`flink-etl-core/src/test/java/com/etl/core/dialect/JdbcDialectsTest.java`**
   - JdbcDialects 工厂类的测试
   - 测试按名称查找和按 URL 匹配逻辑

### 修改文件

1. **`flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialects.java`**
   - 在 `get()` 方法后新增 `getByName(String name)` 方法
   - 按名称查找 Dialect，找不到抛出异常并列出支持的类型

2. **`flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`**
   - 在构造函数第 33-37 行之间插入 dialect 配置读取逻辑（约 10 行）
   - 原代码使用 `JdbcDialects.get(url)`，修改为优先检查 `dialect` 配置参数

3. **`flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java`**
   - 在构造函数第 29-33 行之间插入 dialect 配置读取逻辑（约 10 行）
   - 类似 JdbcSource 的修改逻辑

4. **`PLUGINS.md`**
   - 第 29-41 行：JDBC Source 配置参数表格，新增 `dialect` 参数行
   - 第 100 行附近：新增注意事项，说明 `dialect` 参数用途
   - 第 542-552 行：JDBC Sink 配置参数表格，新增 `dialect` 参数行
   - 第 620 行附近：配置示例部分，新增 Oracle/OceanBase 示例
   - 第 634 行附近：多数据库支持表格，新增 Oracle 和 OceanBase 行

---

## Task 1: 扩展 JdbcDialects 工厂类

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialects.java:40-53`
- Create: `flink-etl-core/src/test/java/com/etl/core/dialect/JdbcDialectsTest.java`

- [ ] **Step 1: 编写 JdbcDialectsTest 测试类**

创建测试文件，测试新增的 `getByName` 方法：

```java
package com.etl.core.dialect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JdbcDialectsTest {

    @Test
    void testGetByName_mysql() {
        JdbcDialect dialect = JdbcDialects.getByName("mysql");
        assertNotNull(dialect);
        assertEquals("mysql", dialect.getName());
        assertTrue(dialect instanceof MySQLDialect);
    }

    @Test
    void testGetByName_postgresql() {
        JdbcDialect dialect = JdbcDialects.getByName("postgresql");
        assertNotNull(dialect);
        assertEquals("postgresql", dialect.getName());
        assertTrue(dialect instanceof PostgreSQLDialect);
    }

    @Test
    void testGetByName_invalidName() {
        assertThrows(IllegalArgumentException.class, () -> {
            JdbcDialects.getByName("invalid_db");
        }, "不支持的 dialect 名称应该抛出异常");
    }

    @Test
    void testGetByName_nullName() {
        assertThrows(IllegalArgumentException.class, () -> {
            JdbcDialects.getByName(null);
        }, "null 名称应该抛出异常");
    }

    @Test
    void testGetByUrl_mysql() {
        String url = "jdbc:mysql://localhost:3306/test";
        JdbcDialect dialect = JdbcDialects.get(url);
        assertNotNull(dialect);
        assertEquals("mysql", dialect.getName());
    }

    @Test
    void testGetByUrl_postgresql() {
        String url = "jdbc:postgresql://localhost:5432/test";
        JdbcDialect dialect = JdbcDialects.get(url);
        assertNotNull(dialect);
        assertEquals("postgresql", dialect.getName());
    }

    @Test
    void testGetByUrl_unsupportedUrl() {
        String url = "jdbc:unsupported://localhost/test";
        assertThrows(IllegalArgumentException.class, () -> {
            JdbcDialects.get(url);
        }, "不支持的数据库 URL 应该抛出异常");
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

运行: `mvn test -Dtest=JdbcDialectsTest -pl flink-etl-core`

预期: 测试失败，因为 `getByName` 方法不存在

- [ ] **Step 3: 实现 getByName 方法**

在 `JdbcDialects.java` 中新增方法：

```java
/**
 * 根据 Dialect 名称获取对应的 Dialect
 * @param name Dialect 名称，如 "mysql", "postgresql", "oracle"
 * @return 对应的 Dialect
 * @throws IllegalArgumentException 如果不支持的 dialect 名称
 */
public static JdbcDialect getByName(String name) {
    if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Dialect 名称不能为空");
    }

    for (JdbcDialect dialect : DIALECTS) {
        if (dialect.getName().equalsIgnoreCase(name)) {
            log.debug("名称 {} 匹配 Dialect: {}", name, dialect.getName());
            return dialect;
        }
    }

    throw new IllegalArgumentException("不支持的 Dialect 类型，名称: " + name +
        "。支持的类型: " + DIALECTS.stream().map(JdbcDialect::getName).collect(Collectors.joining(", ")));
}
```

- [ ] **Step 4: 运行测试验证通过**

运行: `mvn test -Dtest=JdbcDialectsTest -pl flink-etl-core`

预期: 所有测试通过

- [ ] **Step 5: 提交代码**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialects.java
git add flink-etl-core/src/test/java/com/etl/core/dialect/JdbcDialectsTest.java
git commit -m "feat: JdbcDialects 新增 getByName 方法支持显式指定 dialect"
```

---

## Task 2: 实现 OracleDialect 类

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/dialect/OracleDialect.java`
- Create: `flink-etl-core/src/test/java/com/etl/core/dialect/OracleDialectTest.java`

- [ ] **Step 1: 编写 OracleDialectTest 测试类**

```java
package com.etl.core.dialect;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class OracleDialectTest {

    private final OracleDialect dialect = new OracleDialect();

    @Test
    void testGetName() {
        assertEquals("oracle", dialect.getName());
    }

    @Test
    void testAcceptsUrl_oracle() {
        assertTrue(dialect.acceptsUrl("jdbc:oracle:thin:@localhost:1521:test"));
        assertTrue(dialect.acceptsUrl("jdbc:oracle:thin:@//localhost:1521/test"));
    }

    @Test
    void testAcceptsUrl_oceanbase() {
        // OceanBase Oracle 模式使用 oracle 驱动
        assertTrue(dialect.acceptsUrl("jdbc:oceanbase://localhost:2883/test"));
    }

    @Test
    void testAcceptsUrl_other() {
        assertFalse(dialect.acceptsUrl("jdbc:mysql://localhost:3306/test"));
        assertFalse(dialect.acceptsUrl("jdbc:postgresql://localhost:5432/test"));
        assertFalse(dialect.acceptsUrl(null));
    }

    @Test
    void testQuoteIdentifier() {
        assertEquals("\"name\"", dialect.quoteIdentifier("name"));
        assertEquals("\"table_name\"", dialect.quoteIdentifier("table_name"));
    }

    @Test
    void testGetInsertSql() {
        String sql = dialect.getInsertSql("user", new String[]{"id", "name", "email"});
        assertEquals("INSERT INTO \"user\" (\"id\", \"name\", \"email\") VALUES (?, ?, ?)", sql);
    }

    @Test
    void testGetUpsertSql() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Collections.singletonList("id"));
        // Oracle 使用 MERGE INTO 语法
        assertNotNull(sql);
        assertTrue(sql.contains("MERGE INTO"));
        assertTrue(sql.contains("\"user\""));
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE"));
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    @Test
    void testGetUpsertSqlWithCompositeKey() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Arrays.asList("id", "name"));
        assertNotNull(sql);
        assertTrue(sql.contains("MERGE INTO"));
        // 复合主键应该在 ON 条件中包含所有字段
        assertTrue(sql.contains("\"id\""));
        assertTrue(sql.contains("\"name\""));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

运行: `mvn test -Dtest=OracleDialectTest -pl flink-etl-core`

预期: 测试失败，因为 `OracleDialect` 类不存在

- [ ] **Step 3: 实现 OracleDialect 类**

```java
package com.etl.core.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Oracle/OceanBase 数据库方言
 * 支持 Oracle 和 OceanBase Oracle 模式
 */
@Slf4j
@AutoService(JdbcDialect.class)
public class OracleDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "oracle";
    }

    @Override
    public boolean acceptsUrl(String url) {
        if (url == null) {
            return false;
        }
        // 支持 Oracle JDBC URL
        if (url.contains(":oracle:")) {
            return true;
        }
        // 支持 OceanBase Oracle 模式（使用 oracle 驱动）
        if (url.contains(":oceanbase:")) {
            return true;
        }
        return false;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String getUpsertSql(String table, String[] columns, List<String> keyFields) {
        // Oracle 使用 MERGE INTO 语法
        String targetTable = quoteIdentifier(table);

        // 构建列列表
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        // 构建 VALUES 占位符
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.length, "?"));

        // 构建源数据查询（DUAL 表模拟）
        String sourceColumns = Arrays.stream(columns)
                .map(col -> "?" + " AS " + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        // 构建 ON 条件（主键匹配）
        Set<String> keyFieldSet = new HashSet<>(keyFields);
        String onClause = keyFields.stream()
                .map(key -> targetTable + "." + quoteIdentifier(key) + " = source." + quoteIdentifier(key))
                .collect(Collectors.joining(" AND "));

        // 构建 UPDATE 子句（非主键字段）
        String updateClause = Arrays.stream(columns)
                .filter(col -> !keyFieldSet.contains(col))
                .map(col -> quoteIdentifier(col) + " = source." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        // 构建 INSERT 子句
        String insertValues = Arrays.stream(columns)
                .map(col -> "source." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        return String.format(
                "MERGE INTO %s USING (SELECT %s FROM DUAL) source ON (%s) " +
                "WHEN MATCHED THEN UPDATE SET %s " +
                "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                targetTable, sourceColumns, onClause,
                updateClause, colList, insertValues
        );
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

运行: `mvn test -Dtest=OracleDialectTest -pl flink-etl-core`

预期: 所有测试通过

- [ ] **Step 5: 提交代码**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/OracleDialect.java
git add flink-etl-core/src/test/java/com/etl/core/dialect/OracleDialectTest.java
git commit -m "feat: 新增 OracleDialect 支持 Oracle/OceanBase 数据库"
```

---

## Task 3: 修改 JDBC Source 支持 dialect 配置

**Files:**
- Create: `flink-etl-source/flink-etl-source-jdbc/src/test/java/com/etl/source/jdbc/JdbcSourceDialectTest.java`
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 1: 编写 JdbcSourceDialectTest 测试类**

创建测试文件，测试 dialect 配置读取逻辑：

```java
package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.MySQLDialect;
import com.etl.core.dialect.OracleDialect;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JdbcSource dialect 配置测试
 */
public class JdbcSourceDialectTest {

    @Test
    public void testExplicitDialect_mysql() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:oceanbase://localhost:2883/test"); // OceanBase URL
        configMap.put("dialect", "mysql"); // 显式指定 mysql
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SourceConfig config = new SourceConfig();
        config.setConfig(configMap);

        JdbcSource source = new JdbcSource(config);
        // 应该使用显式配置的 mysql dialect，而不是从 URL 自动识别
        assertNotNull(source);
    }

    @Test
    public void testExplicitDialect_oracle() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test"); // MySQL URL
        configMap.put("dialect", "oracle"); // 显式指定 oracle
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SourceConfig config = new SourceConfig();
        config.setConfig(configMap);

        JdbcSource source = new JdbcSource(config);
        // 应该使用显式配置的 oracle dialect
        assertNotNull(source);
    }

    @Test
    public void testInvalidDialect() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("dialect", "invalid_db");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SourceConfig config = new SourceConfig();
        config.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSource(config);
        }, "不支持的 dialect 名称应该抛出异常");
    }

    @Test
    public void testAutoDetectByUrl() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        // 不配置 dialect，应该自动识别
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SourceConfig config = new SourceConfig();
        config.setConfig(configMap);

        JdbcSource source = new JdbcSource(config);
        assertNotNull(source);
    }

    @Test
    public void testAutoDetectUnsupportedUrl() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:unsupported://localhost/test");
        // 不配置 dialect，URL 无法识别
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SourceConfig config = new SourceConfig();
        config.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSource(config);
        }, "不支持的数据库 URL 应该抛出异常");
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

运行: `mvn test -Dtest=JdbcSourceDialectTest -pl flink-etl-source/flink-etl-source-jdbc`

预期: 测试失败，因为 dialect 配置读取逻辑不存在

- [ ] **Step 3: 修改 JdbcSource 构造函数**

在构造函数中新增 dialect 参数处理逻辑，插入到第 33-37 行之间：

```java
public JdbcSource(SourceConfig config) {
    super(config);
    String url = config.getString("url");
    Preconditions.checkNotNull(url, "url is null");

    // 新增：支持显式配置 dialect（约10行代码）
    JdbcDialect dialect;
    String dialectName = config.getString("dialect");
    if (dialectName != null && !dialectName.isEmpty()) {
        // 显式指定 dialect，直接按名称查找
        log.info("使用显式配置的 dialect: {}", dialectName);
        dialect = JdbcDialects.getByName(dialectName);
    } else {
        // 未配置 dialect，根据 URL 自动识别
        log.info("根据 URL 自动识别 dialect");
        dialect = JdbcDialects.get(url);
    }

    // 使用 Dialect 包装 URL（原代码）
    url = dialect.wrapUrl(url);

    // ... 后续代码保持不变
    String username = config.getString("username");
    String password = config.getString("password");

    String table = config.getString("table");
    String sql = config.getString("sql");

    String splitColumn = config.getString("splitColumn");
    SplitStrategy splitStrategy;

    if (splitColumn == null) {
        log.warn("未配置 splitColumn，将使用单分片全表扫描模式，无法并行读取。建议配置 splitColumn 以启用并行分片读取。");
        splitStrategy = SplitStrategy.FULL_TABLE_SCAN;
    } else {
        int jdbcType = SqlUtils.getColumnType(dialect, url, table, sql, splitColumn, username, password);
        splitStrategy = SplitStrategy.fromJdbcType(jdbcType);
        if (splitStrategy == null) {
            throw new IllegalArgumentException(
                    String.format("分片列 '%s' 的 JDBC 类型(%d)不支持分片。支持的类型: %s",
                            splitColumn, jdbcType, SplitStrategy.NUMERIC.getSupportedTypeNames()));
        }
        log.info("分片列 '{}' 使用策略: {}", splitColumn, splitStrategy.getDescription());
    }

    Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

    Integer queryTimeout = config.getInteger("queryTimeout");

    this.jdbcSourceConfig = JdbcSourceConfig.builder()
            .url(url)
            .username(username)
            .password(password)
            .table(table)
            .sql(sql)
            .splitColumn(splitColumn)
            .splitStrategy(splitStrategy)
            .batchSize(batchSize)
            .queryTimeout(queryTimeout)
            .dialect(dialect)
            .build();

    log.info("创建 JdbcSource: {}", this.jdbcSourceConfig);
}
```

- [ ] **Step 4: 运行测试验证通过**

运行: `mvn test -Dtest=JdbcSourceDialectTest -pl flink-etl-source/flink-etl-source-jdbc`

预期: 所有测试通过

- [ ] **Step 5: 提交代码**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/test/java/com/etl/source/jdbc/JdbcSourceDialectTest.java
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java
git commit -m "feat: JDBC Source 新增 dialect 配置支持并添加测试"
```

---

## Task 4: 修改 JDBC Sink 支持 dialect 配置

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkDialectTest.java`
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java`

- [ ] **Step 1: 编写 JdbcSinkDialectTest 测试类**

创建测试文件，测试 dialect 配置读取逻辑：

```java
package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JdbcSink dialect 配置测试
 */
public class JdbcSinkDialectTest {

    @Test
    public void testExplicitDialect_mysql() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:oceanbase://localhost:2883/test"); // OceanBase URL
        configMap.put("dialect", "mysql"); // 显式指定 mysql
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink);
    }

    @Test
    public void testExplicitDialect_oracle() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test"); // MySQL URL
        configMap.put("dialect", "oracle"); // 显式指定 oracle
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink);
    }

    @Test
    public void testInvalidDialect() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("dialect", "invalid_db");
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "不支持的 dialect 名称应该抛出异常");
    }

    @Test
    public void testAutoDetectByUrl() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        // 不配置 dialect，应该自动识别
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink);
    }

    @Test
    public void testAutoDetectUnsupportedUrl() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:unsupported://localhost/test");
        // 不配置 dialect，URL 无法识别
        configMap.put("username", "root");
        configMap.put("password", "password");
        configMap.put("table", "test_table");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        assertThrows(IllegalArgumentException.class, () -> {
            new JdbcSink(config);
        }, "不支持的数据库 URL 应该抛出异常");
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

运行: `mvn test -Dtest=JdbcSinkDialectTest -pl flink-etl-sink/flink-etl-sink-jdbc`

预期: 测试失败，因为 dialect 配置读取逻辑不存在

- [ ] **Step 3: 修改 JdbcSink 构造函数**

在构造函数中新增 dialect 参数处理逻辑，插入到第 29-33 行之间：

```java
public JdbcSink(SinkConfig config) {
    super(config);

    String url = Preconditions.checkNotNull(config.getString("url"), "url is null");
    String username = config.getString("username");
    String password = config.getString("password");

    // 新增：支持显式配置 dialect（约10行代码）
    JdbcDialect dialect;
    String dialectName = config.getString("dialect");
    if (dialectName != null && !dialectName.isEmpty()) {
        // 显式指定 dialect，直接按名称查找
        log.info("使用显式配置的 dialect: {}", dialectName);
        dialect = JdbcDialects.getByName(dialectName);
    } else {
        // 未配置 dialect，根据 URL 自动识别
        log.info("根据 URL 自动识别 dialect");
        dialect = JdbcDialects.get(url);
    }

    String table = config.getString("table");
    String sql = config.getString("sql");
    Preconditions.checkArgument(table != null || sql != null,
        "table 和 sql 必须配置其中一个");

    String modeStr = config.getString("mode", "INSERT");
    WriteMode mode = WriteMode.valueOf(modeStr.toUpperCase());

    List<String> keyFields = null;
    if (mode == WriteMode.UPSERT) {
        List<String> keyFieldsConfig = config.getList("keyFields");
        Preconditions.checkNotNull(keyFieldsConfig, "UPSERT 模式必须配置 keyFields");
        keyFields = keyFieldsConfig;
        log.info("JDBC Sink upsert 模式: table={}, keyFields={}", table, keyFields);
    } else {
        log.info("JDBC Sink insert 模式: table={}", table);
    }

    Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
    Preconditions.checkArgument(batchSize != null && batchSize > 0, "batchSize must be greater than 0");

    this.jdbcSinkConfig = JdbcSinkConfig.builder()
        .url(dialect.wrapUrl(url))
        .username(username)
        .password(password)
        .table(table)
        .sql(sql)
        .dialect(dialect)
        .mode(mode)
        .keyFields(keyFields)
        .batchSize(batchSize)
        .build();

    log.info("创建 JdbcSink: {}", this.jdbcSinkConfig);
}
```

- [ ] **Step 4: 运行测试验证通过**

运行: `mvn test -Dtest=JdbcSinkDialectTest -pl flink-etl-sink/flink-etl-sink-jdbc`

预期: 所有测试通过

- [ ] **Step 5: 提交代码**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkDialectTest.java
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java
git commit -m "feat: JDBC Sink 新增 dialect 配置支持并添加测试"
```

---

## Task 5: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md:29-41` (JDBC Source 配置参数表格)
- Modify: `PLUGINS.md:542-552` (JDBC Sink 配置参数表格)

- [ ] **Step 1: 更新 JDBC Source 配置参数表格**

在配置参数表格中新增 `dialect` 参数：

找到第 29 行开始的表格，修改为：

```markdown
| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL，格式：`jdbc:mysql://host:port/database` |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `dialect` | 否 | 自动识别 | 数据库方言，可选值：`mysql`、`postgresql`、`oracle`。不配置则根据 URL 自动识别 |
| `table` | 条件必填 | - | 表名。与 `sql` 二选一，优先 |
| `sql` | 条件必填 | - | 自定义查询 SQL。与 `table` 二选一 |
| `splitColumn` | 否 | - | 分片列名，支持数值类型（TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/DECIMAL）。不配置则使用单分片全表扫描 |
| `batchSize` | 否 | 100 | 批量读取大小 |
| `queryTimeout` | 否 | 无限制 | 查询超时时间（秒） |
| `schema` | 否 | 自动推断 | Schema 定义，不配置则从数据库元数据自动推断 |
```

在第 100 行附近添加说明：

```markdown
> **注意：**
> - 未配置 `splitColumn` 时将使用单分片全表扫描模式，无法并行读取数据
> - 对于大数据量表，建议配置 `splitColumn` 以启用并行分片读取
> - `splitColumn` 仅支持数值类型（TINYINT, SMALLINT, INT, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC），配置非数值类型列会报错
> - `dialect` 参数用于显式指定数据库类型，适用于 URL 无法正确识别数据库类型的场景（如 OceanBase）
```

- [ ] **Step 2: 更新 JDBC Sink 配置参数表格**

在第 542 行开始的表格中新增 `dialect` 参数：

```markdown
| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `dialect` | 否 | 自动识别 | 数据库方言，可选值：`mysql`、`postgresql`、`oracle`。不配置则根据 URL 自动识别 |
| `table` | 条件必填 | - | 目标表名。与 `sql` 二选一，优先 |
| `sql` | 条件必填 | - | 自定义 SQL，支持具名占位符 `:paramName` |
| `mode` | 否 | `insert` | 写入模式：`insert`（插入）或 `upsert`（存在则更新） |
| `keyFields` | upsert 必填 | - | Upsert 模式的主键/唯一键字段，数组格式 |
| `batchSize` | 否 | `100` | 批量写入大小 |
```

在配置示例后添加说明：

```markdown
> **注意：**
> - `dialect` 参数用于显式指定数据库类型，适用于 URL 无法正确识别数据库类型的场景（如 OceanBase）
> - 不同数据库的 upsert 语法不同：MySQL 使用 `ON DUPLICATE KEY UPDATE`，PostgreSQL 使用 `ON CONFLICT`，Oracle 使用 `MERGE INTO`
```

- [ ] **Step 3: 更新多数据库支持表格**

在第 634 行附近找到"多数据库支持"章节，添加 Oracle 和 OceanBase：

```markdown
#### 多数据库支持

JDBC Sink 自动识别数据库类型并使用对应的标识符转义：

| 数据库 | 标识符转义 | URL 示例 |
|--------|-----------|----------|
| MySQL | `` `name` `` | `jdbc:mysql://host:3306/db` |
| PostgreSQL | `"name"` | `jdbc:postgresql://host:5432/db` |
| Oracle | `"name"` | `jdbc:oracle:thin:@host:1521:SID` |
| OceanBase (Oracle 模式) | `"name"` | `jdbc:oceanbase://host:2883/db` |
| SQLite | `"name"` | `jdbc:sqlite:/path/to/db` |
| SQL Server | `[name]` | `jdbc:sqlserver://host:1433;databaseName=db` |
```

- [ ] **Step 4: 添加 Oracle/OceanBase 配置示例**

在 JDBC Source 配置示例部分（约第 98 行之后）添加：

```markdown
**Oracle 数据库配置：**

```json
{
  "source": {
    "type": "jdbc",
    "outputTable": "users",
    "config": {
      "url": "jdbc:oracle:thin:@localhost:1521:orcl",
      "username": "system",
      "password": "oracle",
      "table": "USERS",
      "dialect": "oracle",
      "splitColumn": "ID",
      "batchSize": 1000
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
      "splitColumn": "ID",
      "batchSize": 1000
    }
  }
}
```

> **说明：** OceanBase Oracle 模式使用 Oracle 兼容驱动，URL 中包含 `oceanbase` 但需显式配置 `dialect: "oracle"`
```

在 JDBC Sink 配置示例部分（约第 616 行之后）添加：

```markdown
**Oracle 数据库 Upsert 配置：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:oracle:thin:@localhost:1521:orcl",
      "username": "system",
      "password": "oracle",
      "table": "TARGET_TABLE",
      "dialect": "oracle",
      "mode": "upsert",
      "keyFields": ["ID"],
      "batchSize": 100
    }
  }
}
```

**OceanBase Oracle 模式配置：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:oceanbase://localhost:2883/test",
      "username": "admin",
      "password": "password",
      "table": "TARGET_TABLE",
      "dialect": "oracle",
      "mode": "upsert",
      "keyFields": ["ID"],
      "batchSize": 100
    }
  }
}
```

> **说明：** Oracle 和 OceanBase Oracle 模式使用 `MERGE INTO` 语法实现 upsert
```

- [ ] **Step 5: 提交文档更新**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md 添加 dialect 配置说明和 Oracle/OceanBase 支持"
```

---

## Task 6: 验证完整功能

**Files:**
- Create: `docs/examples/oracle-dialect-example.json` (示例配置文件)

- [ ] **Step 1: 编译整个项目**

运行: `mvn clean compile`

预期: 编译成功，无错误

- [ ] **Step 2: 运行所有单元测试**

运行: `mvn test`

预期: 所有测试通过，包括：
- `MySQLDialectTest`
- `PostgreSQLDialectTest`
- `OracleDialectTest`
- `JdbcDialectsTest`
- `JdbcSourceDialectTest`
- `JdbcSinkDialectTest`

- [ ] **Step 3: 验证 SPI 加载**

运行测试验证 OracleDialect 已正确注册到 SPI：

```bash
mvn test -Dtest=JdbcDialectsTest#testGetByName_invalidName -pl flink-etl-core
```

预期: 测试通过，错误信息中包含支持的类型列表（mysql, postgresql, oracle）

- [ ] **Step 4: 创建 Oracle/OceanBase dialect 示例配置文件**

创建示例配置文件，展示如何使用 dialect 参数：

`docs/examples/oracle-dialect-example.json`:

```json
{
  "job": {
    "name": "oracle-dialect-example",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "jdbc",
      "outputTable": "source_data",
      "config": {
        "url": "jdbc:oracle:thin:@localhost:1521:orcl",
        "dialect": "oracle",
        "username": "system",
        "password": "oracle",
        "table": "SOURCE_TABLE",
        "splitColumn": "ID",
        "batchSize": 1000
      }
    }
  ],
  "transforms": [
    {
      "type": "sql",
      "outputTable": "transformed_data",
      "config": {
        "sql": "SELECT * FROM source_data WHERE id > 0"
      }
    }
  ],
  "sinks": [
    {
      "type": "jdbc",
      "inputTable": "transformed_data",
      "config": {
        "url": "jdbc:oracle:thin:@localhost:1521:orcl",
        "dialect": "oracle",
        "username": "system",
        "password": "oracle",
        "table": "TARGET_TABLE",
        "mode": "upsert",
        "keyFields": ["ID"],
        "batchSize": 100
      }
    }
  ]
}
```

`docs/examples/oceanbase-dialect-example.json`:

```json
{
  "job": {
    "name": "oceanbase-dialect-example",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "jdbc",
      "outputTable": "source_data",
      "config": {
        "url": "jdbc:oceanbase://localhost:2883/test",
        "dialect": "oracle",
        "username": "admin",
        "password": "password",
        "table": "SOURCE_TABLE",
        "splitColumn": "ID",
        "batchSize": 1000
      }
    }
  ],
  "sinks": [
    {
      "type": "jdbc",
      "inputTable": "source_data",
      "config": {
        "url": "jdbc:oceanbase://localhost:2883/test",
        "dialect": "oracle",
        "username": "admin",
        "password": "password",
        "table": "TARGET_TABLE",
        "mode": "upsert",
        "keyFields": ["ID"],
        "batchSize": 100
      }
    }
  ]
}
```

- [ ] **Step 5: 提交示例配置文件**

```bash
git add docs/examples/oracle-dialect-example.json
git add docs/examples/oceanbase-dialect-example.json
git commit -m "docs: 添加 Oracle/OceanBase dialect 示例配置"
```

- [ ] **Step 6: 打包验证**

运行: `mvn clean package -DskipTests`

预期: 打包成功，生成可执行 JAR 文件

- [ ] **Step 7: 最终提交（如有遗漏）**

检查是否有未提交的文件：

```bash
git status
```

如果有未提交文件，补充提交：

```bash
git add -A
git commit -m "feat: 完成 Oracle/OceanBase dialect 支持"
```

---

## 验收标准

- ✅ `JdbcDialects.getByName("mysql")` 返回 MySQLDialect 实例
- ✅ `JdbcDialects.getByName("postgresql")` 返回 PostgreSQLDialect 实例
- ✅ `JdbcDialects.getByName("oracle")` 返回 OracleDialect 实例
- ✅ `JdbcDialects.getByName("invalid")` 抛出 IllegalArgumentException，错误信息包含支持的类型列表
- ✅ JDBC Source 配置 `dialect: "oracle"` 后使用 OracleDialect
- ✅ JDBC Sink 配置 `dialect: "oracle"` 后使用 OracleDialect
- ✅ 未配置 dialect 时，继续使用 URL 自动识别逻辑
- ✅ 配置错误的 dialect 名称时，抛出异常并列出支持的类型
- ✅ 未配置 dialect 且 URL 无法识别时，抛出异常
- ✅ OracleDialect 生成正确的 MERGE INTO upsert SQL
- ✅ OracleDialect 支持 Oracle 和 OceanBase URL 匹配
- ✅ 所有单元测试通过（包括 dialect 配置测试）
- ✅ PLUGINS.md 文档已更新，包含 dialect 配置说明和 Oracle/OceanBase 支持
- ✅ 示例配置文件已创建，展示如何使用 dialect 参数

---

## 实现要点总结

### Dialect 配置机制

1. **优先级**：显式配置 `dialect` 参数 > URL 自动匹配
2. **错误处理**：
   - 配置了不支持的 dialect 名称 → 抛出异常，列出所有支持的类型
   - 未配置 dialect 但 URL 无法识别 → 抛出异常
3. **日志记录**：记录使用的是显式配置还是自动识别

### Oracle/OceanBase 方言特性

1. **标识符转义**：使用双引号 `"identifier"`（与 PostgreSQL 相同）
2. **UPSERT 语法**：使用 `MERGE INTO` 语句（Oracle 特有）
   - `MERGE INTO table USING (SELECT ... FROM DUAL) source ON (condition)`
   - `WHEN MATCHED THEN UPDATE SET ...`
   - `WHEN NOT MATCHED THEN INSERT ... VALUES ...`
3. **URL 匹配**：支持 `jdbc:oracle:*` 和 `jdbc:oceanbase:*`
4. **OceanBase 兼容**：OceanBase Oracle 模式完全兼容 Oracle SQL 语法，直接复用 OracleDialect

### 测试覆盖

- **JdbcDialectsTest**：
  - 按名称查找测试（mysql、postgresql、oracle、invalid、null）
  - 按 URL 匹配测试
  - 错误处理测试

- **OracleDialectTest**：
  - URL 匹配测试（包括 Oracle 和 OceanBase URL）
  - 标识符转义测试
  - INSERT SQL 生成测试
  - UPSERT SQL 生成测试（单主键、复合主键）

- **JdbcSourceDialectTest**：
  - 显式配置 dialect 参数测试
  - 未配置 dialect 时自动识别测试
  - 错误的 dialect 名称测试
  - 无法识别的 URL 测试

- **JdbcSinkDialectTest**：
  - 显式配置 dialect 参数测试
  - 未配置 dialect 时自动识别测试
  - 错误的 dialect 名称测试
  - 无法识别的 URL 测试

### 关键设计决策

1. **配置优先级**：显式配置 `dialect` 参数优先于 URL 自动匹配，确保用户可以覆盖自动检测逻辑
2. **错误提示友好**：当配置了不支持的 dialect 名称时，错误消息列出所有支持的类型，帮助用户快速修正
3. **向后兼容**：未配置 dialect 参数时保持原有行为（URL 自动匹配），不影响现有配置
4. **OceanBase 支持**：OceanBase Oracle 模式使用 Oracle 兼容驱动，通过 acceptsUrl 同时匹配 `:oracle:` 和 `:oceanbase:`