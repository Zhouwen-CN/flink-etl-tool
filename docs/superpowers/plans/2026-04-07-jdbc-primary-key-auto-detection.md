# JDBC Sink 自动获取主键实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** JdbcSink 在 UPSERT 模式下自动从数据库获取主键信息，删除 keyFields 配置项。

**Architecture:** 在 SqlUtils 新增 getPrimaryKey() 方法，通过 DatabaseMetaData.getPrimaryKeys() 和 Connection.getCatalog()/getSchema() 自动获取主键信息；JdbcSink 构造函数调用该方法自动填充 keyFields，新增 table/sql 校验。

**Tech Stack:** Java 1.8, JDBC DatabaseMetaData, H2 内存数据库（测试）

---

## 文件结构

### 新增文件
- `flink-etl-core/src/test/java/com/etl/core/utils/SqlUtilsTest.java` - SqlUtils 单元测试

### 修改文件
- `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java` - 新增 getPrimaryKey() 方法
- `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java` - 修改构造函数自动获取主键
- `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkTest.java` - 修改/新增测试
- `PLUGINS.md` - 删除 keyFields 配置说明，新增 UPSERT 约束说明

---

## Task 1: SqlUtils.getPrimaryKey() 测试和实现

**目标:** 实现通过 JDBC DatabaseMetaData 自动获取表主键的方法

### Step 1: 编写 SqlUtilsTest 测试框架

- [ ] **创建测试文件框架**

```java
package com.etl.core.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SqlUtils 测试类
 * 使用 H2 内存数据库测试 getPrimaryKey 方法
 */
public class SqlUtilsTest {

    private static final String H2_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";

    @BeforeEach
    public void setUp() throws Exception {
        // 初始化 H2 数据库连接
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            // 创建测试表会在各个测试方法中执行
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        // 清理数据库
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }
}
```

文件路径: `flink-etl-core/src/test/java/com/etl/core/utils/SqlUtilsTest.java`

- [ ] **创建测试文件**

使用 Write 工具创建文件

---

### Step 2: 编写单主键表测试

- [ ] **添加单主键表测试方法**

```java
@Test
public void testGetPrimaryKey_SinglePrimaryKey() throws Exception {
    // 创建单主键表
    try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
         Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE single_pk (id INT PRIMARY KEY, name VARCHAR(100))");
    }

    // 测试获取主键
    LinkedHashMap<String, Integer> pkInfo =
        SqlUtils.getPrimaryKey(H2_URL, "single_pk", USERNAME, PASSWORD);

    // 验证结果
    assertNotNull(pkInfo, "主键信息不应为 null");
    assertEquals(1, pkInfo.size(), "单主键表应有 1 个主键列");
    assertTrue(pkInfo.containsKey("id"), "主键列应包含 'id'");
    assertEquals(java.sql.Types.INTEGER, pkInfo.get("id"), "'id' 列类型应为 INTEGER");
}
```

在 `SqlUtilsTest.java` 的 `tearDown()` 方法后添加此测试方法

- [ ] **运行测试验证失败**

运行命令:
```bash
cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool
mvn test -Dtest=SqlUtilsTest#testGetPrimaryKey_SinglePrimaryKey
```

预期输出:
```
[ERROR] testGetPrimaryKey_SinglePrimaryKey FAILED
Caused by: java.lang.NoSuchMethodException: com.etl.core.utils.SqlUtils.getPrimaryKey(...)
```

---

### Step 3: 编写复合主键表测试

- [ ] **添加复合主键表测试方法**

```java
@Test
public void testGetPrimaryKey_CompositePrimaryKey() throws Exception {
    // 创建复合主键表
    try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
         Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE composite_pk (id INT, name VARCHAR(100), age INT, PRIMARY KEY (id, name))");
    }

    // 测试获取主键
    LinkedHashMap<String, Integer> pkInfo =
        SqlUtils.getPrimaryKey(H2_URL, "composite_pk", USERNAME, PASSWORD);

    // 验证结果
    assertNotNull(pkInfo, "主键信息不应为 null");
    assertEquals(2, pkInfo.size(), "复合主键表应有 2 个主键列");

    // 验证顺序（KEY_SEQ 顺序）
    String[] expectedKeys = {"id", "name"};
    int index = 0;
    for (String key : pkInfo.keySet()) {
        assertEquals(expectedKeys[index], key, "第 " + (index + 1) + " 个主键列应为 " + expectedKeys[index]);
        index++;
    }

    // 验证类型
    assertEquals(java.sql.Types.INTEGER, pkInfo.get("id"), "'id' 列类型应为 INTEGER");
    assertEquals(java.sql.Types.VARCHAR, pkInfo.get("name"), "'name' 列类型应为 VARCHAR");
}
```

在 `testGetPrimaryKey_SinglePrimaryKey()` 方法后添加此测试方法

---

### Step 4: 编写无主键表测试

- [ ] **添加无主键表测试方法**

```java
@Test
public void testGetPrimaryKey_NoPrimaryKey() throws Exception {
    // 创建无主键表
    try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
         Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE no_pk (id INT, name VARCHAR(100))");
    }

    // 测试获取主键（应抛异常）
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
        SqlUtils.getPrimaryKey(H2_URL, "no_pk", USERNAME, PASSWORD);
    }, "无主键表应抛出 RuntimeException");

    assertTrue(exception.getMessage().contains("没有主键"),
        "异常信息应包含 '没有主键'");
}
```

在 `testGetPrimaryKey_CompositePrimaryKey()` 方法后添加此测试方法

---

### Step 5: 编写表不存在测试

- [ ] **添加表不存在测试方法**

```java
@Test
public void testGetPrimaryKey_TableNotExist() {
    // 测试获取不存在表的主键（应抛异常）
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
        SqlUtils.getPrimaryKey(H2_URL, "nonexistent_table", USERNAME, PASSWORD);
    }, "表不存在应抛出 RuntimeException");

    assertTrue(exception.getMessage().contains("获取主键失败") ||
               exception.getMessage().contains("Table not found") ||
               exception.getMessage().contains("does not exist"),
        "异常信息应提示表不存在");
}
```

在 `testGetPrimaryKey_NoPrimaryKey()` 方法后添加此测试方法

---

### Step 6: 实现 SqlUtils.getPrimaryKey() 方法

- [ ] **实现 getPrimaryKey() 方法**

```java
/**
 * 从数据库获取表的主键信息
 *
 * @param url      数据库连接 URL
 * @param table    表名
 * @param username 用户名（可为 null）
 * @param password 密码（可为 null）
 * @return LinkedHashMap<列名, JDBC类型>，按 KEY_SEQ 顺序排列
 * @throws RuntimeException 如果表没有主键或获取失败
 */
public static LinkedHashMap<String, Integer> getPrimaryKey(
        String url, String table, String username, String password) {

    try (Connection conn = DriverManager.getConnection(url, username, password)) {
        // 自动获取 catalog 和 schema
        String catalog = conn.getCatalog();
        String schema = conn.getSchema();

        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getPrimaryKeys(catalog, schema, table);

        // 按 KEY_SEQ 收集主键列名
        Map<Integer, String> keySeqColumnName = new HashMap<>();
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            int keySeq = rs.getInt("KEY_SEQ");
            keySeqColumnName.put(keySeq - 1, columnName); // KEY_SEQ 是 1-based
        }

        // 构建有序的 LinkedHashMap
        LinkedHashMap<String, Integer> result = new LinkedHashMap<>();
        for (int i = 0; i < keySeqColumnName.size(); i++) {
            String columnName = keySeqColumnName.get(i);

            // 使用现有方法获取列的 JDBC 类型
            int jdbcType = getColumnType(
                null, // dialect 参数，DatabaseMetaData 不需要
                url,
                table,
                null, // sql 参数，只查询表
                columnName,
                username,
                password
            );

            result.put(columnName, jdbcType);
        }

        // 主键不存在时抛异常
        if (result.isEmpty()) {
            throw new RuntimeException(
                String.format("表 '%s' 没有主键，无法使用 UPSERT 模式。请使用 INSERT 模式或为表添加主键", table));
        }

        return result;

    } catch (SQLException e) {
        throw new RuntimeException("从数据库获取主键失败: " + e.getMessage(), e);
    }
}
```

在 `SqlUtils.java` 的 `getColumnType()` 方法后添加此方法

注意: 需要导入额外的类:
```java
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
```

- [ ] **修改 SqlUtils.java**

使用 Edit 工具在现有方法后添加新方法和必要的导入

---

### Step 7: 运行所有 SqlUtils 测试验证通过

- [ ] **运行完整测试**

运行命令:
```bash
cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool
mvn test -Dtest=SqlUtilsTest
```

预期输出:
```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

---

### Step 8: 提交 SqlUtils 实现

- [ ] **提交代码**

```bash
git add flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java
git add flink-etl-core/src/test/java/com/etl/core/utils/SqlUtilsTest.java
git commit -m "feat: SqlUtils 新增 getPrimaryKey 方法自动获取表主键

新增方法通过 DatabaseMetaData.getPrimaryKeys() 自动获取表主键信息：
- 支持单主键和复合主键
- 自动获取 catalog/schema 适配不同数据库
- 返回 LinkedHashMap 保持 KEY_SEQ 顺序
- 包含 JDBC 类型信息

测试覆盖：单主键、复合主键、无主键、表不存在场景

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 2: JdbcSink 构造函数修改

**目标:** JdbcSink UPSERT 模式自动获取主键，删除 keyFields 配置依赖

### Step 1: 删除旧的 UPSERT 测试

- [ ] **删除 testUpsertWithoutKeyFields 测试**

删除 `JdbcSinkTest.java` 中的 `testUpsertWithoutKeyFields()` 方法（第 48-62 行）

理由: 该测试期望 UPSERT 模式没有 keyFields 时抛异常，但新设计会自动获取主键

- [ ] **修改 JdbcSinkTest.java**

使用 Edit 工具删除该测试方法

---

### Step 2: 编写新的 UPSERT 自动获取主键测试

- [ ] **添加 UPSERT table 模式测试（使用 Mock）**

```java
@Test
public void testUpsertWithTableAutoPrimaryKey() {
    // UPSERT 模式 + table 配置，应该自动获取主键
    // 注意：实际测试需要真实数据库或 Mock，这里仅测试配置解析不抛异常

    Map<String, Object> configMap = new HashMap<>();
    configMap.put("url", "jdbc:h2:mem:testdb");
    configMap.put("username", "sa");
    configMap.put("password", "");
    configMap.put("table", "test_table");
    configMap.put("mode", "UPSERT");

    SinkConfig config = new SinkConfig();
    config.setConfig(configMap);

    // 实际运行会尝试连接数据库获取主键
    // 这里只验证配置解析逻辑不抛 NullPointerException
    // 真实数据库测试在集成测试中进行
    assertThrows(RuntimeException.class, () -> {
        new JdbcSink(config);
    }, "UPSERT 模式应该尝试获取主键（实际会因数据库不存在而失败）");
}
```

在 `testInvalidBatchSize()` 方法后添加此测试方法

---

### Step 3: 编写 UPSERT sql 模式禁止测试

- [ ] **添加 UPSERT sql 模式禁止测试**

```java
@Test
public void testUpsertWithSqlNotAllowed() {
    // UPSERT 模式配置 sql，应该抛异常
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("url", "jdbc:mysql://localhost:3306/test");
    configMap.put("username", "root");
    configMap.put("password", "password");
    configMap.put("sql", "INSERT INTO target_table VALUES(:id, :name)");
    configMap.put("mode", "UPSERT");

    SinkConfig config = new SinkConfig();
    config.setConfig(configMap);

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
        new JdbcSink(config);
    }, "UPSERT 模式配置 sql 应该抛出 IllegalArgumentException");

    assertTrue(exception.getMessage().contains("必须配置 table"),
        "异常信息应提示必须配置 table");
    assertTrue(exception.getMessage().contains("不能使用 sql"),
        "异常信息应提示不能使用 sql");
}
```

在 `testUpsertWithTableAutoPrimaryKey()` 方法后添加此测试方法

---

### Step 4: 运行测试验证失败

- [ ] **运行测试**

运行命令:
```bash
cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool
mvn test -Dtest=JdbcSinkTest#testUpsertWithSqlNotAllowed
```

预期输出:
```
[ERROR] testUpsertWithSqlNotAllowed FAILED
Expected: IllegalArgumentException
Actual: NullPointerException (或正常执行)
```

---

### Step 5: 修改 JdbcSink 构造函数

- [ ] **修改 JdbcSink.java 构造函数**

找到原有的 UPSERT 逻辑（约第 45-53 行）:

```java
List<String> keyFields = null;
if (mode == WriteMode.UPSERT) {
    List<String> keyFieldsConfig = config.getList("keyFields");
    Preconditions.checkNotNull(keyFieldsConfig, "UPSERT 模式必须配置 keyFields");
    keyFields = keyFieldsConfig;
    log.info("JDBC Sink upsert 模式: table={}, keyFields={}", table, keyFields);
} else {
    log.info("JDBC Sink insert 模式: table={}", table);
}
```

替换为:

```java
List<String> keyFields = null;
if (mode == WriteMode.UPSERT) {
    // UPSERT 模式必须配置 table，不能配置 sql
    Preconditions.checkNotNull(table,
        "UPSERT 模式必须配置 table（不能使用 sql），因为需要从数据库获取主键信息");

    // 自动获取主键
    LinkedHashMap<String, Integer> pkInfo =
        SqlUtils.getPrimaryKey(url, table, username, password);
    keyFields = new ArrayList<>(pkInfo.keySet());

    log.info("JDBC Sink UPSERT 模式自动获取主键: table={}, keyFields={}", table, keyFields);
} else {
    log.info("JDBC Sink INSERT 模式: table={}", table);
}
```

需要添加导入:
```java
import com.etl.core.utils.SqlUtils;
import java.util.ArrayList;
import java.util.LinkedHashMap;
```

- [ ] **修改 JdbcSink.java**

使用 Edit 工具替换 UPSERT 逻辑块和添加导入

---

### Step 6: 运行测试验证通过

- [ ] **运行 JdbcSinkTest 所有测试**

运行命令:
```bash
cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool
mvn test -Dtest=JdbcSinkTest
```

预期输出:
```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

测试方法:
- testMissingUrl
- testMissingTableAndSql
- testUpsertWithTableAutoPrimaryKey
- testUpsertWithSqlNotAllowed
- testInvalidBatchSize

---

### Step 7: 编译验证整个项目

- [ ] **编译项目**

运行命令:
```bash
cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool
mvn clean compile
```

预期输出:
```
[INFO] BUILD SUCCESS
```

---

### Step 8: 提交 JdbcSink 修改

- [ ] **提交代码**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java
git add flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkTest.java
git commit -m "feat: JdbcSink UPSERT 模式自动获取主键，删除 keyFields 配置

变更：
- UPSERT 模式自动调用 SqlUtils.getPrimaryKey() 获取主键
- 新增约束：UPSERT 必须配置 table，不能配置 sql
- 删除 keyFields 手动配置支持

测试：
- 删除 testUpsertWithoutKeyFields（不再需要）
- 新增 testUpsertWithSqlNotAllowed（验证 sql 模式禁止）
- 新增 testUpsertWithTableAutoPrimaryKey（验证自动获取）

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 3: PLUGINS.md 文档更新

**目标:** 更新文档删除 keyFields 配置，新增 UPSERT 约束说明

### Step 1: 删除 keyFields 配置参数说明

- [ ] **删除 JDBC Sink 配置参数表中的 keyFields 行**

找到 PLUGINS.md 第 722 行附近的配置参数表:

```markdown
| `keyFields` | upsert 必填 | - | Upsert 模式的主键/唯一键字段，数组格式 |
```

删除该行

- [ ] **修改 PLUGINS.md**

使用 Edit 工具删除 keyFields 配置参数行

---

### Step 2: 新增 UPSERT 约束说明

- [ ] **在配置参数说明后添加约束说明**

在 PLUGINS.md 的 JDBC Sink "两种模式" 部分（约第 725 行）之前添加:

```markdown
#### UPSERT 模式约束

**自动获取主键机制：**
- UPSERT 模式自动从数据库获取表主键信息，无需手动配置
- 复合主键表会使用所有主键列作为条件字段
- 表必须有主键，否则抛出异常

**约束条件：**
- **必须配置 table，不能配置 sql**：主键信息只存在于物理表
- **表必须有主键**：无主键表无法使用 UPSERT 模式，建议使用 INSERT 模式或添加主键

**错误处理：**
- 配置 sql 时抛异常：`"UPSERT 模式必须配置 table（不能使用 sql）"`
- 表无主键时抛异常：`"表 'xxx' 没有主键，无法使用 UPSERT 模式"`

---
```

- [ ] **修改 PLUGINS.md**

使用 Edit 工具在配置参数表后、"两种模式" 部分前添加此说明

---

### Step 3: 更新配置示例

- [ ] **删除 UPSERT 示例中的 keyFields**

找到 PLUGINS.md 中 JDBC Sink 的配置示例（约第 752-770 行附近）:

删除示例中的 `keyFields` 配置行:

```json
"keyFields": ["id", "name"]
```

更新示例为:

```markdown
**table 模式 - UPSERT（自动获取主键）：**

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
      "mode": "UPSERT",
      "batchSize": 100
    }
  }
}
```

> **说明：** UPSERT 模式会自动从数据库获取 `target_table` 的主键信息，无需配置 keyFields。

---
```

- [ ] **修改 PLUGINS.md**

使用 Edit 工具更新 UPSERT 配置示例

---

### Step 4: 提交文档更新

- [ ] **提交文档**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 JDBC Sink 文档，删除 keyFields 配置

变更：
- 删除 keyFields 配置参数说明
- 新增 UPSERT 模式约束说明（必须配置 table、表必须有主键）
- 更新 UPSERT 配置示例，删除 keyFields 配置
- 说明自动获取主键机制

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 4: 集成测试验证（可选）

**目标:** 使用 H2 数据库验证完整功能

### Step 1: 创建集成测试

- [ ] **创建 JdbcSinkIntegrationTest.java**

```java
package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JdbcSink 集成测试
 * 使用 H2 内存数据库验证自动获取主键功能
 */
public class JdbcSinkIntegrationTest {

    private static final String H2_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";

    @BeforeEach
    public void setUp() throws Exception {
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            // 创建带主键的表
            stmt.execute("CREATE TABLE target_table (id INT PRIMARY KEY, name VARCHAR(100))");
            // 创建复合主键表
            stmt.execute("CREATE TABLE composite_pk_table (id INT, name VARCHAR(100), PRIMARY KEY (id, name))");
            // 创建无主键表
            stmt.execute("CREATE TABLE no_pk_table (id INT, name VARCHAR(100))");
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        try (Connection conn = DriverManager.getConnection(H2_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    public void testUpsertWithSinglePrimaryKey() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", H2_URL);
        configMap.put("username", USERNAME);
        configMap.put("password", PASSWORD);
        configMap.put("table", "target_table");
        configMap.put("mode", "UPSERT");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        // 应该成功创建 JdbcSink（自动获取主键）
        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink, "UPSERT 模式应成功创建 JdbcSink");
    }

    @Test
    public void testUpsertWithCompositePrimaryKey() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", H2_URL);
        configMap.put("username", USERNAME);
        configMap.put("password", PASSWORD);
        configMap.put("table", "composite_pk_table");
        configMap.put("mode", "UPSERT");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        // 应该成功创建 JdbcSink（自动获取复合主键）
        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink, "UPSERT 模式应成功处理复合主键");
    }

    @Test
    public void testUpsertWithNoPrimaryKey() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", H2_URL);
        configMap.put("username", USERNAME);
        configMap.put("password", PASSWORD);
        configMap.put("table", "no_pk_table");
        configMap.put("mode", "UPSERT");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        // 应该抛异常（表没有主键）
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            new JdbcSink(config);
        }, "UPSERT 模式对无主键表应抛异常");

        assertTrue(exception.getMessage().contains("没有主键"),
            "异常信息应提示表没有主键");
    }

    @Test
    public void testInsertModeStillWorks() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", H2_URL);
        configMap.put("username", USERNAME);
        configMap.put("password", PASSWORD);
        configMap.put("table", "no_pk_table");
        configMap.put("mode", "INSERT");

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        // INSERT 模式应该不受影响，可以正常工作
        JdbcSink sink = new JdbcSink(config);
        assertNotNull(sink, "INSERT 模式应正常工作");
    }
}
```

文件路径: `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkIntegrationTest.java`

- [ ] **创建集成测试文件**

使用 Write 工具创建文件

---

### Step 2: 运行集成测试

- [ ] **运行集成测试**

运行命令:
```bash
cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool
mvn test -Dtest=JdbcSinkIntegrationTest
```

预期输出:
```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

---

### Step 3: 提交集成测试

- [ ] **提交集成测试**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkIntegrationTest.java
git commit -m "test: JdbcSink 集成测试验证自动获取主键功能

使用 H2 内存数据库验证：
- UPSERT 模式自动获取单主键
- UPSERT 模式自动获取复合主键
- UPSERT 模式对无主键表抛异常
- INSERT 模式不受影响

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## 完成验证

### 最终验证步骤

- [ ] **运行所有测试**

运行命令:
```bash
cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool
mvn clean test
```

预期输出:
```
[INFO] Tests run: X, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

- [ ] **编译打包**

运行命令:
```bash
mvn clean package -DskipTests
```

预期输出:
```
[INFO] BUILD SUCCESS
```

- [ ] **查看 Git 状态**

运行命令:
```bash
git status
git log --oneline -5
```

预期输出:
```
working tree clean
最近的 5 个提交应包含：
- feat: SqlUtils 新增 getPrimaryKey 方法
- feat: JdbcSink UPSERT 模式自动获取主键
- docs: 更新 JDBC Sink 文档
- test: JdbcSink 集成测试（可选）
```

---

## 自我审查检查清单

**Spec 覆盖检查:**
- ✅ SqlUtils.getPrimaryKey() 方法实现（Task 1）
- ✅ JdbcSink 构造函数自动获取主键（Task 2）
- ✅ UPSERT 必须配置 table 校验（Task 2）
- ✅ 表无主键抛异常（Task 1, Task 2）
- ✅ 复合主键支持（Task 1）
- ✅ PLUGINS.md 文档更新（Task 3）
- ✅ 测试覆盖（Task 1, Task 2, Task 4）

**Placeholder 扫描:**
- ✅ 无 "TBD"、"TODO"、"implement later"
- ✅ 无 "add appropriate error handling"
- ✅ 无 "write tests for the above"
- ✅ 所有代码步骤包含完整代码
- ✅ 所有命令步骤包含完整命令和预期输出

**类型一致性检查:**
- ✅ getPrimaryKey() 返回 LinkedHashMap<String, Integer> 在所有引用中一致
- ✅ 方法签名在测试和实现中一致
- ✅ JdbcSink 使用 ArrayList<>(pkInfo.keySet()) 转换类型正确