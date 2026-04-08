# JDBC Sink 可选 keyFields 配置实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 JDBC Sink 的 UPSERT 模式添加可选的 keyFields 配置参数，优先使用用户配置的主键字段，未配置时自动从数据库获取。

**Architecture:** 在 JdbcSink 构造函数中增加配置参数读取逻辑，优先检查用户配置的 keyFields，若未配置则调用
SqlUtils.getPrimaryKey() 自动获取。保持向后兼容，现有无需配置 keyFields 的配置文件继续有效。

**Tech Stack:** Java 1.8, Flink 1.15.2, Lombok, JDBC DatabaseMetaData

---

## 文件结构

**修改文件：**

- `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java` - 构造函数增加 keyFields 配置读取逻辑
- `PLUGINS.md` - 更新 JDBC Sink 配置参数文档，新增 keyFields 参数说明

**测试文件：**

- `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkTest.java` - 新增测试验证 keyFields 配置优先级

**无需修改：**

- `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/config/JdbcSinkConfig.java` - 已包含 keyFields
  字段，无需修改
- `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java` - getPrimaryKey 方法已实现，无需修改

---

## Task 1: 更新 JdbcSink 构造函数逻辑

**Files:**

- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java:48-62`
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java` - 添加 getJdbcSinkConfig()
  getter 方法
- Test: `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkTest.java`

- [ ] **Step 1: 在 JdbcSink 类中添加 getter 方法**

为测试访问 private 字段，在 JdbcSink 类中添加 getter 方法（在构造函数后）：

```java
/**
 * 获取 JDBC Sink 配置对象（用于测试）
 */
public JdbcSinkConfig getJdbcSinkConfig() {
    return jdbcSinkConfig;
}
```

- [ ] **Step 2: 编写测试验证 keyFields 配置优先级**

在测试文件中新增测试方法，验证：

1. 用户配置 keyFields 时使用配置值
2. 未配置时自动从数据库获取

注意：使用 H2 内存数据库进行测试，需先创建测试表。

```java
import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JdbcSinkTest {

    @BeforeAll
    static void setupTestDatabase() throws Exception {
        // 创建 H2 内存数据库和测试表
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "");
             Statement stmt = conn.createStatement()) {
            // 创建带主键的表（主键为 id）
            stmt.execute("CREATE TABLE users_with_pk (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))");
            // 创建无主键但有唯一索引的表
            stmt.execute("CREATE TABLE users_no_pk (id INT, name VARCHAR(100), email VARCHAR(100) UNIQUE)");
        }
    }

    @Test
    void testKeyFieldsUserConfigured() throws Exception {
        // 测试用户配置 keyFields 的场景（使用配置的主键，而非数据库主键）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "users_with_pk");
        configMap.put("mode", "UPSERT");
        configMap.put("keyFields", Arrays.asList("email")); // 用户配置 email 为主键（忽略真实主键 id）

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证使用用户配置的主键 email，而非数据库主键 id
        List<String> keyFields = sinkConfig.getKeyFields();
        assertNotNull(keyFields);
        assertEquals(1, keyFields.size());
        assertTrue(keyFields.contains("email"));
        assertFalse(keyFields.contains("id")); // 不应包含真实主键
    }

    @Test
    void testKeyFieldsAutoFetched() throws Exception {
        // 测试未配置 keyFields 时自动从数据库获取主键
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "users_with_pk");
        configMap.put("mode", "UPSERT");
        // 不配置 keyFields，应自动获取表主键

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证自动获取的主键为 id（数据库真实主键）
        List<String> keyFields = sinkConfig.getKeyFields();
        assertNotNull(keyFields);
        assertEquals(1, keyFields.size());
        assertTrue(keyFields.contains("id"));
    }

    @Test
    void testKeyFieldsNoPkButUserConfigured() throws Exception {
        // 测试表无主键但用户配置了 keyFields（应正常使用配置值）
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:h2:mem:testdb");
        configMap.put("username", "sa");
        configMap.put("password", "");
        configMap.put("table", "users_no_pk"); // 该表无主键，但有 email 唯一索引
        configMap.put("mode", "UPSERT");
        configMap.put("keyFields", Arrays.asList("email")); // 手动配置唯一键字段

        SinkConfig config = new SinkConfig();
        config.setConfig(configMap);

        JdbcSink sink = new JdbcSink(config);
        JdbcSinkConfig sinkConfig = sink.getJdbcSinkConfig();

        // 验证使用用户配置的唯一键 email
        List<String> keyFields = sinkConfig.getKeyFields();
        assertNotNull(keyFields);
        assertEquals(1, keyFields.size());
        assertTrue(keyFields.contains("email"));
    }
}
```

- [ ] **Step 3: 运行测试验证失败（getter 方法未添加）**

```bash
mvn test -Dtest=JdbcSinkTest -pl flink-etl-sink/flink-etl-sink-jdbc
```

预期结果：编译失败，因为 `getJdbcSinkConfig()` 方法不存在。

- [ ] **Step 4: 修改 JdbcSink 构造函数实现配置优先级**

修改 `JdbcSink.java` 第 48-62 行的 UPSERT 模式逻辑：

```java
List<String> keyFields = null;
if(mode ==WriteMode.UPSERT){
        // UPSERT 模式必须配置 table，不能配置 sql
        Preconditions.

checkArgument(table !=null,
        "UPSERT 模式必须配置 table（不能使用 sql），因为需要主键信息");

// 优先使用用户配置的 keyFields，未配置时自动获取
List<String> configuredKeyFields = config.getList("keyFields");
    if(configuredKeyFields !=null&&!configuredKeyFields.

isEmpty()){
keyFields =configuredKeyFields;
        log.

info("JDBC Sink UPSERT 模式使用配置的主键: table={}, keyFields={}",table, keyFields);
    }else{
// 自动获取主键
LinkedHashMap<String, Integer> pkInfo =
        SqlUtils.getPrimaryKey(url, table, username, password);
keyFields =new ArrayList<>(pkInfo.

keySet());
        log.

info("JDBC Sink UPSERT 模式自动获取主键: table={}, keyFields={}",table, keyFields);
    }
            }else{
            log.

info("JDBC Sink INSERT 模式: table={}",table);
}
```

**关键变更点：**

- 新增 `config.getList("keyFields")` 读取用户配置
- 判断逻辑：用户配置优先 → 未配置时自动获取
- 更新日志消息区分两种情况

- [ ] **Step 5: 运行测试验证通过**

```bash
mvn test -Dtest=JdbcSinkTest -pl flink-etl-sink/flink-etl-sink-jdbc
```

预期结果：三个测试均通过，验证配置优先级正确。

- [ ] **Step 6: 提交代码变更**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java \
        flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkTest.java
git commit -m "feat: JDBC Sink UPSERT 模式支持可选 keyFields 配置

用户可显式配置 keyFields 参数指定主键字段：
- 已配置 keyFields：使用用户配置的主键列表
- 未配置 keyFields：自动从数据库获取主键信息

向后兼容现有配置文件，无需修改即可继续运行。"
```

---

## Task 2: 更新 PLUGINS.md 文档

**Files:**

- Modify: `PLUGINS.md:712-738` (JDBC Sink 配置参数表和 UPSERT 模式说明)

- [ ] **Step 1: 更新配置参数表**

在配置参数表（第 712-722 行）中新增 keyFields 参数说明：

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
| `keyFields` | 否 | 自动获取 | **UPSERT 模式专用**：主键/唯一键字段列表。未配置时自动从数据库获取主键信息 |
| `batchSize` | 否 | `100` | 批量写入大小 |
```

- [ ] **Step 2: 更新 UPSERT 模式约束说明**

修改第 724-738 行的 UPSERT 模式约束说明：

```markdown
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

- 配置 sql 时抛异常：`"UPSERT 模式必须配置 table（不能使用 sql），因为需要主键信息"`
- 未配置 keyFields 且表无主键时抛异常：`"表 'xxx' 没有主键，无法使用 UPSERT 模式。请使用 INSERT 模式、手动配置 keyFields 或为表添加主键"`
```

- [ ] **Step 3: 新增配置示例**

在第 749-760 行之后新增手动配置 keyFields 的示例：

```markdown
**table 模式 - UPSERT 手动指定主键：**

适用场景：表无主键但有唯一索引，或需使用部分字段作为匹配条件。

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/test",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "upsert",
      "keyFields": ["email"],  // 使用 email 字段作为唯一键匹配
      "batchSize": 100
    }
  }
}
```

> **说明：** 该配置使用 `email` 字段作为 UPSERT 的匹配条件，即使表有其他主键也不会使用。

```

- [ ] **Step 4: 验证文档格式**

检查 Markdown 格式是否正确，表格对齐、代码块语法等。

- [ ] **Step 5: 提交文档变更**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 JDBC Sink 文档，新增 keyFields 可选配置说明

说明 UPSERT 模式两种主键配置方式：
- 自动获取主键（推荐，未配置 keyFields 时）
- 手动指定主键（可选，配置 keyFields 时）

新增手动指定主键的配置示例，适用于表无主键场景。"
```

---

## Task 3: 验证向后兼容性

**Files:**

- Test: 运行现有测试和示例配置

- [ ] **Step 1: 运行所有 JDBC Sink 相关测试**

```bash
mvn test -pl flink-etl-sink/flink-etl-sink-jdbc
```

预期结果：所有现有测试通过，验证向后兼容性。

- [ ] **Step 2: 运行示例配置验证功能正常**

使用 docs/examples 中的现有配置文件（无需修改）运行测试：

```bash
mvn clean package -DskipTests
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file docs/examples/mysql-to-console.json
```

预期结果：任务正常运行，验证未配置 keyFields 的现有配置文件无需修改。

- [ ] **Step 3: 创建新配置验证新功能**

创建测试配置文件验证 keyFields 配置功能：

```json
{
  "job": {
    "name": "test-keyfields",
    "mode": "batch"
  },
  "sources": [
    {
      "type": "jdbc",
      "outputTable": "source_data",
      "config": {
        "url": "jdbc:mysql://localhost:3306/test",
        "username": "root",
        "password": "password",
        "table": "source_table"
      }
    }
  ],
  "sinks": [
    {
      "type": "jdbc",
      "inputTable": "source_data",
      "config": {
        "url": "jdbc:mysql://localhost:3306/test",
        "username": "root",
        "password": "password",
        "table": "target_table",
        "mode": "upsert",
        "keyFields": [
          "email"
        ]
      }
    }
  ]
}
```

运行验证：

```bash
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file test-keyfields.json
```

预期结果：任务正常运行，使用 email 字段作为 UPSERT 匹配条件。

- [ ] **Step 4: 提交验证测试文件（可选）**

如果需要保留测试配置：

```bash
git add docs/examples/test-keyfields-config.json
git commit -m "docs: 新增 keyFields 配置示例"
```

---

## 实现要点总结

1. **配置优先级**：用户配置 > 自动获取
2. **向后兼容**：现有配置无需修改继续有效
3. **适用场景扩展**：
    - 表有主键：自动获取或手动指定
    - 表无主键但有唯一索引：必须手动配置 keyFields
4. **文档完整性**：说明两种方式、优先级、适用场景、示例配置

---

## 验收标准

- ✅ 用户配置 keyFields 时使用配置值
- ✅ 未配置 keyFields 时自动从数据库获取
- ✅ 向后兼容现有配置文件
- ✅ 文档完整说明两种方式和优先级
- ✅ 所有测试通过
- ✅ 示例配置正常运行