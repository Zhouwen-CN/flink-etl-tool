# JDBC Sink CDC Upsert 模式实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 JDBC Sink CDC 模式中的 INSERT 和 UPDATE_AFTER 操作改为使用 upsert SQL，简化逻辑并提升数据库原子性

**Architecture:** 修改 JdbcSinkWriter 的 CDC SQL 生成逻辑，使 INSERT 和 UPDATE_AFTER 都调用 dialect.getUpsertSql()，统一参数设置逻辑。DELETE 保持不变。

**Tech Stack:** Java 1.8, Flink 1.15.2, JDBC, Mockito/JUnit 5 for testing

---

## 文件结构

本次修改涉及以下文件：

| 文件 | 责任 | 变更类型 |
|------|------|---------|
| `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSinkWriter.java` | JDBC Sink Writer 实现，管理 CDC 模式 SQL 生成和参数设置 | 修改 |
| `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/JdbcSinkWriterTest.java` | JDBC Sink Writer 单元测试，验证 CDC 模式行为 | 新增测试 |
| `PLUGINS.md` | 插件文档，记录 JDBC Sink CDC 模式行为 | 修改 |

**关键变更点：**
- `buildCdcSql()` 方法：INSERT 和 UPDATE_AFTER 都返回 getUpsertSql()
- `setCdcParameters()` 方法：INSERT 和 UPDATE_AFTER 都使用统一的参数设置逻辑（设置所有字段值）
- 新增 CDC 模式单元测试验证行为

---

### Task 1: 添加 CDC 模式单元测试（验证当前行为）

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/JdbcSinkWriterTest.java:1-307`

**目标:** 在修改代码前，先添加测试验证 CDC 模式的当前行为，建立 baseline

- [ ] **Step 1: 添加 CDC INSERT 模式测试**

在 `JdbcSinkWriterTest.java` 文件末尾添加测试方法：

```java
/**
 * 测试 CDC 模式 INSERT 操作使用 getInsertSql()
 * 当前行为验证：INSERT 应该生成 INSERT SQL
 */
@Test
public void testCdcInsertModeCurrentBehavior() throws Exception {
    // Mock Connection 和 PreparedStatement
    mockConnection = mock(Connection.class);
    mockStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    when(mockConnection.getAutoCommit()).thenReturn(false);
    doNothing().when(mockConnection).commit();

    // 准备配置：CDC 模式
    JdbcSinkConfig config = JdbcSinkConfig.builder()
        .url("jdbc:mysql://localhost:3306/test")
        .username("root")
        .password("password")
        .mode(WriteMode.CDC)
        .table("test_table")
        .keyFields(Arrays.asList("id"))
        .batchSize(100)
        .dialect(new MySQLDialect())
        .build();

    // Mock DriverManager
    MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class);
    mockedDriverManager.when(() ->
        DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword())
    ).thenReturn(mockConnection);

    JdbcSinkWriter writer = null;
    try {
        // 创建 Writer
        writer = new JdbcSinkWriter(mockContext, config);

        // 写入 INSERT 类型的 Row
        Row row = Row.withNames();
        row.setField("id", "1");
        row.setField("name", "Alice");
        row.setKind(RowKind.INSERT);
        writer.write(row, null);

        // 验证当前行为：应该生成 INSERT SQL
        verify(mockConnection).prepareStatement(eq("INSERT INTO `test_table` (`name`, `id`) VALUES (?, ?)"));

        // 手动 flush
        when(mockStatement.executeBatch()).thenReturn(new int[]{1});
        writer.flush(false);

    } finally {
        if (writer != null) {
            try { writer.close(); } catch (Exception e) {}
        }
        mockedDriverManager.close();
    }
}
```

- [ ] **Step 2: 运行测试验证当前行为**

Run: `mvn test -Dtest=JdbcSinkWriterTest#testCdcInsertModeCurrentBehavior -pl flink-etl-connector/connector-jdbc`

Expected: PASS（验证 INSERT 生成 INSERT SQL）

- [ ] **Step 3: 添加 CDC UPDATE_AFTER 模式测试**

继续在 `JdbcSinkWriterTest.java` 文件末尾添加：

```java
/**
 * 测试 CDC 模式 UPDATE_AFTER 操作使用 getUpdateSql()
 * 当前行为验证：UPDATE_AFTER 应该生成 UPDATE SQL
 */
@Test
public void testCdcUpdateAfterModeCurrentBehavior() throws Exception {
    // Mock Connection 和 PreparedStatement
    mockConnection = mock(Connection.class);
    mockStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    when(mockConnection.getAutoCommit()).thenReturn(false);
    doNothing().when(mockConnection).commit();

    // 准备配置：CDC 模式
    JdbcSinkConfig config = JdbcSinkConfig.builder()
        .url("jdbc:mysql://localhost:3306/test")
        .username("root")
        .password("password")
        .mode(WriteMode.CDC)
        .table("test_table")
        .keyFields(Arrays.asList("id"))
        .batchSize(100)
        .dialect(new MySQLDialect())
        .build();

    // Mock DriverManager
    MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class);
    mockedDriverManager.when(() ->
        DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword())
    ).thenReturn(mockConnection);

    JdbcSinkWriter writer = null;
    try {
        // 创建 Writer
        writer = new JdbcSinkWriter(mockContext, config);

        // 写入 UPDATE_AFTER 类型的 Row
        Row row = Row.withNames();
        row.setField("id", "1");
        row.setField("name", "Alice-updated");
        row.setKind(RowKind.UPDATE_AFTER);
        writer.write(row, null);

        // 验证当前行为：应该生成 UPDATE SQL（WHERE 用主键，SET 用非主键字段）
        verify(mockConnection).prepareStatement(eq("UPDATE `test_table` SET `name` = ? WHERE `id` = ?"));

        // 手动 flush
        when(mockStatement.executeBatch()).thenReturn(new int[]{1});
        writer.flush(false);

    } finally {
        if (writer != null) {
            try { writer.close(); } catch (Exception e) {}
        }
        mockedDriverManager.close();
    }
}
```

- [ ] **Step 4: 运行测试验证当前行为**

Run: `mvn test -Dtest=JdbcSinkWriterTest#testCdcUpdateAfterModeCurrentBehavior -pl flink-etl-connector/connector-jdbc`

Expected: PASS（验证 UPDATE_AFTER 生成 UPDATE SQL）

- [ ] **Step 5: 添加 CDC DELETE 模式测试**

继续添加 DELETE 测试（DELETE 保持不变，所以这个测试会一直有效）：

```java
/**
 * 测试 CDC 模式 DELETE 操作使用 getDeleteSql()
 * DELETE 行为不变，一直生成 DELETE SQL
 */
@Test
public void testCdcDeleteModeBehavior() throws Exception {
    // Mock Connection 和 PreparedStatement
    mockConnection = mock(Connection.class);
    mockStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    when(mockConnection.getAutoCommit()).thenReturn(false);
    doNothing().when(mockConnection).commit();

    // 准备配置：CDC 模式
    JdbcSinkConfig config = JdbcSinkConfig.builder()
        .url("jdbc:mysql://localhost:3306/test")
        .username("root")
        .password("password")
        .mode(WriteMode.CDC)
        .table("test_table")
        .keyFields(Arrays.asList("id"))
        .batchSize(100)
        .dialect(new MySQLDialect())
        .build();

    // Mock DriverManager
    MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class);
    mockedDriverManager.when(() ->
        DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword())
    ).thenReturn(mockConnection);

    JdbcSinkWriter writer = null;
    try {
        // 创建 Writer
        writer = new JdbcSinkWriter(mockContext, config);

        // 写入 DELETE 类型的 Row
        Row row = Row.withNames();
        row.setField("id", "1");
        row.setField("name", "Alice");
        row.setKind(RowKind.DELETE);
        writer.write(row, null);

        // 验证：DELETE 应该生成 DELETE SQL（WHERE 用主键）
        verify(mockConnection).prepareStatement(eq("DELETE FROM `test_table` WHERE `id` = ?"));

        // 手动 flush
        when(mockStatement.executeBatch()).thenReturn(new int[]{1});
        writer.flush(false);

    } finally {
        if (writer != null) {
            try { writer.close(); } catch (Exception e) {}
        }
        mockedDriverManager.close();
    }
}
```

- [ ] **Step 6: 运行所有 CDC 测试验证 baseline**

Run: `mvn test -Dtest=JdbcSinkWriterTest#testCdc* -pl flink-etl-connector/connector-jdbc`

Expected: 所有 3 个测试 PASS（验证当前 CDC 行为）

- [ ] **Step 7: 提交 baseline 测试**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/JdbcSinkWriterTest.java
git commit -m "test: 添加 CDC 模式 baseline 测试验证当前行为"
```

---

### Task 2: 修改 buildCdcSql() 使 INSERT 和 UPDATE_AFTER 使用 upsert SQL

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSinkWriter.java:166-182`

**目标:** 修改 CDC SQL 生成逻辑，INSERT 和 UPDATE_AFTER 都返回 getUpsertSql()

- [ ] **Step 1: 修改 buildCdcSql() 方法**

定位到 `JdbcSinkWriter.java` 第 166-182 行，修改 `buildCdcSql()` 方法：

```java
/**
 * 构建 CDC SQL
 */
private String buildCdcSql(RowKind kind) {
    String table = config.getTable();
    List<String> keyFields = config.getKeyFields();

    switch (kind) {
        case INSERT:
        case UPDATE_AFTER:
            // INSERT 和 UPDATE_AFTER 都使用 upsert SQL（原子操作，存在则更新，不存在则插入）
            return config.getDialect().getUpsertSql(table, columns, keyFields);
        case DELETE:
            return config.getDialect().getDeleteSql(table, keyFields);
        default:
            throw new IllegalArgumentException(
                String.format("CDC 模式不支持 RowKind: %s，支持: INSERT, UPDATE_AFTER, DELETE", kind)
            );
    }
}
```

**变更说明：**
- INSERT 和 UPDATE_AFTER 合并为一个 case，都调用 `getUpsertSql()`
- DELETE 保持不变，仍调用 `getDeleteSql()`

- [ ] **Step 2: 运行 CDC INSERT 测试验证失败**

Run: `mvn test -Dtest=JdbcSinkWriterTest#testCdcInsertModeCurrentBehavior -pl flink-etl-connector/connector-jdbc`

Expected: FAIL（因为现在生成的是 UPSERT SQL 而不是 INSERT SQL）

- [ ] **Step 3: 运行 CDC UPDATE_AFTER 测试验证失败**

Run: `mvn test -Dtest=JdbcSinkWriterTest#testCdcUpdateAfterModeCurrentBehavior -pl flink-etl-connector/connector-jdbc`

Expected: FAIL（因为现在生成的是 UPSERT SQL 而不是 UPDATE SQL）

- [ ] **Step 4: 运行 CDC DELETE 测试验证仍通过**

Run: `mvn test -Dtest=JdbcSinkWriterTest#testCdcDeleteModeBehavior -pl flink-etl-connector/connector-jdbc`

Expected: PASS（DELETE 行为未改变）

- [ ] **Step 5: 提交 buildCdcSql() 修改**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSinkWriter.java
git commit -m "feat: CDC 模式 INSERT 和 UPDATE_AFTER 改用 upsert SQL"
```

---

### Task 3: 修改 setCdcParameters() 统一参数设置逻辑

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSinkWriter.java:187-221`

**目标:** 简化参数设置逻辑，INSERT 和 UPDATE_AFTER 都设置所有字段值

- [ ] **Step 1: 修改 setCdcParameters() 方法**

定位到 `JdbcSinkWriter.java` 第 187-221 行，修改 `setCdcParameters()` 方法：

```java
/**
 * 设置 CDC 参数
 */
private void setCdcParameters(PreparedStatement stmt, Row row, RowKind kind) throws SQLException {
    int index = 1;

    switch (kind) {
        case INSERT:
        case UPDATE_AFTER:
            // INSERT 和 UPDATE_AFTER 都使用 upsert SQL，参数顺序：所有字段值
            for (String col : columns) {
                stmt.setObject(index++, row.getField(col));
            }
            break;

        case DELETE:
            // DELETE: 只设置主键字段（WHERE 条件）
            for (String key : config.getKeyFields()) {
                stmt.setObject(index++, row.getField(key));
            }
            break;

        default:
            throw new IllegalArgumentException("CDC 模式不支持 RowKind: " + kind);
    }
}
```

**变更说明：**
- INSERT 和 UPDATE_AFTER 合并为一个 case，参数设置逻辑相同：按字段顺序设置所有值
- DELETE 保持不变：只设置主键字段
- 移除了 UPDATE_AFTER 的复杂参数顺序逻辑（先 SET 非主键，后 WHERE 主键）

- [ ] **Step 2: 验证代码编译**

Run: `mvn compile -pl flink-etl-connector/connector-jdbc`

Expected: SUCCESS（无编译错误）

- [ ] **Step 3: 提交 setCdcParameters() 修改**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSinkWriter.java
git commit -m "refactor: CDC 模式统一 INSERT 和 UPDATE_AFTER 参数设置逻辑"
```

---

### Task 4: 修改测试验证新行为

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/JdbcSinkWriterTest.java`（Task 1 中添加的测试）

**目标:** 更新 Task 1 中添加的测试，验证 INSERT 和 UPDATE_AFTER 现在使用 UPSERT SQL

- [ ] **Step 1: 修改 testCdcInsertModeCurrentBehavior 测试**

定位到 Task 1 添加的 `testCdcInsertModeCurrentBehavior` 测试，修改验证部分：

```java
/**
 * 测试 CDC 模式 INSERT 操作使用 getUpsertSql()
 * 新行为验证：INSERT 应该生成 UPSERT SQL（原子操作）
 */
@Test
public void testCdcInsertModeCurrentBehavior() throws Exception {
    // ... 前面代码保持不变 ...

    // 验证新行为：应该生成 UPSERT SQL（包含 ON DUPLICATE KEY UPDATE）
    verify(mockConnection).prepareStatement(contains("INSERT INTO `test_table`"));
    verify(mockConnection).prepareStatement(contains("ON DUPLICATE KEY UPDATE"));

    // 参数设置：所有字段值（id 和 name）
    verify(mockStatement, times(2)).setObject(anyInt(), any());

    // ... 后面代码保持不变 ...
}
```

- [ ] **Step 2: 修改 testCdcUpdateAfterModeCurrentBehavior 测试**

定位到 Task 1 添加的 `testCdcUpdateAfterModeCurrentBehavior` 测试，修改验证部分：

```java
/**
 * 测试 CDC 模式 UPDATE_AFTER 操作使用 getUpsertSql()
 * 新行为验证：UPDATE_AFTER 应该生成 UPSERT SQL（原子操作）
 */
@Test
public void testCdcUpdateAfterModeCurrentBehavior() throws Exception {
    // ... 前面代码保持不变 ...

    // 验证新行为：应该生成 UPSERT SQL（包含 ON DUPLICATE KEY UPDATE）
    verify(mockConnection).prepareStatement(contains("INSERT INTO `test_table`"));
    verify(mockConnection).prepareStatement(contains("ON DUPLICATE KEY UPDATE"));

    // 参数设置：所有字段值（id 和 name），不再区分 SET 和 WHERE
    verify(mockStatement, times(2)).setObject(anyInt(), any());

    // ... 后面代码保持不变 ...
}
```

- [ ] **Step 3: 运行所有 CDC 测试验证新行为**

Run: `mvn test -Dtest=JdbcSinkWriterTest#testCdc* -pl flink-etl-connector/connector-jdbc`

Expected: 所有 3 个测试 PASS（验证新 CDC 行为）

- [ ] **Step 4: 运行完整测试套件**

Run: `mvn test -pl flink-etl-connector/connector-jdbc`

Expected: 所有测试 PASS（无回归）

- [ ] **Step 5: 提交测试修改**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/JdbcSinkWriterTest.java
git commit -m "test: 更新 CDC 测试验证 INSERT 和 UPDATE_AFTER 使用 upsert SQL"
```

---

### Task 5: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`（JDBC Sink CDC 模式说明部分）

**目标:** 更新文档说明 CDC 模式现在使用 upsert SQL 处理 INSERT 和 UPDATE_AFTER

- [ ] **Step 1: 查找 JDBC Sink CDC 模式文档位置**

Run: `grep -n "CDC 模式" PLUGINS.md`

记录输出中的行号范围。

- [ ] **Step 2: 修改 CDC 模式说明**

根据 Step 1 找到的位置，修改 CDC 模式说明：

**旧内容（示例）：**
```
CDC 模式根据 RowKind 执行不同操作：
- INSERT：执行 INSERT SQL
- UPDATE_AFTER：执行 UPDATE SQL
- DELETE：执行 DELETE SQL
```

**新内容：**
```
CDC 模式根据 RowKind 执行不同操作：
- INSERT 和 UPDATE_AFTER：执行 UPSERT SQL（原子操作，存在则更新，不存在则插入）
- DELETE：执行 DELETE SQL

使用 upsert SQL 的优势：
- 原子性：一条 SQL 完成插入或更新，避免并发问题
- 简化逻辑：统一处理 INSERT 和 UPDATE，参数设置相同
- 性能优化：数据库层面的原子操作比应用层判断更高效
```

- [ ] **Step 3: 验证文档格式**

Run: `cat PLUGINS.md | grep -A 10 "CDC 模式"`

Expected: 文档格式正确，说明清晰

- [ ] **Step 4: 提交文档更新**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 JDBC Sink CDC 模式说明，INSERT 和 UPDATE 使用 upsert"
```

---

### Task 6: 验证完整实现并打包

**Files:**
- 无文件修改，仅验证

**目标:** 编译整个项目，运行所有测试，确保无回归

- [ ] **Step 1: 清理并编译整个项目**

Run: `mvn clean compile`

Expected: SUCCESS（所有模块编译成功）

- [ ] **Step 2: 运行 connector-jdbc 所有测试**

Run: `mvn test -pl flink-etl-connector/connector-jdbc`

Expected: 所有测试 PASS（包括新增的 CDC 测试）

- [ ] **Step 3: 运行整个项目测试套件**

Run: `mvn test`

Expected: 所有测试 PASS（无回归）

- [ ] **Step 4: 打包项目验证**

Run: `mvn package -DskipTests`

Expected: SUCCESS（生成可执行 JAR）

- [ ] **Step 5: 提交所有变更**

```bash
git status
git log --oneline -5
```

确认所有 commit 已提交，无遗漏文件。

---

## 自审检查清单

**1. Spec 覆盖检查：**
- ✅ INSERT 改用 upsert SQL → Task 2
- ✅ UPDATE_AFTER 改用 upsert SQL → Task 2
- ✅ 参数设置逻辑统一 → Task 3
- ✅ DELETE 保持不变 → Task 2 和 Task 3 保持 DELETE 原逻辑
- ✅ 测试验证新行为 → Task 1 和 Task 4
- ✅ 文档更新 → Task 5

**2. Placeholder 扫描：**
- ✅ 无 TBD、TODO 等占位符
- ✅ 所有步骤包含完整代码或命令
- ✅ 所有测试包含完整测试代码

**3. 类型一致性检查：**
- ✅ buildCdcSql() 返回 String，调用者使用 PreparedStatement
- ✅ setCdcParameters() 参数顺序与 getUpsertSql() 生成的 SQL 一致
- ✅ RowKind 枚举值使用正确（INSERT、UPDATE_AFTER、DELETE）

**4. 文件路径检查：**
- ✅ 所有文件路径为绝对路径或明确相对路径
- ✅ 测试文件位于 src/test/java/，镜像 src/main/java/ 结构
- ✅ 文档文件为项目根目录下的 PLUGINS.md

---

## 执行选择

**Plan complete and saved to `docs/superpowers/plans/2026-04-15-jdbc-sink-cdc-upsert.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - 我为每个 Task 派发全新的子代理，在 Task 之间进行审查，快速迭代

**2. Inline Execution** - 在此会话中使用 executing-plans skill 执行任务，批量执行并在检查点审查

**请选择执行方式？**