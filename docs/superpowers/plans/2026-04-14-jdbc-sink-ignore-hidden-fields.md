# JDBC Sink 忽略 `__` 开头隐藏字段实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 JDBC Sink 在写入数据时自动忽略以 `__` 开头的隐藏字段（如 Kafka Source 添加的 `__topic__`）

**Architecture:** 在 `JdbcSinkWriter` 首次获取 Row 字段名时过滤掉 `__` 前缀的字段，这样所有 SQL 生成逻辑（INSERT/UPSERT/CDC）都会自动受益于此改动。

**Tech Stack:** Java, JUnit 5, Mockito

---

## 文件变更概览

| 文件 | 操作 | 职责 |
|------|------|------|
| `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java` | 修改 | 过滤 `__` 开头的隐藏字段 |
| `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java` | 修改 | 新增测试用例验证隐藏字段过滤 |
| `PLUGINS.md` | 修改 | 文档说明此特性 |

---

### Task 1: 修改 JdbcSinkWriter 过滤隐藏字段

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java:71-76`
- Test: `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java`

- [ ] **Step 1: 添加隐藏字段过滤逻辑**

修改 `write` 方法中首次获取字段名的逻辑，过滤掉以 `__` 开头的字段：

```java
@Override
public void write(Row row, Context context) throws IOException, InterruptedException {
    try {
        // 首次写入时缓存列名（过滤掉 __ 开头的隐藏字段）
        if (columns == null) {
            columns = row.getFieldNames(true)
                .filter(name -> !name.startsWith("__"))
                .toArray(String[]::new);
            log.info("JDBC Sink 写入字段（已过滤隐藏字段）: {}", Arrays.toString(columns));
        }

        if (config.getMode() == WriteMode.CDC) {
            writeCdcRow(row);
        } else {
            writeNormalRow(row);
        }
        // ... 其余代码不变
```

- [ ] **Step 2: 运行现有测试确保不破坏功能**

Run: `mvn test -pl flink-etl-sink/flink-etl-sink-jdbc -Dtest=JdbcSinkWriterTest`
Expected: 所有现有测试通过

- [ ] **Step 3: 提交代码**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java
git commit -m "feat(jdbc-sink): 忽略 __ 开头的隐藏字段"
```

---

### Task 2: 新增单元测试验证隐藏字段过滤

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java`

- [ ] **Step 1: 添加测试用例**

在 `JdbcSinkWriterTest.java` 中添加新测试方法，验证 `__topic__` 等隐藏字段被正确过滤：

```java
/**
 * 测试 INSERT 模式过滤 __ 开头的隐藏字段
 * 当 Row 包含 __topic__ 等隐藏字段时，这些字段不应被写入 SQL
 */
@Test
public void testInsertModeIgnoresHiddenFields() throws Exception {
    // Mock Connection 和 PreparedStatement
    mockConnection = mock(Connection.class);
    mockStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    when(mockConnection.getAutoCommit()).thenReturn(false);
    doNothing().when(mockConnection).commit();

    // 准备配置：INSERT 模式
    JdbcSinkConfig config = JdbcSinkConfig.builder()
        .url("jdbc:mysql://localhost:3306/test")
        .username("root")
        .password("password")
        .mode(WriteMode.INSERT)
        .table("test_table")
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
        writer = new JdbcSinkWriter(mockContext, config);

        // 写入包含隐藏字段的 Row（模拟 Kafka Source 输出的数据）
        Row row = Row.withNames();
        row.setField("id", "1");
        row.setField("name", "Alice");
        row.setField("__topic__", "test-topic");  // 隐藏字段，应该被忽略
        row.setField("__partition__", 0);          // 隐藏字段，应该被忽略
        writer.write(row, null);

        // 验证：SQL 中不应包含 __topic__ 和 __partition__ 字段
        verify(mockConnection).prepareStatement(
            eq("INSERT INTO `test_table` (`name`, `id`) VALUES (?, ?)")
        );
        // 只验证 id 和 name 两个字段，不包含隐藏字段
        verify(mockStatement, times(2)).setObject(anyInt(), any());

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

- [ ] **Step 2: 运行测试验证**

Run: `mvn test -pl flink-etl-sink/flink-etl-sink-jdbc -Dtest=JdbcSinkWriterTest#testInsertModeIgnoresHiddenFields`
Expected: PASS

- [ ] **Step 3: 运行全部测试**

Run: `mvn test -pl flink-etl-sink/flink-etl-sink-jdbc`
Expected: 所有测试通过

- [ ] **Step 4: 提交代码**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java
git commit -m "test(jdbc-sink): 新增隐藏字段过滤测试"
```

---

### Task 3: 更新文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 在 JDBC Sink 部分添加特性说明**

在 `PLUGINS.md` 的 JDBC Sink 部分添加说明：
```markdown
### JDBC Sink

**写入时自动忽略 `__` 开头的隐藏字段**

当数据来源（如 Kafka Source）包含 `__topic__`、`__partition__` 等隐藏字段时，JDBC Sink 会自动过滤这些字段，只写入业务数据字段。
```

- [ ] **Step 2: 提交代码**

```bash
git add PLUGINS.md
git commit -m "docs: 说明 JDBC Sink 自动忽略隐藏字段特性"
```