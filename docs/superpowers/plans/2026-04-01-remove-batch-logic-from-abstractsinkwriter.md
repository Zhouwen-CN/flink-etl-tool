# AbstractSinkWriter 批量逻辑移除实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 AbstractSinkWriter 从批量写入管理器转变为最小化抽象基类，让具体 Sink Writer 自行决定批量行为

**Architecture:** 移除 AbstractSinkWriter 的批量逻辑（batchSize/pendingCount），只保留 context/config 字段访问。JdbcSinkWriter 自行实现批量管理，ConsoleSinkWriter 简化为直接输出。

**Tech Stack:** Java 8, Flink 1.15.2, JUnit 5, Mockito

---

## 文件结构

**修改文件：**
- `flink-etl-core/src/main/java/com/etl/core/sink/AbstractSinkWriter.java` - 核心重构，移除批量逻辑
- `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java` - 新增批量管理代码
- `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkWriter.java` - 简化，移除 hack
- `flink-etl-core/src/main/java/com/etl/core/sink/AbstractSink.java` - 移除 getDefaultBatchSize()
- `flink-etl-core/src/test/java/com/etl/core/sink/AbstractSinkWriterTest.java` - 删除批量测试，新增基础测试
- `flink-etl-sink/flink-etl-sink-console/src/test/java/com/etl/sink/console/ConsoleSinkWriterTest.java` - 简化测试
- `PLUGINS.md` - 更新 Sink 插件开发指南

**新增文件：**
- `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java` - 新增批量逻辑测试类

---

## Task 1: JdbcSinkWriter 批量逻辑测试准备

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java`

- [ ] **Step 1: 创建 JdbcSinkWriterTest 基础测试类**

创建测试类文件，包含基础 setup 方法和 Mock 初始化：

```java
package com.etl.sink.jdbc;

import com.etl.jdbc.sink.configJdbcSinkConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.mockito.Mockito.*;

/**
 * JdbcSinkWriter 批量写入测试
 */
public class JdbcSinkWriterTest {

    @Mock
    protected Sink.InitContext mockContext;

    @Mock
    protected SinkWriterMetricGroup mockMetricGroup;

    @Mock
    protected Connection mockConnection;

    @Mock
    protected PreparedStatement mockStatement;

    @BeforeEach
    public void setUp() {
        mockContext = mock(Sink.InitContext.class);
        mockMetricGroup = mock(SinkWriterMetricGroup.class);

        when(mockContext.getSubtaskId()).thenReturn(0);
        when(mockContext.getNumberOfParallelSubtasks()).thenReturn(1);
        when(mockContext.metricGroup()).thenReturn(mockMetricGroup);
    }

    protected Row createTestRow(String... values) {
        Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;
    }
}
```

- [ ] **Step 2: 提交测试基础类**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java
git commit -m "test(jdbc): 新增 JdbcSinkWriterTest 基础测试类"
```

---

## Task 2: AbstractSinkWriter 重构

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/sink/AbstractSinkWriter.java`

- [ ] **Step 1: 移除批量相关字段和方法**

编辑 `AbstractSinkWriter.java`，删除以下内容：
- 删除 `batchSize` 字段（第 46 行）
- 删除 `pendingCount` 字段（第 49 行）
- 删除 `initialized` 字段（第 52 行）
- 删除 `ensureInitialized()` 方法（第 83-88 行）
- 删除 `open()` 方法（第 98-100 行）
- 删除 `getSubtaskId()` 方法（第 107-109 行）
- 删除 `getNumberOfParallelSubtasks()` 方法（第 116-118 行）
- 删除 `getMetricGroup()` 方法（第 126-128 行）
- 删除 `write()` 模板方法实现（第 140-150 行）
- 删除 `flush()` 模板方法实现（第 172-184 行）
- 删除 `flushBatch()` 抽象方法（第 192 行）
- 删除 `handleFlushFailure()` 方法（第 202-204 行）
- 删除 `close()` 模板方法（第 213-224 行）
- 删除 `cleanup()` 抽象方法（第 232 行）

- [ ] **Step 2: 简化构造函数**

修改构造函数，只接收 context 和 config 参数：

```java
/**
 * 构造函数
 *
 * @param context Writer 初始化上下文
 * @param config Sink 配置对象
 */
public AbstractSinkWriter(Sink.InitContext context, ConfigT config) {
    this.context = context;
    this.config = config;
}
```

删除原构造函数中的 batchSize 参数和校验逻辑（第 65-75 行）。

- [ ] **Step 3: 将 write() 和 flush() 改为抽象方法**

添加抽象方法声明：

```java
/**
 * 写入数据
 * 子类实现此方法定义写入逻辑，自行决定是否需要批量管理
 *
 * @param row 数据行
 * @param context 写入上下文
 * @throws IOException 如果写入失败
 * @throws InterruptedException 如果写入被中断
 */
@Override
public abstract void write(Row row, Context context) throws IOException, InterruptedException;

/**
 * 提交数据
 * 子类实现此方法定义提交逻辑
 *
 * @param endOfInput 是否为输入结束时的 flush
 * @throws IOException 如果提交失败
 * @throws InterruptedException 如果提交被中断
 */
@Override
public abstract void flush(boolean endOfInput) throws IOException, InterruptedException;
```

- [ ] **Step 4: 更新类注释**

更新类级别注释，说明新的设计理念：

```java
/**
 * SinkWriter 抽象基类
 * 简化 Flink SinkWriter API 的实现，提供最小化抽象
 *
 * <p>该类特点：
 * <ul>
 *   <li>InitContext 访问：通过 protected context 字段，子类可直接访问运行时信息</li>
 *   <li>配置管理：通过 protected config 字段，统一管理 Sink 配置参数</li>
 *   <li>最小化抽象：只定义 write 和 flush 抽象方法，具体行为完全由子类实现</li>
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>{@link #write(Row, Context)}：写入数据的逻辑（自行决定是否批量）</li>
 *   <li>{@link #flush(boolean)}：提交数据的逻辑（如批量提交）</li>
 *   <li>{@link SinkWriter#close()}：清理资源的逻辑（如关闭连接）</li>
 * </ul>
 *
 * @param <ConfigT> Sink 配置类型
 */
```

- [ ] **Step 5: 验证编译通过**

```bash
mvn clean compile -DskipTests
```

预期：编译成功，无错误。

- [ ] **Step 6: 提交 AbstractSinkWriter 重构**

```bash
git add flink-etl-core/src/main/java/com/etl/core/sink/AbstractSinkWriter.java
git commit -m "refactor(core): AbstractSinkWriter 移除批量逻辑，简化为最小化抽象基类"
```

---

## Task 3: AbstractSinkWriterTest 重构

**Files:**
- Modify: `flink-etl-core/src/test/java/com/etl/core/sink/AbstractSinkWriterTest.java`

- [ ] **Step 1: 删除批量相关测试方法**

删除以下测试方法：
- `testBatchWriteAndFlush()`（第 57-70 行）
- `testManualFlush()`（第 73-89 行）
- `testCloseWithPendingData()`（第 92-102 行）
- `testInvalidBatchSize()`（第 105-108 行）
- `testFlushFailureHandling()`（第 111-121 行）
- `createWriter(int batchSize)` 工厂方法（第 47 行）

- [ ] **Step 2: 新增 context/config 访问测试**

添加新的测试方法验证基础功能：

```java
@Test
public void testContextAccess() throws Exception {
    AbstractSinkWriter writer = createWriter();

    assertNotNull(writer.context);
    assertEquals(0, writer.context.getSubtaskId());
    assertEquals(1, writer.context.getNumberOfParallelSubtasks());
}

@Test
public void testConfigAccess() throws Exception {
    AbstractSinkWriter writer = createWriter();

    assertNotNull(writer.config);
}
```

- [ ] **Step 3: 运行测试验证**

```bash
mvn test -Dtest=AbstractSinkWriterTest -pl flink-etl-core
```

预期：测试通过（注意：具体实现类的测试可能会失败，因为还未重构）。

- [ ] **Step 4: 提交测试重构**

```bash
git add flink-etl-core/src/test/java/com/etl/core/sink/AbstractSinkWriterTest.java
git commit -m "refactor(core): AbstractSinkWriterTest 删除批量测试，新增基础功能测试"
```

---

## Task 4: JdbcSinkWriter 实现批量管理

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java`

- [ ] **Step 1: 新增批量管理字段**

在类中添加字段：

```java
/** 批量大小 */
private final int batchSize;

/** 待写入数据计数 */
private int pendingCount = 0;
```

- [ ] **Step 2: 修改构造函数初始化批量字段**

修改构造函数，初始化 batchSize 并直接初始化数据库连接：

```java
public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
    super(context, config);
    this.batchSize = config.getBatchSize();

    // 直接初始化数据库连接（不再延迟初始化）
    try {
        connection = DriverManager.getConnection(
            config.getUrl(),
            config.getUsername(),
            config.getPassword()
        );
        connection.setAutoCommit(false);
        log.info("JDBC Sink 已连接: url={}, subtaskId={}", config.getUrl(), context.getSubtaskId());
    } catch (SQLException e) {
        throw new IOException("Failed to initialize JDBC connection", e);
    }
}
```

删除原构造函数中的 `super(context, config, config.getBatchSize())` 调用。

- [ ] **Step 3: 实现 write() 方法，包含自动 flush 判断**

修改 `writeRow()` 方法名为 `write()`，并添加自动 flush 判断：

```java
@Override
public void write(Row row, Context context) throws IOException, InterruptedException {
    try {
        if (statement == null) {
            initStatement(row);
        }

        for (int i = 0; i < columns.length; i++) {
            statement.setObject(i + 1, row.getField(columns[i]));
        }

        statement.addBatch();
        pendingCount++;

        // 达到批量大小时自动 flush
        if (pendingCount >= batchSize) {
            flush(false);
        }
    } catch (SQLException e) {
        throw new IOException("Failed to write row", e);
    }
}
```

删除原有的 `writeRow()` 抽象方法实现。

- [ ] **Step 4: 实现 flush() 方法，包含 rollback 异常处理**

修改 `flushBatch()` 方法名为 `flush()`，并添加 rollback 逻辑：

```java
@Override
public void flush(boolean endOfInput) throws IOException, InterruptedException {
    if (pendingCount == 0) {
        return;
    }

    try {
        int[] results = statement.executeBatch();
        connection.commit();
        pendingCount = 0;

        log.debug("已写入 {} 条记录, subtaskId={}", results.length, this.context.getSubtaskId());
    } catch (SQLException e) {
        // 回滚事务
        try {
            if (connection != null) {
                connection.rollback();
                log.warn("Flush 失败，已回滚事务");
            }
        } catch (SQLException rollbackEx) {
            log.error("回滚失败", rollbackEx);
        }
        throw new IOException("Failed to flush batch", e);
    }
}
```

删除原有的 `flushBatch()` 抽象方法实现和 `handleFlushFailure()` 方法。

- [ ] **Step 5: 实现 close() 方法，包含 flush 和资源清理**

修改 `cleanup()` 方法名为 `close()`，并添加 flush 调用：

```java
@Override
public void close() throws IOException {
    try {
        // 提交剩余数据
        flush(true);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while flushing during close", e);
    } finally {
        // 清理资源
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
            log.info("JDBC Sink 资源清理完成, subtaskId={}", context.getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to cleanup JDBC resources", e);
        }
    }
}
```

删除原有的 `cleanup()` 抽象方法实现。

- [ ] **Step 6: 移除 open() 方法覆盖**

删除 `open()` 方法覆盖（第 35-50 行），因为现在在构造函数中直接初始化。

- [ ] **Step 7: 验证编译通过**

```bash
mvn clean compile -DskipTests -pl flink-etl-sink/flink-etl-sink-jdbc
```

预期：编译成功。

- [ ] **Step 8: 提交 JdbcSinkWriter 重构**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java
git commit -m "refactor(jdbc): JdbcSinkWriter 自行实现批量管理逻辑"
```

---

## Task 5: ConsoleSinkWriter 简化

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkWriter.java`

- [ ] **Step 1: 移除 Integer.MAX_VALUE hack**

修改构造函数，移除 batchSize 参数：

```java
public ConsoleSinkWriter(Sink.InitContext context, boolean showSubtask) throws IOException {
    super(context, true);  // config 参数传入 Boolean.TRUE
    this.showSubtask = showSubtask;
    this.subtaskId = context.getSubtaskId();
    this.totalSubtasks = context.getNumberOfParallelSubtasks();

    log.info("Console Sink Writer 初始化, subtask[{}/{}]", subtaskId + 1, totalSubtasks);
}
```

删除原构造函数中的 `super(context, true, Integer.MAX_VALUE)` 调用。

- [ ] **Step 2: 实现 write() 方法直接输出**

修改 `writeRow()` 方法名为 `write()`：

```java
@Override
public void write(Row row, Context context) throws IOException, InterruptedException {
    if (showSubtask) {
        System.out.printf("[subtask-%d/%d] %s%n", subtaskId + 1, totalSubtasks, row);
    } else {
        System.out.println(row);
    }
}
```

删除原有的 `writeRow()` 抽象方法实现。

- [ ] **Step 3: 实现 flush() 空方法**

修改 `flushBatch()` 方法名为 `flush()`，实现为空操作：

```java
@Override
public void flush(boolean endOfInput) throws IOException, InterruptedException {
    // Console Sink 不需要批量提交，空实现
}
```

删除原有的 `flushBatch()` 空方法实现。

- [ ] **Step 4: 实现 close() 空方法**

修改 `cleanup()` 方法名为 `close()`，实现为空操作：

```java
@Override
public void close() throws IOException {
    log.debug("Console Sink Writer 关闭");
}
```

删除原有的 `cleanup()` 抽象方法实现。

- [ ] **Step 5: 验证编译通过**

```bash
mvn clean compile -DskipTests -pl flink-etl-sink/flink-etl-sink-console
```

预期：编译成功。

- [ ] **Step 6: 提交 ConsoleSinkWriter 简化**

```bash
git add flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkWriter.java
git commit -m "refactor(console): ConsoleSinkWriter 简化，移除 Integer.MAX_VALUE hack"
```

---

## Task 6: AbstractSink 重构

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/sink/AbstractSink.java`

- [ ] **Step 1: 移除 getDefaultBatchSize() 方法**

删除 `getDefaultBatchSize()` 方法（第 58-60 行）：

```java
protected int getDefaultBatchSize() {
    return 100;
}
```

- [ ] **Step 2: 验证编译通过**

```bash
mvn clean compile -DskipTests -pl flink-etl-core
```

预期：编译成功。

- [ ] **Step 3: 提交 AbstractSink 重构**

```bash
git add flink-etl-core/src/main/java/com/etl/core/sink/AbstractSink.java
git commit -m "refactor(core): AbstractSink 移除 getDefaultBatchSize() 方法"
```

---

## Task 7: ConsoleSinkWriterTest 简化

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-console/src/test/java/com/etl/sink/console/ConsoleSinkWriterTest.java`

- [ ] **Step 1: 添加基本输出测试**

新增测试方法验证基本输出功能：

```java
@Test
public void testWriteOutput() throws Exception {
    ConsoleSinkWriter writer = new ConsoleSinkWriter(mockContext, true);

    Row row = createTestRow("value1", "value2");
    writer.write(row, mock(SinkWriter.Context.class));

    // 验证输出到控制台（可以通过 System.out 捕获验证）
}
```

- [ ] **Step 2: 添加 flush 空实现测试**

新增测试方法验证 flush() 空实现：

```java
@Test
public void testFlushDoesNothing() throws Exception {
    ConsoleSinkWriter writer = new ConsoleSinkWriter(mockContext, false);

    writer.flush(false);
    // 无异常即为成功
}
```

- [ ] **Step 3: 添加 close 空实现测试**

新增测试方法验证 close() 空实现：

```java
@Test
public void testCloseDoesNothing() throws Exception {
    ConsoleSinkWriter writer = new ConsoleSinkWriter(mockContext, false);

    writer.close();
    // 无异常即为成功
}
```

- [ ] **Step 4: 运行测试验证**

```bash
mvn test -Dtest=ConsoleSinkWriterTest -pl flink-etl-sink/flink-etl-sink-console
```

预期：测试通过。

- [ ] **Step 5: 提交测试简化**

```bash
git add flink-etl-sink/flink-etl-sink-console/src/test/java/com/etl/sink/console/ConsoleSinkWriterTest.java
git commit -m "refactor(console): ConsoleSinkWriterTest 简化测试"
```

---

## Task 8: 完整测试验证

**Files:**
- 无文件修改，只运行测试

- [ ] **Step 1: 运行所有 Sink 相关测试**

```bash
mvn test -pl flink-etl-core,flink-etl-sink/flink-etl-sink-console,flink-etl-sink/flink-etl-sink-jdbc
```

预期：所有测试通过。

- [ ] **Step 2: 运行完整项目测试**

```bash
mvn clean test
```

预期：所有测试通过。

- [ ] **Step 3: 验证测试覆盖率**

如果项目配置了 JaCoCo，运行覆盖率检查：

```bash
mvn clean test jacoco:report
```

预期：测试覆盖率不低于重构前。

---

## Task 9: 文档更新

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 更新 Sink 插件开发指南章节**

找到 PLUGINS.md 中的"Sink 插件开发指南"章节（约第 461-500 行），更新内容：

```markdown
## Sink 插件开发指南

### 使用新 Sink API

所有新 Sink 插件推荐使用 `AbstractSink` 和 `AbstractSinkWriter` 基类。

#### AbstractSinkWriter 特点

- **最小化抽象**：只提供 context 和 config 字段访问
- **子类完全自主**：自行实现 write()、flush()、close() 方法
- **InitContext 访问**：通过 `context` 字段直接获取运行时信息（subtaskId、并行度、metrics）

#### 开发步骤

1. 创建 Sink 类，继承 `AbstractSink`
2. 在构造函数中进行参数校验和配置对象构建
3. 创建 Writer 类，继承 `AbstractSinkWriter`
4. 实现 `write()` 方法：写入数据逻辑（自行决定是否批量）
5. 实现 `flush()` 方法：提交数据逻辑（如批量提交）
6. 实现 `close()` 方法：清理资源逻辑（如关闭连接）
7. 注册 SPI（使用 `@AutoService(SinkPlugin.class)`）

#### 批量管理

需要批量写入的 Sink（如 JDBC）自行管理：
- 维护 `batchSize` 和 `pendingCount` 字段
- 在 `write()` 中判断是否触发 flush
- 在 `flush()` 中执行批量提交
- 在 `close()` 中提交剩余数据

不需要批量的 Sink（如 Console）：
- `write()` 直接输出
- `flush()` 空实现
- `close()` 空实现或简单清理

### InitContext 使用

Writer 可以通过 `context` 字段访问：
- `context.getSubtaskId()` - 获取子任务 ID
- `context.getNumberOfParallelSubtasks()` - 获取总并行度
- `context.metricGroup()` - 获取度量组（用于上报指标）

### 异常处理

- `write()` 失败 → 抛出 IOException，Flink 从 checkpoint 重试
- `flush()` 失败 → 自行处理异常（如 rollback），然后抛出 IOException
- `close()` 时 flush 失败 → 抛出异常，任务失败
```

- [ ] **Step 2: 提交文档更新**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md Sink 插件开发指南"
```

---

## Task 10: 验收测试

**Files:**
- 无文件修改，运行验收测试

- [ ] **Step 1: 验证 JDBC Sink 批量写入功能**

运行现有的 JDBC Sink 测试或示例配置：

```bash
mvn test -Dtest=JdbcSinkTest -pl flink-etl-sink/flink-etl-sink-jdbc
```

预期：批量写入功能正常。

- [ ] **Step 2: 验证 Console Sink 输出功能**

运行 Console Sink 测试：

```bash
mvn test -Dtest=ConsoleSinkTest -pl flink-etl-sink/flink-etl-sink-console
```

预期：输出功能正常。

- [ ] **Step 3: 检查代码质量**

检查关键文件：
- AbstractSinkWriter 行数减少，职责清晰
- ConsoleSinkWriter 不包含 Integer.MAX_VALUE
- JdbcSinkWriter 批量逻辑清晰，有完整注释

- [ ] **Step 4: 检查文档准确性**

阅读 PLUGINS.md，确保文档描述与代码一致。

---

## Task 11: 最终提交和清理

**Files:**
- 无文件修改，清理和总结

- [ ] **Step 1: 查看所有 commit 历史**

```bash
git log --oneline -n 10
```

预期：看到所有重构相关的 commit。

- [ ] **Step 2: 确认所有改动已提交**

```bash
git status
```

预期：工作目录干净，无未提交文件。

- [ ] **Step 3: 推送到远程分支（可选）**

如果需要推送：

```bash
git push origin master
```

---

## 总结

**重构完成标准：**
- ✅ AbstractSinkWriter 只保留 context/config 字段和抽象方法
- ✅ JdbcSinkWriter 自行管理批量逻辑
- ✅ ConsoleSinkWriter 简化，移除 hack
- ✅ 所有测试通过
- ✅ 文档更新完成

**关键变更：**
- AbstractSinkWriter 从 233 行减少到约 50 行
- JdbcSinkWriter 新增约 30 行批量管理代码
- ConsoleSinkWriter 简化约 10 行
- 设计清晰，职责分离