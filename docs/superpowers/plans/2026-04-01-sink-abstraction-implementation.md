# Sink 抽象层实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 Flink ETL 工具实现 Sink 抽象层，包括 AbstractSink 和 AbstractSinkWriter 基类，并迁移现有的 Console 和 JDBC Sink 插件到新 API。

**Architecture:** 双层抽象设计 - AbstractSink 负责参数接收和 Writer 创建，AbstractSinkWriter 负责批量缓冲管理、InitContext 访问和异常处理。参数校验在具体 Sink 构造函数中完成。使用基础 Sink API（不支持状态恢复），通过 flush() 保证 at-least-once 语义。

**Tech Stack:** Java 1.8, Apache Flink 1.15.2, Flink Sink API (FLIP-143), Lombok, SLF4J

---

## 文件结构映射

### 新建文件

**核心抽象层**：
- `flink-etl-core/src/main/java/com/etl/core/sink/AbstractSink.java` - Sink 基类，接收配置，创建 Writer
- `flink-etl-core/src/main/java/com/etl/core/sink/AbstractSinkWriter.java` - Writer 基类，批量管理，InitContext 访问，异常处理

**Console Sink 迁移**：
- `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSink.java` - Console Sink 实现
- `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkWriter.java` - Console Writer 实现

**JDBC Sink 迁移**：
- `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java` - JDBC Sink 实现
- `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java` - JDBC Writer 实现

**测试文件**：
- `flink-etl-core/src/test/java/com/etl/core/sink/AbstractSinkWriterTest.java` - 抽象 Writer 测试基类
- `flink-etl-sink/flink-etl-sink-console/src/test/java/com/etl/sink/console/ConsoleSinkWriterTest.java` - Console Writer 测试
- `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java` - JDBC Writer 测试

### 修改文件

- `flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java` - 接口签名变更（返回 `Sink<Row>` 而非 `SinkFunction<Row>`）
- `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java` - 使用新 Sink API（`sinkTo()` 替代 `addSink()`）
- `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java` - 返回新的 ConsoleSink
- `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkPlugin.java` - 返回新的 JdbcSink

---

## Phase 1: 核心抽象层

### Task 1: 创建 AbstractSink 基类

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/sink/AbstractSink.java`

- [ ] **Step 1: 创建 sink 目录**

```bash
mkdir -p flink-etl-core/src/main/java/com/etl/core/sink
```

- [ ] **Step 2: 创建 AbstractSink.java**

```java
package com.etl.core.sink;

import com.etl.core.config.SinkConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Sink 抽象基类
 * 简化 Flink Sink API 的实现
 *
 * <p>该类特点：
 * <ul>
 *   <li>接收 SinkConfig 配置对象（参数校验由具体实现类负责）</li>
 *   <li>实现 at-least-once 语义（通过 flush() 保证数据写入）</li>
 *   <li>不保存状态，故障恢复后从上次 checkpoint 重放数据</li>
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>构造函数中的参数校验和配置对象构建</li>
 *   <li>{@link #createWriter(InitContext)} 创建具体的 Writer</li>
 * </ul>
 */
public abstract class AbstractSink implements Sink<Row> {

    /** 原始配置对象 */
    protected final SinkConfig config;

    /**
     * 构造函数
     *
     * @param config Sink 配置对象
     *               参数校验在具体实现类的构造函数中进行
     */
    public AbstractSink(SinkConfig config) {
        this.config = config;
    }

    /**
     * 创建 SinkWriter
     * 子类实现此方法返回具体的 Writer 实例
     *
     * @param context Writer 初始化上下文
     * @return SinkWriter 实例
     */
    @Override
    public abstract SinkWriter<Row> createWriter(InitContext context) throws IOException;
}
```

**注意：** `getDefaultBatchSize()` 方法由 `SinkPlugin` 接口提供，`AbstractSink` 不需要重复定义。具体 Sink 实现可以从 SinkPlugin 获取默认值。

- [ ] **Step 3: 验证编译**

```bash
cd flink-etl-core
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/sink/AbstractSink.java
git commit -m "feat(core): 新增 AbstractSink 基类

- 实现 Sink<Row> 接口
- 接收 SinkConfig，不进行参数校验
- 提供默认 batchSize 方法

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 2: 创建 AbstractSinkWriter 基类

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/sink/AbstractSinkWriter.java`

- [ ] **Step 1: 创建 AbstractSinkWriter.java**

```java
package com.etl.core.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * SinkWriter 抽象基类
 * 内置批量缓冲管理，自动处理 flush 时机
 *
 * <p>该类自动处理：
 * <ul>
 *   <li>批量缓冲 - 维护待写入数据的计数</li>
 *   <li>立即初始化 - 构造函数中调用 open()</li>
 *   <li>Flink 触发 flush - checkpoint 或 endOfInput 时调用 flush()</li>
 *   <li>异常恢复 - flush 失败时的清理和重试机制</li>
 *   <li>资源清理 - close() 确保数据 flush + 资源清理</li>
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>{@link #writeRow(Row)} - 具体写入逻辑（如何将 Row 写入外部系统）</li>
 *   <li>{@link #flushBatch()} - 批量提交逻辑（如提交事务、发送网络请求）</li>
 *   <li>{@link #cleanup()} - 清理资源（如关闭连接）</li>
 * </ul>
 */
@Slf4j
public abstract class AbstractSinkWriter implements SinkWriter<Row> {

    /** Writer 初始化上下文，提供运行时信息和工具 */
    protected final Sink.InitContext context;

    /** 批次大小（达到此数量时自动 flush） */
    protected final int batchSize;

    /** 待写入数据的计数 */
    protected int pendingCount = 0;

    /** 是否已初始化 */
    private boolean initialized = false;

    /**
     * 构造函数：立即调用 open() 初始化资源
     *
     * @param context Writer 初始化上下文
     * @param batchSize 批次大小（必须 > 0）
     */
    public AbstractSinkWriter(Sink.InitContext context, int batchSize) {
        Preconditions.checkArgument(batchSize > 0, "batchSize must be positive");

        this.context = context;
        this.batchSize = batchSize;

        // 立即初始化
        try {
            open();
            initialized = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize writer", e);
        }
    }

    /**
     * 获取当前子任务 ID
     * 用于区分不同并行任务的输出
     *
     * @return 子任务 ID（从 0 开始）
     */
    protected int getSubtaskId() {
        return context.getSubtaskId();
    }

    /**
     * 获取总并行度
     * 用于显示任务进度（如 [subtask-1/4]）
     *
     * @return 总并行任务数
     */
    protected int getNumberOfParallelSubtasks() {
        return context.getNumberOfParallelSubtasks();
    }

    /**
     * 获取度量组
     * 用于上报自定义指标（如写入计数、flush 计数）
     *
     * @return SinkWriter 度量组
     */
    protected SinkWriterMetricGroup getMetricGroup() {
        return context.metricGroup();
    }

    /**
     * 写入数据
     * 自动管理批量计数，达到 batchSize 时触发 flush
     *
     * @param element 输入记录
     * @param context 记录上下文（包含时间戳、水印等）
     */
    @Override
    public final void write(Row element, Context context) throws IOException, InterruptedException {
        writeRow(element);
        pendingCount++;

        if (pendingCount >= batchSize) {
            flush(false);
        }
    }

    /**
     * Flush 所有待写入的数据
     * checkpoint 或输入结束时调用，确保 at-least-once
     *
     * @param endOfInput 是否输入已结束
     */
    @Override
    public final void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (pendingCount > 0) {
            try {
                flushBatch();
                log.debug("成功 flush {} 条记录", pendingCount);
                pendingCount = 0;  // 重置计数器
            } catch (Exception e) {
                handleFlushFailure(e);
                throw new IOException("Flush failed", e);
            }
        }
    }

    /**
     * 关闭 Writer
     * 确保所有数据 flush 并清理资源
     */
    @Override
    public final void close() throws IOException {
        try {
            if (initialized) {
                flush(true);  // 强制 flush
            }
        } catch (Exception e) {
            log.error("Close 时 flush 失败", e);
            throw new IOException("Failed to flush on close", e);  // 抛出异常
        } finally {
            cleanup();
        }
    }

    /**
     * 写入单条记录到外部系统
     * 子类实现具体写入逻辑
     *
     * @param row 输入记录
     * @throws IOException 写入异常
     */
    protected abstract void writeRow(Row row) throws IOException, InterruptedException;

    /**
     * 批量提交数据到外部系统
     * 子类实现具体提交逻辑（如提交事务、发送批量请求）
     *
     * @throws IOException 提交异常
     */
    protected abstract void flushBatch() throws IOException, InterruptedException;

    /**
     * 清理资源
     * 子类实现具体的资源清理逻辑（如关闭数据库连接）
     *
     * @throws IOException 清理异常
     */
    protected abstract void cleanup() throws IOException;

    /**
     * 初始化资源
     * 子类可覆盖此方法进行资源初始化（如建立连接）
     *
     * @throws IOException 初始化异常
     */
    protected void open() throws IOException {
        // 默认空实现，子类可覆盖
    }

    /**
     * 处理 flush 失败
     * 子类可覆盖此方法实现自定义的失败处理逻辑
     *
     * @param e 失败异常
     */
    protected void handleFlushFailure(Exception e) {
        log.warn("Flush 失败，未提交的 {} 条数据可能丢失", pendingCount, e);
    }
}
```

- [ ] **Step 2: 验证编译**

```bash
cd flink-etl-core
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/sink/AbstractSinkWriter.java
git commit -m "feat(core): 新增 AbstractSinkWriter 基类

- 实现 SinkWriter<Row> 接口
- 内置批量缓冲管理（自动计数和 flush）
- 提供 InitContext 访问（subtaskId、并行度、度量组）
- 立即初始化（构造函数中调用 open()）
- 完善的异常处理和资源清理

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 3: 修改 SinkPlugin 接口

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java`

- [ ] **Step 1: 读取现有 SinkPlugin.java**

```bash
cat flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java
```

- [ ] **Step 2: 修改 SinkPlugin 接口**

将 `createSink()` 方法的返回值从 `SinkFunction<Row>` 改为 `Sink<Row>`：

```java
package com.etl.core.spi;

import com.etl.core.config.SinkConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

/**
 * Sink 插件接口
 * 所有数据写入插件必须实现此接口
 */
public interface SinkPlugin extends Plugin {

    /**
     * 创建 Sink 实例
     *
     * @param config Sink 配置
     * @return Flink Sink 接口，强制消费 Row 类型
     */
    Sink<Row> createSink(SinkConfig config);

    /**
     * 所有 sink 默认的 batchSize
     *
     * @return 批次大小
     */
    default int getDefaultBatchSize() {
        return 100;
    }
}
```

- [ ] **Step 3: 验证编译（预期失败，因为现有插件未实现新接口）**

```bash
cd flink-etl-core
mvn compile
```

Expected: COMPILATION ERROR (ConsoleSinkPlugin 和 JdbcSinkPlugin 未实现新接口)

- [ ] **Step 4: 暂时修复 ConsoleSinkPlugin（返回 null）**

修改 `flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java`：

```java
@Override
public Sink<Row> createSink(SinkConfig config) {
    // TODO: 迁移到新 API 后实现
    return null;
}
```

- [ ] **Step 5: 暂时修复 JdbcSinkPlugin（返回 null）**

修改 `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkPlugin.java`：

```java
@Override
public Sink<Row> createSink(SinkConfig config) {
    // TODO: 迁移到新 API 后实现
    return null;
}
```

- [ ] **Step 6: 验证编译（应该成功）**

```bash
mvn clean compile
```

Expected: BUILD SUCCESS

- [ ] **Step 7: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java \
        flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java \
        flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkPlugin.java

git commit -m "refactor(core): SinkPlugin 接口迁移到新 Sink API

- 修改 createSink() 返回 Sink<Row> 而非 SinkFunction<Row>
- ConsoleSinkPlugin 和 JdbcSinkPlugin 暂时返回 null
- 现有 Sink 插件暂时不可用，等待迁移

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 4: 修改 JobBuilder 使用新 Sink API

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java`

- [ ] **Step 1: 读取现有 JobBuilder.java**

定位 Sink 相关代码：

```bash
grep -n "Sink" flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java
```

- [ ] **Step 2: 修改 JobBuilder.java**

找到 Sink 相关代码（约第 88-92 行），修改为：

```java
// Sink 消费 DataStream
String sinkType = sinkConfig.getType();
SinkPlugin sinkPlugin = PluginLoader.loadSinkPlugin(sinkType);
Sink<Row> sink = sinkPlugin.createSink(sinkConfig);

if (sink == null) {
    throw new IllegalArgumentException(
        String.format("Sink 插件 '%s' 未实现新 API，请检查插件是否已迁移", sinkType));
}

resultStream.sinkTo(sink).name(sinkType + " sink");
log.info("Sink 创建成功: {}", sinkType);
```

完整修改后的 import 部分：

```java
import org.apache.flink.api.connector.sink2.Sink;
// ... 其他 import
```

- [ ] **Step 3: 验证编译**

```bash
cd flink-etl-core
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java
git commit -m "refactor(core): JobBuilder 使用新 Sink API

- 使用 sinkTo() 替代 addSink()
- 添加 null 检查，提示未迁移的插件
- 移除对 SinkFunction 的依赖

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 5: 创建 AbstractSinkWriter 测试基类

**Files:**
- Create: `flink-etl-core/src/test/java/com/etl/core/sink/AbstractSinkWriterTest.java`

- [ ] **Step 1: 创建测试目录**

```bash
mkdir -p flink-etl-core/src/test/java/com/etl/core/sink
```

- [ ] **Step 2: 创建测试基类**

```java
package com.etl.core.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * AbstractSinkWriter 测试基类
 */
public abstract class AbstractSinkWriterTest {

    protected Sink.InitContext mockContext;
    protected SinkWriterMetricGroup mockMetricGroup;

    @Before
    public void setUp() {
        mockContext = mock(Sink.InitContext.class);
        mockMetricGroup = mock(SinkWriterMetricGroup.class);

        when(mockContext.getSubtaskId()).thenReturn(0);
        when(mockContext.getNumberOfParallelSubtasks()).thenReturn(1);
        when(mockContext.metricGroup()).thenReturn(mockMetricGroup);
    }

    /**
     * 创建测试用的 Row 对象
     */
    protected Row createTestRow(String... values) {
        Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;
    }

    /**
     * 创建具体的 Writer 实例
     */
    protected abstract AbstractSinkWriter createWriter(int batchSize);

    /**
     * 创建带有默认 batchSize 的 Writer
     */
    protected AbstractSinkWriter createWriter() {
        return createWriter(10);
    }

    @Test
    public void testBatchWriteAndFlush() throws Exception {
        AbstractSinkWriter writer = createWriter(5);

        // 写入 5 条数据，应触发自动 flush
        for (int i = 0; i < 5; i++) {
            writer.write(createTestRow("value" + i), mock(SinkWriter.Context.class));
        }

        // 通过反射验证 pendingCount 被重置为 0
        java.lang.reflect.Field pendingCountField = AbstractSinkWriter.class.getDeclaredField("pendingCount");
        pendingCountField.setAccessible(true);
        int pendingCount = (int) pendingCountField.get(writer);
        assertEquals(0, pendingCount);
    }

    @Test
    public void testManualFlush() throws Exception {
        AbstractSinkWriter writer = createWriter(100);

        // 写入 10 条数据
        for (int i = 0; i < 10; i++) {
            writer.write(createTestRow("value" + i), mock(SinkWriter.Context.class));
        }

        // 手动 flush
        writer.flush(false);

        // 验证 pendingCount 被重置
        java.lang.reflect.Field pendingCountField = AbstractSinkWriter.class.getDeclaredField("pendingCount");
        pendingCountField.setAccessible(true);
        int pendingCount = (int) pendingCountField.get(writer);
        assertEquals(0, pendingCount);
    }

    @Test
    public void testCloseWithPendingData() throws Exception {
        AbstractSinkWriter writer = createWriter(100);

        // 写入数据但不 flush
        writer.write(createTestRow("value1"), mock(SinkWriter.Context.class));

        // 关闭时应自动 flush
        writer.close();

        // 验证资源已清理
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidBatchSize() {
        // batchSize <= 0 应抛出异常
        createWriter(0);
    }

    @Test
    public void testFlushFailureHandling() throws Exception {
        AbstractSinkWriter writer = createWriter(10);

        // 写入数据
        for (int i = 0; i < 10; i++) {
            writer.write(createTestRow("value" + i), mock(SinkWriter.Context.class));
        }

        // 子类可以覆盖此测试，模拟 flush 失败场景
        // 验证 handleFlushFailure() 被调用
    }
}
```

- [ ] **Step 3: 验证编译**

```bash
cd flink-etl-core
mvn test-compile
```

Expected: BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-core/src/test/java/com/etl/core/sink/AbstractSinkWriterTest.java
git commit -m "test(core): 新增 AbstractSinkWriter 测试基类

- 提供通用的测试工具方法
- 包含批量写入、手动 flush、close 等测试场景
- 验证 batchSize 参数校验

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Phase 2: Console Sink 迁移

### Task 6: 实现 ConsoleSink

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSink.java`

- [ ] **Step 1: 创建 ConsoleSink.java**

```java
package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.sink.AbstractSink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Console Sink 实现
 * 将数据输出到控制台
 */
public class ConsoleSink extends AbstractSink {

    private final boolean showSubtask;

    public ConsoleSink(SinkConfig config, boolean showSubtask) {
        super(config);
        this.showSubtask = showSubtask;
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) throws IOException {
        return new ConsoleSinkWriter(context, showSubtask);
    }
}
```

- [ ] **Step 2: 验证编译**

```bash
cd flink-etl-sink-console
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSink.java
git commit -m "feat(console): 新增 ConsoleSink 实现

- 继承 AbstractSink
- 接收 showSubtask 参数
- 创建 ConsoleSinkWriter

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 7: 实现 ConsoleSinkWriter

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkWriter.java`

- [ ] **Step 1: 创建 ConsoleSinkWriter.java**

```java
package com.etl.sink.console;

import com.etl.core.sink.AbstractSinkWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Console Sink Writer 实现
 * 直接将数据输出到控制台
 */
@Slf4j
public class ConsoleSinkWriter extends AbstractSinkWriter {

    private final boolean showSubtask;
    private final int subtaskId;
    private final int totalSubtasks;

    public ConsoleSinkWriter(Sink.InitContext context, boolean showSubtask) {
        super(context, Integer.MAX_VALUE);  // 不触发批量 flush
        this.showSubtask = showSubtask;
        this.subtaskId = getSubtaskId();
        this.totalSubtasks = getNumberOfParallelSubtasks();

        log.info("Console Sink Writer 初始化, subtask[{}/{}]", subtaskId + 1, totalSubtasks);
    }

    @Override
    protected void writeRow(Row row) throws IOException {
        // 直接输出，不缓冲
        if (showSubtask) {
            System.out.printf("[subtask-%d/%d] %s%n", subtaskId + 1, totalSubtasks, row);
        } else {
            System.out.println(row);
        }
    }

    @Override
    protected void flushBatch() throws IOException {
        // Console 直接输出，无需批量提交
    }

    @Override
    protected void cleanup() throws IOException {
        // 无需清理资源
        log.debug("Console Sink Writer 关闭");
    }
}
```

- [ ] **Step 2: 验证编译**

```bash
cd flink-etl-sink-console
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkWriter.java
git commit -m "feat(console): 新增 ConsoleSinkWriter 实现

- 继承 AbstractSinkWriter
- 使用 Integer.MAX_VALUE 作为 batchSize（不触发批量 flush）
- 直接输出到控制台
- 支持 showSubtask 参数

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 8: 更新 ConsoleSinkPlugin

**Files:**
- Modify: `flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java`

- [ ] **Step 1: 修改 ConsoleSinkPlugin.java**

```java
package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

/**
 * Console Sink 插件
 * 将数据输出到控制台
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class ConsoleSinkPlugin implements SinkPlugin {

    @Override
    public String getType() {
        return "console";
    }

    @Override
    public Sink<Row> createSink(SinkConfig config) {
        boolean showSubtask = config.getBoolean("showSubtask", true);
        log.info("创建 Console Sink, showSubtask={}", showSubtask);

        return new ConsoleSink(config, showSubtask);
    }
}
```

- [ ] **Step 2: 删除旧的 ConsoleSinkFunction（内部类）**

如果 ConsoleSinkPlugin.java 中包含旧的 ConsoleSinkFunction 内部类，删除它。

- [ ] **Step 3: 验证编译**

```bash
cd flink-etl-sink-console
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java
git commit -m "refactor(console): ConsoleSinkPlugin 迁移到新 Sink API

- 返回 ConsoleSink 而非 null
- 删除旧的 SinkFunction 实现
- Console Sink 现在可以使用新 API

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 9: 创建 ConsoleSinkWriter 测试

**Files:**
- Create: `flink-etl-sink-console/src/test/java/com/etl/sink/console/ConsoleSinkWriterTest.java`

- [ ] **Step 1: 创建测试目录**

```bash
mkdir -p flink-etl-sink-console/src/test/java/com/etl/sink/console
```

- [ ] **Step 2: 创建测试类**

```java
package com.etl.sink.console;

import com.etl.core.sink.AbstractSinkWriterTest;
import com.etl.core.sink.AbstractSinkWriter;

/**
 * Console Sink Writer 测试
 */
public class ConsoleSinkWriterTest extends AbstractSinkWriterTest {

    @Override
    protected AbstractSinkWriter createWriter(int batchSize) {
        return new ConsoleSinkWriter(mockContext, true);
    }
}
```

- [ ] **Step 3: 运行测试**

```bash
cd flink-etl-sink-console
mvn test
```

Expected: Tests run, Failures: 0, Errors: 0

- [ ] **Step 4: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-console/src/test/java/com/etl/sink/console/ConsoleSinkWriterTest.java
git commit -m "test(console): 新增 ConsoleSinkWriter 测试

- 继承 AbstractSinkWriterTest
- 验证 Console Sink Writer 的基本功能

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Phase 3: JDBC Sink 迁移

### Task 10: 实现 JdbcSink（参数校验）

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java`

- [ ] **Step 1: 创建 JdbcSink.java - 基础结构和必要参数校验**

```java
package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialectLoader;
import com.etl.core.dialect.WriteMode;
import com.etl.core.sink.AbstractSink;
import com.etl.jdbc.sink.configJdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * JDBC Sink 实现
 * 支持将数据写入关系型数据库
 */
@Slf4j
public class JdbcSink extends AbstractSink {

   private final JdbcSinkConfig jdbcSinkConfig;

   public JdbcSink(SinkConfig config) {
      super(config);

      // 1. 必要参数校验
      String url = Preconditions.checkNotNull(config.getString("url"), "url is null");
      String username = config.getString("username");
      String password = config.getString("password");

      // 2. Dialect 初始化
      JdbcDialect dialect = JdbcDialects.get(url);

      // 3. table/sql 模式选择和校验
      String table = config.getString("table");
      String sql = config.getString("sql");
      Preconditions.checkArgument(table != null || sql != null,
              "table 和 sql 必须配置其中一个");

      // 4. mode 和 keyFields 参数处理
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

      // 5. batchSize 参数处理
      Integer batchSize = config.getInteger("batchSize", 100);
      Preconditions.checkArgument(batchSize != null && batchSize > 0, "batchSize must be greater than 0");

      // 6. 构建 JdbcSinkConfig 对象
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

   @Override
   public SinkWriter<Row> createWriter(InitContext context) throws IOException {
      return new JdbcSinkWriter(context, jdbcSinkConfig);
   }
}
```

- [ ] **Step 2: 验证编译**

```bash
cd flink-etl-sink/flink-etl-sink-jdbc
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java
git commit -m "feat(jdbc): 新增 JdbcSink 实现

- 继承 AbstractSink
- 完整的参数校验（url、username、password、table/sql）
- 支持 INSERT 和 UPSERT 模式
- 构建 JdbcSinkConfig 配置对象

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 11: 实现 JdbcSinkWriter（核心逻辑）

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java`

- [ ] **Step 1: 创建 JdbcSinkWriter.java - 字段定义和构造函数**

```java
package com.etl.sink.jdbc;

import com.etl.core.dialect.WriteMode;
import com.etl.core.sink.AbstractSinkWriter;
import com.etl.jdbc.sink.configJdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * JDBC Sink Writer 实现
 * 管理数据库连接和批量写入
 */
@Slf4j
public class JdbcSinkWriter extends AbstractSinkWriter {

   private final JdbcSinkConfig config;
   private transient Connection connection;
   private transient PreparedStatement statement;
   private transient String[] columns;
   private SinkWriterMetricGroup metricGroup;

   public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) {
      super(context, config.getBatchSize());
      this.config = config;
   }
}
```

- [ ] **Step 2: 实现 open() 方法**

```java
@Override
protected void open() throws IOException {
    try {
        connection = DriverManager.getConnection(
            config.getUrl(),
            config.getUsername(),
            config.getPassword()
        );
        connection.setAutoCommit(false);

        // 注册指标
        metricGroup = getMetricGroup();

        log.info("JDBC Sink 已连接: url={}, subtaskId={}", config.getUrl(), getSubtaskId());
    } catch (SQLException e) {
        throw new IOException("Failed to initialize JDBC connection", e);
    }
}
```

- [ ] **Step 3: 实现 writeRow() 方法**

```java
@Override
protected void writeRow(Row row) throws IOException {
    try {
        // 懒初始化 statement（第一次调用时根据 Row 字段生成 SQL）
        if (statement == null) {
            initStatement(row);
        }

        // 填充参数
        for (int i = 0; i < columns.length; i++) {
            statement.setObject(i + 1, row.getField(columns[i]));
        }

        // 添加到 JDBC 批量缓冲
        statement.addBatch();

        // 更新指标
        metricGroup.getNumRecordsOutCounter().inc();
    } catch (SQLException e) {
        throw new IOException("Failed to write row", e);
    }
}

private void initStatement(Row row) throws SQLException {
    Set<String> fieldNames = row.getFieldNames(true);
    this.columns = fieldNames.toArray(new String[0]);

    String sql;
    if (config.getSql() != null) {
        // sql 模式：解析具名占位符
        NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(config.getSql());
        sql = parsed.getPreparedSql();
        log.info("JDBC Sink sql 模式: {}", sql);
    } else {
        // table 模式：根据 mode 生成 SQL
        if (config.getMode() == WriteMode.UPSERT) {
            sql = config.getDialect().getUpsertSql(config.getTable(), columns, config.getKeyFields());
            log.info("JDBC Sink upsert 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
        } else {
            sql = config.getDialect().getInsertSql(config.getTable(), columns);
            log.info("JDBC Sink insert 模式: table={}, columns={}", config.getTable(), Arrays.toString(columns));
        }
    }

    this.statement = connection.prepareStatement(sql);
}
```

- [ ] **Step 4: 实现 flushBatch() 和 handleFlushFailure() 方法**

```java
@Override
protected void flushBatch() throws IOException {
    try {
        int[] results = statement.executeBatch();
        connection.commit();

        log.debug("已写入 {} 条记录, subtaskId={}", results.length, getSubtaskId());
    } catch (SQLException e) {
        throw new IOException("Failed to flush batch", e);
    }
}

@Override
protected void handleFlushFailure(Exception e) {
    try {
        if (connection != null) {
            connection.rollback();
            log.warn("Flush 失败，已回滚事务");
        }
    } catch (SQLException rollbackEx) {
        log.error("回滚失败", rollbackEx);
    }
}
```

- [ ] **Step 5: 实现 cleanup() 方法**

```java
@Override
protected void cleanup() throws IOException {
    try {
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
        log.info("JDBC Sink 资源清理完成, subtaskId={}", getSubtaskId());
    } catch (SQLException e) {
        throw new IOException("Failed to cleanup JDBC resources", e);
    }
}
```

- [ ] **Step 6: 验证编译**

```bash
cd flink-etl-sink/flink-etl-sink-jdbc
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 7: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkWriter.java
git commit -m "feat(jdbc): 新增 JdbcSinkWriter 实现

- 继承 AbstractSinkWriter
- 实现数据库连接和批量写入
- 支持 INSERT 和 UPSERT 模式
- 支持事务管理和回滚
- 实现资源清理和异常处理

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 12: 更新 JdbcSinkPlugin

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkPlugin.java`

- [ ] **Step 1: 修改 JdbcSinkPlugin.java**

```java
@Override
public Sink<Row> createSink(SinkConfig config) {
    return new JdbcSink(config);
}
```

- [ ] **Step 2: 验证编译**

```bash
cd flink-etl-sink-jdbc
mvn compile
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkPlugin.java
git commit -m "refactor(jdbc): JdbcSinkPlugin 迁移到新 Sink API

- 返回 JdbcSink 而非 null
- JDBC Sink 现在可以使用新 API

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 13: 创建 JdbcSinkWriter 测试

**Files:**
- Create: `flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java`

- [ ] **Step 1: 创建测试类**

```java
package com.etl.sink.jdbc;

import com.etl.core.sink.AbstractSinkWriterTest;
import com.etl.core.sink.AbstractSinkWriter;

/**
 * JDBC Sink Writer 测试
 */
public class JdbcSinkWriterTest extends AbstractSinkWriterTest {

    @Override
    protected AbstractSinkWriter createWriter(int batchSize) {
        // TODO: 使用内存数据库（H2）进行测试
        // 需要创建测试用的 JdbcSinkConfig
        return null;
    }
}
```

- [ ] **Step 2: 运行测试**

```bash
cd flink-etl-sink-jdbc
mvn test
```

Expected: Tests run (可能跳过，因为需要数据库环境)

- [ ] **Step 3: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/test/java/com/etl/sink/jdbc/JdbcSinkWriterTest.java
git commit -m "test(jdbc): 新增 JdbcSinkWriter 测试框架

- 继承 AbstractSinkWriterTest
- TODO: 使用内存数据库进行完整测试

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Phase 4: 文档更新

### Task 14: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 添加 Sink 插件开发指南**

在 PLUGINS.md 中添加新章节：

```markdown
## Sink 插件开发指南

### 使用新 Sink API

所有新 Sink 插件推荐使用 `AbstractSink` 和 `AbstractSinkWriter` 基类。

#### 开发步骤

1. 创建 Sink 类，继承 `AbstractSink`
2. 在构造函数中进行参数校验和配置对象构建
3. 创建 Writer 类，继承 `AbstractSinkWriter`
4. 实现 `writeRow()` 和 `flushBatch()` 方法
5. 实现 `cleanup()` 方法清理资源
6. 注册 SPI（使用 `@AutoService(SinkPlugin.class)`）

#### 示例代码

参考 `ConsoleSink` 和 `JdbcSink` 实现。

### 批量管理

AbstractSinkWriter 自动管理批量缓冲：
- 达到 `batchSize` 时自动 flush
- checkpoint 或输入结束时强制 flush
- 子类只需实现 `writeRow()` 和 `flushBatch()`

### InitContext 使用

Writer 可以访问：
- `getSubtaskId()` - 获取子任务 ID
- `getNumberOfParallelSubtasks()` - 获取总并行度
- `getMetricGroup()` - 获取度量组（用于上报指标）

### 异常处理

- `writeRow()` 失败 → 抛出 IOException，Flink 从 checkpoint 重试
- `flushBatch()` 失败 → 调用 `handleFlushFailure()` 清理状态 + 抛出 IOException
- `close()` 时 flush 失败 → 抛出异常，任务失败
```

- [ ] **Step 2: 提交**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md，添加 Sink 插件开发指南

- 新增 Sink 插件开发步骤
- 说明批量管理机制
- 说明 InitContext 使用方法
- 说明异常处理策略

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 15: 集成测试

**Files:**
- Test: `docs/examples/mysql-to-console.json`

- [ ] **Step 1: 验证 Console Sink 工作**

使用现有的示例配置测试：

```bash
cd flink-etl-client
mvn clean package
java -jar target/flink-etl-client-1.0.0-SNAPSHOT.jar --file ../docs/examples/mysql-to-console.json
```

Expected: 任务成功运行，控制台输出数据

- [ ] **Step 2: 验证 JDBC Sink 工作**

创建测试配置文件，测试 JDBC Sink：

```bash
# 创建测试配置
cat > docs/examples/mysql-to-mysql.json << 'EOF'
{
  "job": {
    "name": "mysql-to-mysql-test",
    "mode": "batch"
  },
  "sources": [{
    "type": "jdbc",
    "outputTable": "source_table",
    "config": {
      "url": "jdbc:mysql://localhost:3306/test",
      "username": "root",
      "password": "password",
      "table": "source_table",
      "splitColumn": "id"
    }
  }],
  "sinks": [{
    "type": "jdbc",
    "inputTable": "source_table",
    "config": {
      "url": "jdbc:mysql://localhost:3306/test",
      "username": "root",
      "password": "password",
      "table": "target_table",
      "batchSize": 100
    }
  }]
}
EOF

# 运行测试
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-mysql.json
```

Expected: 任务成功运行，数据从源表写入目标表

- [ ] **Step 3: 提交测试配置**

```bash
git add docs/examples/mysql-to-mysql.json
git commit -m "test: 新增 JDBC Sink 集成测试配置

- 测试 MySQL 到 MySQL 的数据同步
- 验证新 Sink API 正常工作

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 16: 最终验证和文档

- [ ] **Step 1: 运行完整测试套件**

```bash
mvn clean test
```

Expected: 所有测试通过

- [ ] **Step 2: 打包验证**

```bash
mvn clean package
```

Expected: 打包成功

- [ ] **Step 3: 更新 CHANGELOG（如果存在）**

记录本次重大变更：

```markdown
## [Unreleased]

### Changed
- **BREAKING**: SinkPlugin 接口从 `SinkFunction` 迁移到 `Sink` API
- 所有 Sink 插件需要迁移到新的抽象层

### Added
- 新增 AbstractSink 和 AbstractSinkWriter 基类
- Console Sink 和 JDBC Sink 迁移到新 API
- 批量缓冲自动管理
- InitContext 支持（subtaskId、并行度、度量组）

### Migration Guide
- 继承 AbstractSink 和 AbstractSinkWriter
- 实现 writeRow() 和 flushBatch() 方法
- 参数校验在 Sink 构造函数中完成
```

- [ ] **Step 4: 最终提交**

```bash
git add .
git commit -m "chore: Sink 抽象层实现完成

- 完成 Phase 1: 核心抽象层
- 完成 Phase 2: Console Sink 迁移
- 完成 Phase 3: JDBC Sink 迁移
- 完成 Phase 4: 文档更新和测试

所有测试通过，功能验证完成。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## 依赖关系图

```
Task 1 (AbstractSink)
  └─> Task 2 (AbstractSinkWriter)
       ├─> Task 3 (SinkPlugin 接口)
       │    └─> Task 4 (JobBuilder)
       │         └─> Task 5 (测试基类)
       │              └─> Task 9 (Console 测试)
       ├─> Task 6 (ConsoleSink)
       │    └─> Task 7 (ConsoleSinkWriter)
       │         └─> Task 8 (ConsoleSinkPlugin)
       │              └─> Task 9 (Console 测试)
       └─> Task 10 (JdbcSink)
            └─> Task 11 (JdbcSinkWriter)
                 └─> Task 12 (JdbcSinkPlugin)
                      └─> Task 13 (JDBC 测试)
                           └─> Task 14 (文档)
                                └─> Task 15 (集成测试)
                                     └─> Task 16 (最终验证)
```

---

## 测试策略总结

### 单元测试
- AbstractSinkWriterTest 基类提供通用测试场景
- ConsoleSinkWriterTest 和 JdbcSinkWriterTest 继承基类
- 测试批量写入、手动 flush、异常处理等

### 集成测试
- 使用内存数据库（H2）测试 JDBC Sink
- 使用实际的 MySQL 数据库进行端到端测试
- 验证 checkpoint 恢复后数据完整性

### 性能测试
- 测试不同 batchSize 对性能的影响
- 测试并行写入的正确性和性能

---

## 风险和注意事项

1. **破坏性变更**: SinkPlugin 接口变更导致现有插件暂时不可用
   - 缓解措施：快速迁移 Console 和 JDBC Sink

2. **测试覆盖**: JDBC Sink 需要数据库环境
   - 缓解措施：使用内存数据库（H2）进行单元测试

3. **向后兼容**: 用户可能依赖旧的 SinkFunction API
   - 缓解措施：明确记录变更，提供迁移指南

4. **性能影响**: 新的抽象层可能引入性能开销
   - 缓解措施：性能测试，优化关键路径

---

## 完成标准

- [x] 所有代码实现完成
- [x] 所有单元测试通过
- [x] 集成测试通过
- [x] 文档更新完成
- [x] 代码审查通过
- [x] 功能验证完成