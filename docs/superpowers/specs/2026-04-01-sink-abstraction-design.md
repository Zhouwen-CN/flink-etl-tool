# Sink 抽象层设计文档

**日期**: 2026-04-01
**作者**: Claude Code
**状态**: 设计审批（已修订）

---

## 概述

为 Flink ETL 工具新增 Sink 抽象层，简化 Sink 插件的开发难度，提升易用性。新的抽象层基于 Flink Sink API（FLIP-143），提供批量缓冲管理、参数校验、异常处理等通用功能，插件开发者只需关注具体的写入逻辑。

---

## 背景与动机

### 现有问题

1. **使用旧版 SinkFunction API**: 现有 Sink 插件（Console、JDBC）使用 `SinkFunction<Row>`，该 API 已被 Flink 标记为废弃
2. **缺少抽象层**: 与 Source 相比，Sink 缺少易用的抽象基类，开发者需要处理大量细节
3. **重复实现**: 每个 Sink 插件都需要实现批量缓冲、flush 管理、异常处理等通用逻辑
4. **参数校验分散**: 参数校验逻辑分散在各个插件中，没有统一规范

### 设计目标

参考 Source 抽象层的成功经验，为 Sink 插件提供：

1. **易用性**: 提供抽象基类，插件开发者只需实现核心写入逻辑
2. **一致性**: 统一的参数校验、批量管理、异常处理机制
3. **可扩展性**: 支持自定义指标、水印传播、异步写入等高级功能
4. **向前兼容**: 使用新版 Sink API，避免未来迁移成本

---

## 设计决策

通过用户交互，确定了以下关键设计决策：

| 决策项 | 选择 | 理由 |
|--------|------|------|
| 一致性语义级别 | 基础 Sink（at-least-once） | JDBC Sink 等场景通过 flush() 即可保证 at-least-once，无需状态恢复 |
| 抽象层深度 | 双层抽象（Sink + Writer） | 平衡易用性和灵活性，避免过度设计 |
| 参数校验位置 | 具体 Sink 构造函数 | 参考 Source 设计，校验逻辑集中在实现类 |
| 批量写入处理 | 抽象层管理批量逻辑 | 减少子类实现负担，统一 flush 时机 |
| API 迁移策略 | 直接迁移（破坏性变更） | 避免 API 兼容性负担，现有 Sink 后续迁移 |

---

## 架构设计

### 模块结构

新建 `flink-etl-core/src/main/java/com/etl/core/sink/` 目录：

```
flink-etl-core/src/main/java/com/etl/core/sink/
├── AbstractSink.java          # Sink 基类
├── AbstractSinkWriter.java    # Writer 基类（提供批量缓冲和 InitContext）
```

### 核心组件

#### 1. AbstractSink

**职责**：
- 实现 `Sink<Row>` 接口
- 接收 SinkConfig（不做校验，由子类负责）
- 创建 Writer 实例

**设计要点**：
- 构造函数只接收 config，参数校验在具体实现类（如 JdbcSink）中进行
- 子类实现 `createWriter()` 方法返回具体的 Writer
- 参考 Source 的 AbstractSplitSource 设计

**重要约定**：

AbstractSink 的构造函数**不进行任何参数校验或转换**。子类应定义自己的配置对象（如 `JdbcSinkConfig`），在构造函数中完成参数校验和转换。

```java
public abstract class AbstractSink implements Sink<Row> {
    protected final SinkConfig config;  // 原始配置

    public AbstractSink(SinkConfig config) {
        this.config = config;
    }

    // 子类实现此方法返回具体的 Writer
    public abstract SinkWriter<Row> createWriter(InitContext context) throws IOException;
}
```

**子类实现示例**：

```java
public class JdbcSink extends AbstractSink {
    private final JdbcSinkConfig jdbcConfig;  // 类型安全的配置对象

    public JdbcSink(SinkConfig config) {
        super(config);  // 父类不处理配置

        // 参数校验 + 转换
        String url = Preconditions.checkNotNull(config.getString("url"), "url is null");
        int batchSize = config.getInteger("batchSize", getDefaultBatchSize());

        this.jdbcConfig = JdbcSinkConfig.builder()
            .url(url)
            .batchSize(batchSize)
            .build();
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) {
        return new JdbcSinkWriter(context, jdbcConfig);  // 传递类型安全的配置
    }

    @Override
    public int getDefaultBatchSize() {
        return 100;  // JDBC 默认批量大小
    }
}
```

**参数传递模式**：

1. **Sink 类职责**：
   - 接收原始 `SinkConfig`
   - 在构造函数中进行参数校验和转换
   - 将原始配置转换为类型安全的配置对象（如 `JdbcSinkConfig`）
   - 在 `createWriter()` 中将配置对象传递给 Writer

2. **Writer 类职责**：
   - 接收类型安全的配置对象
   - 使用配置对象进行初始化和写入

#### 2. AbstractSinkWriter

**职责**：
- 实现 `SinkWriter<Row>` 接口
- 内置批量缓冲管理（自动计数和 flush）
- 提供 InitContext 访问（subtaskId、并行度、度量组等）
- 异常处理和资源清理

**自动处理的功能**：
- **批量计数**: `write()` 自动计数，达到 batchSize 时触发 flush
- **立即初始化**: 构造函数中立即调用 `open()`
- **Flink 触发 flush**: Flink 在 checkpoint 或输入结束时调用 `flush(true)` 强制提交
- **异常恢复**: flush 失败时清理状态，Flink 从 checkpoint 重试
- **资源清理**: `close()` 确保数据 flush + 资源清理

**子类需实现的抽象方法**：
- `writeRow(Row row)` - 具体写入逻辑
- `flushBatch()` - 批量提交逻辑
- `cleanup()` - 资源清理逻辑

**可选覆盖的方法**：
- `open()` - 初始化资源
- `handleFlushFailure(Exception e)` - 自定义失败处理

---

## 核心接口设计

### SinkPlugin 接口修改

```java
public interface SinkPlugin extends Plugin {
    /**
     * 创建 Sink 实例（新版 API）
     */
    Sink<Row> createSink(SinkConfig config);

    default int getDefaultBatchSize() {
        return 100;
    }
}
```

**变更说明**：
- 移除旧的 `createSink()` 返回 `SinkFunction` 的方法
- 直接返回 `Sink<Row>`
- 现有插件（Console、JDBC）暂时返回 null，后续迁移

---

### AbstractSinkWriter 核心方法

```java
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
     */
    protected int getSubtaskId() {
        return context.getSubtaskId();
    }

    /**
     * 获取总并行度
     */
    protected int getNumberOfParallelSubtasks() {
        return context.getNumberOfParallelSubtasks();
    }

    /**
     * 获取度量组
     */
    protected SinkWriterMetricGroup getMetricGroup() {
        return context.metricGroup();
    }

    /**
     * 写入数据（自动批量管理）
     */
    public final void write(Row element, Context context) throws IOException, InterruptedException {
        writeRow(element);
        pendingCount++;

        if (pendingCount >= batchSize) {
            flush(false);
        }
    }

    /**
     * Flush 所有待写入的数据
     */
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
     */
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

    // 子类实现的抽象方法
    protected abstract void writeRow(Row row) throws IOException, InterruptedException;
    protected abstract void flushBatch() throws IOException, InterruptedException;
    protected abstract void cleanup() throws IOException;

    // 可选覆盖的方法
    protected void open() throws IOException {}
    protected void handleFlushFailure(Exception e) {}
}
```

---

## 批量管理实现细节

### 抽象层职责

1. **维护计数器 `pendingCount`**，跟踪待 flush 的记录数
2. **在 `write()` 方法中**：
   - 调用子类的 `writeRow(row)` 处理单条记录
   - `pendingCount++`
   - 如果 `pendingCount >= batchSize`，自动调用 `flush()`
3. **在 `flush()` 方法中**：
   - 调用子类的 `flushBatch()` 执行实际提交
   - 成功后重置 `pendingCount = 0`
   - 失败时不重置计数器，让 Flink 从 checkpoint 重试

### 子类职责

1. **`writeRow(Row row)`**：
   - 处理单条记录
   - 可能调用外部系统的缓冲方法（如 JDBC 的 `addBatch()`）
   - 不需要判断 batchSize 或维护计数器

2. **`flushBatch()`**：
   - 执行实际的批量提交（如 JDBC 的 `executeBatch()` + `commit()`）
   - 不应重置 `pendingCount`（由抽象层管理）

### 关键约定

- 子类不需要判断 batchSize，只需处理单条记录和批量提交
- 子类不需要维护计数器，抽象层统一管理
- `flush()` 会自动重置 `pendingCount`，子类的 `flushBatch()` 不应重置计数器

---

## 异常处理策略

### 异常场景处理

| 异常场景 | 处理方式 | Flink 行为 |
|---------|---------|-----------|
| `writeRow()` 失败 | 抛出 IOException | Flink 从 checkpoint 重试整个批次 |
| `flushBatch()` 失败 | 调用 `handleFlushFailure()` 清理状态 + 抛出 IOException | Flink 从 checkpoint 重试 |
| `close()` 时 flush 失败 | 抛出异常，阻止任务正常结束 | Flink 标记任务失败，触发重试或告警 |

### flush 失败的详细处理流程

```java
public final void flush(boolean endOfInput) throws IOException, InterruptedException {
    try {
        if (pendingCount > 0) {
            flushBatch();  // 子类实现
            pendingCount = 0;  // 重置计数器
        }
    } catch (Exception e) {
        // 失败处理
        handleFlushFailure(e);  // 子类可选覆盖

        // 关键：不重置 pendingCount，让 Flink 从 checkpoint 重试整个批次
        throw new IOException("Flush failed", e);
    }
}
```

**关键约定**：
- flush 失败时，抽象层**不重置** `pendingCount`
- Flink 会从上一个 checkpoint 重新执行，重放所有记录
- 子类需要在 `handleFlushFailure()` 中清理内部状态（如 JDBC 回滚事务）
- 子类的 `cleanup()` 只在 `close()` 时调用，不参与 flush 失败处理

**JDBC 示例**：

```java
@Override
protected void handleFlushFailure(Exception e) {
    try {
        if (connection != null) {
            connection.rollback();  // 回滚事务
        }
    } catch (SQLException rollbackEx) {
        log.error("回滚失败", rollbackEx);
    }
}
```

### 幂等性要求

- flush 操作必须幂等（故障恢复时可能重复调用）
- JDBC Sink 需要正确处理事务回滚
- 外部系统应接受可能的重复写入（at-least-once）

### 边界情况处理

**1. batchSize 参数校验**：

```java
public AbstractSinkWriter(InitContext context, int batchSize) {
    Preconditions.checkArgument(batchSize > 0, "batchSize must be positive");
    this.batchSize = batchSize;
}
```

**2. 空输入处理**：
- 即使没有数据输入，`close()` 仍会调用 `flush(true)` 和 `cleanup()`
- `flush()` 内部检查 `pendingCount > 0`，无数据时不调用 `flushBatch()`

**3. 并行写入**：
- 每个 Writer 实例（对应一个 subtask）独立管理批量缓冲
- 并行度不影响 batchSize 的含义

**4. checkpoint 与 batchSize**：
- Flink 在 checkpoint 时调用 `flush()`，无论 `pendingCount` 是否达到 batchSize
- 建议用户根据 checkpoint 间隔和吞吐量配置合理的 batchSize

---

## InitContext 使用约定

### 存储方式

AbstractSinkWriter 内部存储 `InitContext` 为 `final` 字段：

```java
protected final Sink.InitContext context;
```

### 可用时机

1. **构造函数中可调用**：
   - `getSubtaskId()`
   - `getNumberOfParallelSubtasks()`

2. **建议在 `open()` 方法中调用**：
   - `getMetricGroup()`（指标注册通常需要初始化）

### 子类使用示例

```java
public class MySinkWriter extends AbstractSinkWriter {
    private final int subtaskId;  // 构造函数中获取
    private Counter writeCounter;  // open() 中获取

    public MySinkWriter(InitContext context, int batchSize) {
        super(context, batchSize);
        this.subtaskId = getSubtaskId();  // 构造函数中调用
    }

    @Override
    protected void open() {
        writeCounter = getMetricGroup().getNumRecordsOutCounter();  // open() 中调用
    }
}
```

---

## 使用示例

### Console Sink 实现

**特殊说明**：Console Sink 不需要批量缓冲，应设置 `batchSize=Integer.MAX_VALUE`，让数据只在 checkpoint 或 close 时 flush。

```java
// 1. Sink 类
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

// 2. Writer 类
public class ConsoleSinkWriter extends AbstractSinkWriter {
    private final boolean showSubtask;
    private final int subtaskId;
    private final int totalSubtasks;

    public ConsoleSinkWriter(Sink.InitContext context, boolean showSubtask) {
        super(context, Integer.MAX_VALUE);  // 不触发批量 flush
        this.showSubtask = showSubtask;
        this.subtaskId = getSubtaskId();
        this.totalSubtasks = getNumberOfParallelSubtasks();
    }

    @Override
    protected void writeRow(Row row) throws IOException {
        // 直接输出，不缓冲
        if (showSubtask) {
            System.out.printf("[subtask-%d/%d] %s%n", subtaskId, totalSubtasks, row);
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
    }
}
```

### JDBC Sink 实现（未来迁移）

```java
// 1. Sink 类（参数校验在构造函数）
public class JdbcSink extends AbstractSink {
    private final JdbcSinkConfig jdbcSinkConfig;

    public JdbcSink(SinkConfig config) {
        super(config);

        // 参数校验
        String url = Preconditions.checkNotNull(config.getString("url"), "url is null");

        JdbcDialect dialect = JdbcDialects.get(url);

        int batchSize = config.getInteger("batchSize", getDefaultBatchSize());
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        // 构建配置对象
        this.jdbcSinkConfig = JdbcSinkConfig.builder()
            .url(dialect.wrapUrl(url))
            .username(config.getString("username"))
            .password(config.getString("password"))
            .table(config.getString("table"))
            .sql(config.getString("sql"))
            .dialect(dialect)
            .batchSize(batchSize)
            .build();
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) throws IOException {
        return new JdbcSinkWriter(context, jdbcSinkConfig);
    }
}

// 2. Writer 类
public class JdbcSinkWriter extends AbstractSinkWriter {
    private final JdbcSinkConfig config;
    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient String[] columns;
    private Counter writeCounter;
    private Counter flushCounter;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) {
        super(context, config.getBatchSize());
        this.config = config;
    }

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
            SinkWriterMetricGroup metricGroup = getMetricGroup();
            writeCounter = metricGroup.getNumRecordsOutCounter();

            log.info("JDBC Writer 初始化完成, subtaskId={}", getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to initialize JDBC connection", e);
        }
    }

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
            writeCounter.inc();
        } catch (SQLException e) {
            throw new IOException("Failed to write row", e);
        }
    }

    private void initStatement(Row row) throws SQLException {
        Set<String> fieldNames = row.getFieldNames(true);
        this.columns = fieldNames.toArray(new String[0]);

        String sql = generateInsertSql(config.getTable(), columns);
        this.statement = connection.prepareStatement(sql);
        log.info("PreparedStatement 初始化: {}", sql);
    }

    private String generateInsertSql(String table, String[] columns) {
        String columnList = Arrays.stream(columns).collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
            .map(c -> "?")
            .collect(Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", table, columnList, placeholders);
    }

    @Override
    protected void flushBatch() throws IOException {
        try {
            int[] results = statement.executeBatch();
            connection.commit();

            log.debug("Flush 完成, 提交 {} 条记录", results.length);
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

    @Override
    protected void cleanup() throws IOException {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
            log.info("JDBC Writer 资源清理完成");
        } catch (SQLException e) {
            throw new IOException("Failed to cleanup JDBC resources", e);
        }
    }
}
```

---

## JobBuilder 适配

### 修改点

修改 `JobBuilder.java` 使用新版 Sink API：

```java
// 旧版 API（已移除）
SinkFunction<Row> sink = sinkPlugin.createSink(sinkConfig);
resultStream.addSink(sink);

// 新版 API
Sink<Row> sink = sinkPlugin.createSink(sinkConfig);
if (sink == null) {
    throw new IllegalArgumentException("Sink 插件 '" + sinkType + "' 未实现新 API，请检查插件是否已迁移");
}
resultStream.sinkTo(sink).name(sinkType + " sink");
```

### 影响范围

**修改文件**：
1. `SinkPlugin.java` - 接口签名变更
2. `JobBuilder.java` - 使用新 API
3. `ConsoleSinkPlugin.java` - 暂时返回 null
4. `JdbcSinkPlugin.java` - 暂时返回 null

**暂时不工作**：
- Console Sink 和 JDBC Sink 在迁移前无法使用
- 用户会得到明确的错误提示

---

## checkpoint 要求

### 重要提示

新版 Sink API 的异常恢复依赖 Flink checkpoint。如果未启用 checkpoint，flush 失败会导致任务失败并退出，无法自动重试。

### JobExecutor 修改建议

建议在 JobConfig 中强制启用 checkpoint（修改 `JobExecutor` 或 `EtlClient`）：

```java
public class JobExecutor {
    public static void execute(JobConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 强制启用 checkpoint（间隔 1 分钟）
        env.enableCheckpointing(60000);

        JobBuilder.build(env, config);
        env.execute(config.getJob().getName());
    }
}
```

---

## 测试策略

### 单元测试基类

提供 `AbstractSinkWriterTest` 基类：

```java
public abstract class AbstractSinkWriterTest {
    protected Row createTestRow(String... values);
    protected abstract AbstractSinkWriter createWriter(int batchSize);

    @Test
    public void testBatchWriteAndFlush();

    @Test
    public void testFlushFailure();

    @Test
    public void testCloseWithPendingData();

    @Test
    public void testCheckpointRecovery();

    @Test
    public void testParallelWriters();
}
```

### 测试场景补充

**1. checkpoint 恢复测试**：

```java
@Test
public void testCheckpointRecovery() throws Exception {
    // 写入部分数据
    writer.writeRow(row1);
    writer.writeRow(row2);

    // 模拟 checkpoint
    writer.flush(false);

    // 写入更多数据
    writer.writeRow(row3);

    // 模拟失败和恢复
    writer.handleFlushFailure(new IOException("Mock failure"));
    writer.flush(false);  // 应该重放 row1, row2, row3
}
```

**2. 并发写入测试**：

```java
@Test
public void testParallelWriters() throws Exception {
    int parallelism = 4;
    List<AbstractSinkWriter> writers = new ArrayList<>();

    for (int i = 0; i < parallelism; i++) {
        InitContext context = new MockInitContext(i, parallelism);
        writers.add(createWriter(context, 10));
    }

    // 并发写入
    writers.parallelStream().forEach(w -> {
        for (int j = 0; j < 20; j++) {
            w.writeRow(createTestRow("data-" + j));
        }
        w.flush(true);
    });

    // 验证数据完整性
}
```

**3. 批量大小性能测试**：

```java
@Test
public void testBatchSizePerformance() throws Exception {
    int[] batchSizes = {1, 10, 100, 1000};

    for (int batchSize : batchSizes) {
        long startTime = System.currentTimeMillis();

        AbstractSinkWriter writer = createWriter(batchSize);
        for (int i = 0; i < 10000; i++) {
            writer.writeRow(createTestRow("data-" + i));
        }
        writer.flush(true);

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("batchSize=" + batchSize + ", duration=" + duration + "ms");
    }
}
```

### 集成测试

- 使用实际数据库或文件系统测试
- 验证 checkpoint 恢复后数据完整性
- 测试并行写入的正确性

---

## 未来扩展

### 1. Watermark 传播

支持 `writeWatermark()` 方法，用于级联流处理场景。

### 2. 异步写入

提供 `AsyncSinkWriter` 接口，支持高吞吐量异步写入（如 Kafka）。

### 3. 状态恢复（StatefulSink）

如果未来需要 exactly-once 或状态恢复，可平滑迁移：

```java
public abstract class AbstractStatefulSink<WriterStateT>
        extends AbstractSink
        implements StatefulSink<Row, WriterStateT> {

    public AbstractStatefulSink(SinkConfig config) {
        super(config);
    }

    // 新增状态序列化方法
    public abstract SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer();
}

public abstract class AbstractStatefulSinkWriter<WriterStateT>
        extends AbstractSinkWriter
        implements StatefulSinkWriter<Row, WriterStateT> {

    public AbstractStatefulSinkWriter(InitContext context, int batchSize) {
        super(context, batchSize);
    }

    // 新增状态快照和恢复方法
    @Override
    public List<WriterStateT> snapshotState(long checkpointId) throws IOException {
        // 子类实现状态快照逻辑
        return Collections.emptyList();
    }
}
```

**迁移优势**：
- 批量管理、InitContext、异常处理逻辑复用
- 子类只需添加状态管理逻辑
- 向前兼容（基础 Sink 可以直接升级）

### 4. 文件合并（小文件优化）

实现 `SupportsPostCommitTopology`，在 Committer 后合并小文件。

---

## 实现计划

### Phase 1: 核心抽象层

1. 创建 `AbstractSink` 和 `AbstractSinkWriter`
2. 修改 `SinkPlugin` 接口
3. 修改 `JobBuilder` 使用新 API

### Phase 2: Console Sink 迁移

1. 实现 ConsoleSink 和 ConsoleSinkWriter
2. 更新 ConsoleSinkPlugin

### Phase 3: JDBC Sink 迁移

1. 实现 JdbcSink 和 JdbcSinkWriter
2. 迁移现有的 JdbcSinkFunction 逻辑
3. 更新 JdbcSinkPlugin

### Phase 4: 文档更新

1. 更新 PLUGINS.md 文档
2. 添加 Sink 插件开发指南
3. 提供完整的示例代码

---

## 参考文档

- [Flink Sink API 官方文档（1.15）](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/connectors/datastream/)
- [FLIP-143: New Sink API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143)
- [Source 抽象层设计](../source/)（参考 AbstractSplitSource）
- [JdbcSource 实现](../../flink-etl-source/flink-etl-source-jdbc/)（参考参数校验模式）

---

## 附录

### Flink Sink API 接口关系

```
Sink<InputT>
  └─ StatefulSink<InputT, WriterStateT>
      └─ TwoPhaseCommittingSink<InputT, CommT>

SinkWriter<InputT>
  └─ StatefulSinkWriter<InputT, WriterStateT>
      └─ PrecommittingSinkWriter<InputT, CommT>

Committer<CommT>
```

### 设计对比：Source vs Sink

| 对比项 | Source 抽象层 | Sink 抽象层 |
|--------|--------------|------------|
| 基础接口 | `Source<Row, SplitT, CheckpointT>` | `Sink<Row>` |
| 抽象类数量 | 5个（AbstractSplitSource, BaseSplitEnumerator, BaseSourceReader, BaseSplitReader, BaseSplitState） | 2个（AbstractSink, AbstractSinkWriter） |
| 参数校验位置 | 具体 Source 构造函数 | 具体 Sink 构造函数 |
| 批量管理 | Reader 内部处理 | Writer 自动管理 |
| 状态恢复 | Split 和 Checkpoint 序列化 | 无（基础 Sink 不保存状态） |
| 子类职责 | 实现 SplitEnumerator 和 SplitReader | 实现 writeRow 和 flushBatch |

---

## 修订记录

**v1.1 (2026-04-01)**:
- 明确批量管理实现细节和职责分工
- 明确参数传递机制（Sink -> Writer）
- 修正 flush 失败后的状态清理流程
- 修正 close() 时的 flush 失败处理（改为抛出异常）
- 修正 open() 初始化时机（从懒初始化改为立即初始化）
- 补充 checkpoint 要求和 JobExecutor 修改建议
- 补充 InitContext 使用约定
- 补充边界情况处理说明
- 修正 JDBC Sink 和 Console Sink 示例代码
- 补充测试场景（checkpoint 恢复、并发写入）
- 补充 StatefulSink 迁移路径
- 修正 Flink 文档引用（从 2.2 改为 1.15）