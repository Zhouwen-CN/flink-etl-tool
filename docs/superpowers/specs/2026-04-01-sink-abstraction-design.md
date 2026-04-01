# Sink 抽象层设计文档

**日期**: 2026-04-01
**作者**: Claude Code
**状态**: 设计审批

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
- 提供默认 batchSize

**设计要点**：
- 构造函数只接收 config，参数校验在具体实现类（如 JdbcSink）中进行
- 子类实现 `createWriter()` 方法返回具体的 Writer
- 参考 Source 的 AbstractSplitSource 设计

#### 2. AbstractSinkWriter

**职责**：
- 实现 `SinkWriter<Row>` 接口
- 内置批量缓冲管理（自动计数和 flush）
- 提供 InitContext 访问（subtaskId、并行度、度量组等）
- 懒初始化和异常处理

**自动处理的功能**：
- **批量计数**: `write()` 自动计数，达到 batchSize 时触发 flush
- **懒初始化**: 第一次 `write()` 时调用 `open()`
- **自动 flush**: checkpoint 或输入结束时强制 flush
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

    // 构造函数：接收 InitContext 和 batchSize
    public AbstractSinkWriter(Sink.InitContext context, int batchSize);

    // 自动批量管理
    public final void write(Row element, Context context) throws IOException;

    // 自动 flush 管理
    public final void flush(boolean endOfInput) throws IOException;

    // InitContext 工具方法
    protected int getSubtaskId();
    protected int getNumberOfParallelSubtasks();
    protected SinkWriterMetricGroup getMetricGroup();

    // 子类实现的抽象方法
    protected abstract void writeRow(Row row) throws IOException;
    protected abstract void flushBatch() throws IOException;
    protected abstract void cleanup() throws IOException;

    // 可选覆盖的方法
    protected void open() throws IOException;
    protected void handleFlushFailure(Exception e);
}
```

---

## 使用示例

### Console Sink 实现

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
        super(context, 1); // 每条立即输出
        this.showSubtask = showSubtask;
        this.subtaskId = getSubtaskId();
        this.totalSubtasks = getNumberOfParallelSubtasks();
    }

    @Override
    protected void writeRow(Row row) throws IOException {
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
        String url = config.getString("url");
        Preconditions.checkNotNull(url, "url is null");

        // 构建配置对象
        this.jdbcSinkConfig = JdbcSinkConfig.builder()
            .url(url)
            .username(config.getString("username"))
            .password(config.getString("password"))
            .table(config.getString("table"))
            .batchSize(config.getInteger("batchSize", getDefaultBatchSize()))
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
    private Counter writeCounter;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) {
        super(context, config.getBatchSize());
        this.config = config;
    }

    @Override
    protected void open() throws IOException {
        connection = DriverManager.getConnection(config.getUrl(), ...);
        connection.setAutoCommit(false);
        statement = connection.prepareStatement(generateSql());
        writeCounter = getMetricGroup().getNumRecordsOutCounter();
    }

    @Override
    protected void writeRow(Row row) throws IOException {
        fillStatement(row);
        statement.addBatch();
        writeCounter.inc();
    }

    @Override
    protected void flushBatch() throws IOException {
        statement.executeBatch();
        connection.commit();
    }

    @Override
    protected void cleanup() throws IOException {
        if (statement != null) statement.close();
        if (connection != null) connection.close();
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
    throw new IllegalArgumentException("Sink 插件未实现新 API");
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

## 批量管理与异常处理

### 批量缓冲策略

- **内嵌设计**: 批量计数直接内嵌在 AbstractSinkWriter，不独立成类
- **自动触发**: `write()` 达到 batchSize 时自动调用 `flush()`
- **checkpoint 触发**: checkpoint 或输入结束时强制 flush

### 异常处理策略

| 异常场景 | 处理方式 | Flink 行为 |
|---------|---------|-----------|
| `writeRow()` 失败 | 抛出 IOException | Flink 从 checkpoint 重试整个批次 |
| `flushBatch()` 失败 | 清理状态 + 抛出 IOException | Flink 从 checkpoint 重试 |
| `close()` 时 flush 失败 | 记录日志 + 继续清理 | 任务继续结束 |

### 幂等性要求

- flush 操作必须幂等（故障恢复时可能重复调用）
- JDBC Sink 需要正确处理事务回滚
- 外部系统应接受可能的重复写入（at-least-once）

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
        implements StatefulSink<Row, WriterStateT> {
    // 添加状态序列化和恢复逻辑
}
```

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

- [Flink Sink API 官方文档](https://nightlies.apache.org/flink/flink-docs-release-2.2/zh/docs/dev/datastream/sinks/)
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