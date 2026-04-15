# Mock Source 设计文档

**日期：** 2026-04-13
**作者：** Zhouwen-CN
**状态：** Draft

---

## 目录

1. [概述](#概述)
2. [需求分析](#需求分析)
3. [设计方案](#设计方案)
4. [配置参数设计](#配置参数设计)
5. [架构设计](#架构设计)
6. [核心类实现逻辑](#核心类实现逻辑)
7. [数据流转机制](#数据流转机制)
8. [测试策略](#测试策略)
9. [扩展性考虑](#扩展性考虑)

---

## 概述

### 背景

Mock Source 是一个用于测试和演示的数据源插件，能够生成模拟数据用于 Flink ETL 任务的开发和验证。通过配置驱动的数据生成，无需依赖外部数据源即可快速验证整个数据流转链路。

### 目标

1. **简化测试流程**：无需搭建 MySQL、Kafka 等外部数据源即可测试 Transform 和 Sink 逻辑
2. **CDC 场景支持**：支持生成带有 RowKind 标记的数据，验证 CDC Sink 的写入逻辑
3. **灵活配置**：支持固定数据配置和随机数据生成两种模式
4. **架构一致性**：遵循项目现有的 Source 抽象层设计，与 JDBC Source、LocalFile Source 保持一致

### 使用场景

- **快速原型验证**：无需外部依赖即可测试完整的 ETL 任务配置
- **Transform 逻辑测试**：提供稳定的测试数据验证 SQL Transform 的正确性
- **Sink 功能测试**：验证 Console Sink、JDBC Sink（包括 CDC 模式）的写入逻辑
- **流式任务演示**：演示 streaming 模式下的持续数据处理流程

---

## 需求分析

### 功能需求

根据用户需求，Mock Source 需要支持以下功能：

1. **Schema 配置**：
   - 必须配置 schema，定义输出数据的字段结构
   - 只支持简单类型：STRING、BOOLEAN、INT、LONG、DOUBLE、DECIMAL、TIMESTAMP
   - 不支持复杂类型：ARRAY、OBJECT 等

2. **固定数据配置（rows）**：
   - 支持配置 rows 参数，提供固定的测试数据
   - rows 格式为数组，每项包含 `kind`（RowKind）和 `data`（字段值）
   - 支持 Flink 的四种 RowKind：INSERT、UPDATE_BEFORE、UPDATE_AFTER、DELETE

3. **随机数据生成**：
   - 未配置 rows 时，根据 schema 随机生成数据
   - batch 模式：生成固定数量的数据（numRows 参数，默认 10）
   - streaming 模式：定时生成数据（intervalMs 参数，默认 1000ms）
   - 随机生成的数据全部为 INSERT 类型

4. **运行模式适配**：
   - batch 模式：数据有限，读完即结束（Boundedness.BOUNDED）
   - streaming 模式：数据无限，持续生成（Boundedness.UNBOUNDED）

5. **分片策略**：
   - 不进行分片，直接使用单分片
   - 固定分片 ID："mock-split-0"

### 配置验证规则

| 运行模式 | 有效配置 | 无效配置（忽略并警告） | 默认值 |
|---------|---------|---------------------|--------|
| batch | schema、rows 或 numRows | intervalMs | numRows: 10 |
| streaming | schema、intervalMs | rows、numRows | intervalMs: 1000 |

**配置优先级：**
- batch 模式：优先使用 rows，未配置时使用 numRows 随机生成
- streaming 模式：忽略 rows 和 numRows，使用 intervalMs 定时生成

### 随机数据生成规则

采用固定规则生成，简化实现：

| 字段类型 | 生成规则 | 示例值 |
|---------|---------|--------|
| STRING | `"field_<随机UUID前8位>"` | `"field_a1b2c3d4"` |
| BOOLEAN | 随机 true/false | `true` |
| INT | 随机整数（0-10000） | `42` |
| LONG | 随机长整数（0-10000） | `1234L` |
| DOUBLE | 随机浮点数（0.0-10000.0） | `56.78` |
| DECIMAL | 随机 Decimal（2位小数，范围 0-10000） | `Decimal(123.45)` |
| TIMESTAMP | 当前时间戳 | `2026-04-13 10:30:00` |

---

## 设计方案

### 方案选择：简化单分片实现

基于需求分析，选择**方案 B：简化单分片实现**，理由如下：

1. **架构一致性**：继承 `AbstractSplitSource`，遵循项目所有 Source 插件的统一设计
2. **需求匹配**：明确单分片设计，符合"不进行分片"需求，避免过度设计
3. **复用基础设施**：使用 `BaseSplitEnumerator`（自动处理分片分配）、`BaseSourceReader`（自动处理线程模型）
4. **实现简洁**：核心逻辑集中在 `MockSplitReader`，batch/streaming 模式逻辑清晰
5. **可扩展性**：保持架构开放，未来可扩展支持多分片（如多个 Mock 实例并行）

### 与其他方案的对比

| 方案 | 优点 | 缺点 | 适用场景 |
|------|------|------|---------|
| A. 完整 FLIP-27 实现 | 完全符合规范，支持多分片 | 实现冗余，过度设计 | 需要多分片扩展 |
| **B. 简化单分片**（选中） | 架构一致，实现简洁，符合需求 | 不支持多分片（需求明确不需要） | 当前需求 |
| C. 内置 Source | 快速实现 | 不符合项目架构，难以复用基础设施 | 快速原型（不推荐） |

---

## 配置参数设计

### 配置参数表

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `schema` | 是 | - | Schema 定义，只支持简单类型（STRING、BOOLEAN、INT、LONG、DOUBLE、DECIMAL、TIMESTAMP） |
| `rows` | 否 | - | 固定数据配置，数组格式，每项包含 `kind`（RowKind）和 `data`（字段值）。batch 模式时使用，streaming 模式时忽略 |
| `numRows` | 否 | 10 | batch 模式随机生成的行数。仅在未配置 `rows` 时生效，streaming 模式时忽略 |
| `intervalMs` | 否 | 1000 | streaming 模式生成数据的间隔时间（毫秒）。batch 模式时忽略 |

### rows 配置格式

```json
{
  "rows": [
    {
      "kind": "INSERT",
      "data": {
        "id": 1,
        "name": "张三",
        "age": 18,
        "active": true,
        "email": "zhangsan@example.com"
      }
    },
    {
      "kind": "UPDATE_AFTER",
      "data": {
        "id": 2,
        "name": "李四",
        "age": 25
      }
    },
    {
      "kind": "DELETE",
      "data": {
        "id": 3
      }
    }
  ]
}
```

**RowKind 支持范围：**
- `INSERT` - 插入数据
- `UPDATE_BEFORE` - 更新前的数据
- `UPDATE_AFTER` - 更新后的数据
- `DELETE` - 删除数据

**校验规则：**
- rows 配置中的 data 字段必须严格匹配 schema 定义的字段名和类型
- 字段类型不匹配时抛异常：`SchemaConfigException`
- 缺失字段时抛异常（不允许部分字段）

### 配置示例

#### Batch 模式 - 固定数据

```json
{
  "job": {
    "name": "mock-batch-fixed",
    "mode": "batch"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "users",
    "config": {
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "age": "INT",
        "active": "BOOLEAN",
        "email": "STRING"
      },
      "rows": [
        {
          "kind": "INSERT",
          "data": {
            "id": 1,
            "name": "张三",
            "age": 25,
            "active": true,
            "email": "zhangsan@example.com"
          }
        },
        {
          "kind": "UPDATE_AFTER",
          "data": {
            "id": 2,
            "name": "李四",
            "age": 30,
            "active": false,
            "email": "lisi@example.com"
          }
        },
        {
          "kind": "DELETE",
          "data": {
            "id": 3,
            "name": "王五",
            "age": 28,
            "active": true,
            "email": "wangwu@example.com"
          }
        }
      ]
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "users",
    "config": {}
  }]
}
```

#### Batch 模式 - 随机生成

```json
{
  "job": {
    "name": "mock-batch-random",
    "mode": "batch"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "orders",
    "config": {
      "schema": {
        "orderId": "LONG",
        "amount": "DOUBLE",
        "status": "STRING",
        "created_at": "TIMESTAMP"
      },
      "numRows": 50
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "orders",
    "config": {}
  }]
}
```

#### Streaming 模式 - 定时生成

```json
{
  "job": {
    "name": "mock-streaming",
    "mode": "streaming"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "events",
    "config": {
      "schema": {
        "eventId": "LONG",
        "eventType": "STRING",
        "timestamp": "TIMESTAMP"
      },
      "intervalMs": 500
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "events",
    "config": {}
  }]
}
```

#### CDC 测试场景

配合 JDBC Sink 的 CDC 模式测试：

```json
{
  "job": {
    "name": "mock-cdc-test",
    "mode": "batch"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "users_cdc",
    "config": {
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "email": "STRING"
      },
      "rows": [
        {
          "kind": "INSERT",
          "data": { "id": 1, "name": "Alice", "email": "alice@example.com" }
        },
        {
          "kind": "INSERT",
          "data": { "id": 2, "name": "Bob", "email": "bob@example.com" }
        },
        {
          "kind": "UPDATE_AFTER",
          "data": { "id": 1, "name": "Alice Updated", "email": "alice_new@example.com" }
        },
        {
          "kind": "DELETE",
          "data": { "id": 2, "name": "Bob", "email": "bob@example.com" }
        }
      ]
    }
  }],
  "sinks": [{
    "type": "jdbc",
    "inputTable": "users_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/test_db",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "cdc",
      "keyFields": ["id"],
      "batchSize": 100
    }
  }]
}
```

---

## 架构设计

### 模块结构

```
flink-etl-source/
└── flink-etl-source-mock/
    ├── pom.xml
    └── src/
        ├── main/
        │   └── java/
        │       └── com/etl/source/mock/
        │           ├── MockSourcePlugin.java          # SPI 插件入口（@AutoService）
        │           ├── MockSource.java                # 主类（继承 AbstractSplitSource）
        │           ├── MockSplit.java                 # 单分片（固定 ID）
        │           ├── MockSplitEnumerator.java       # 分片枚举器（继承 BaseSplitEnumerator）
        │           ├── MockSplitReader.java           # 分片读取器（继承 BaseSplitReader）
        │           ├── MockSourceReader.java          # 源阅读器（继承 BaseSourceReader）
        │           ├── MockRecordEmitter.java         # 记录发射器
        │           ├── MockEnumCheckpoint.java        # 枚举器检查点
        │           ├── MockSplitState.java            # 分片状态
        │           ├── config/
        │           │   └── MockSourceConfig.java      # 配置封装类（实现 Serializable）
        │           └── generator/
        │               ├── DataRowGenerator.java      # 从 rows 配置生成数据
        │               └── RandomRowGenerator.java    # 随机生成 Row 数据
        └── test/
            └── java/
                └── com/etl/source/mock/
                    ├── MockSourceTest.java            # 配置解析和校验测试
                    ├── MockSplitReaderTest.java       # 分片读取器测试
                    └── generator/
                        ├── DataRowGeneratorTest.java
                        └── RandomRowGeneratorTest.java
```

### 类职责划分

| 类名 | 职责 | 继承/实现 | 关键方法 |
|------|------|----------|---------|
| `MockSourcePlugin` | SPI 入口，创建 MockSource 实例 | `SourcePlugin` + `@AutoService` | `identifier()`、`createSource()` |
| `MockSource` | 参数校验、schema 校验、创建 Enumerator/Reader、定义有界性 | `AbstractSplitSource<MockSplit, MockEnumCheckpoint>` | `getBoundedness()`、`createEnumerator()`、`createReader()`、`validateSimpleTypesOnly()` |
| `MockSplit` | 单分片，固定 ID="mock-split-0" | `BaseSourceSplit` | `splitId()`、`getMockConfig()` |
| `MockSplitEnumerator` | 在 `start()` 时创建并分配单分片 | `BaseSplitEnumerator<MockSplit, MockEnumCheckpoint>` | `start()`、`snapshotState()` |
| `MockSplitReader` | **核心逻辑**：batch 模式读取 rows/numRows 数据；streaming 模式定时生成数据 | `BaseSplitReader<Row, MockSplit>` | `fetchNextBatch()`（batch）、`scheduleNextGeneration()`（streaming） |
| `MockSourceReader` | 包装 MockSplitReader，处理分片状态 | `BaseSourceReader<Row, Row, MockSplit, MockSplitState>` | `initializedState()`、`toSplitType()` |
| `MockRecordEmitter` | 发射 Row 数据到下游 | `RecordEmitter<Row, Row, MockSplitState>` | `emitRecord()` |
| `MockSourceConfig` | 配置封装：rows、numRows、intervalMs、schema、运行模式 | POJO（实现 Serializable） | Builder 模式构建 |
| `DataRowGenerator` | 从 rows 配置解析并生成 Row 数据（包含 RowKind） | 工具类 | `generateRows()`、`parseRowKind()` |
| `RandomRowGenerator` | 根据 schema 随机生成 Row 数据（固定规则） | 工具类 | `generateRow()`、`generateValue()` |

### 类依赖关系图

```
MockSourcePlugin
  └── creates MockSource
        ├── validates config → MockSourceConfig
        ├── validates schema → EtlSchema
        ├── creates MockSplitEnumerator
        │     └── extends BaseSplitEnumerator
        │     └── creates MockSplit (single split)
        ├── creates MockSourceReader
        │     └── extends BaseSourceReader
        │     └── uses MockRecordEmitter
        │     └── creates MockSplitReader
        │           ├── extends BaseSplitReader
        │           ├── uses DataRowGenerator (batch rows mode)
        │           └── uses RandomRowGenerator (batch/streaming random mode)
        ├── creates serializers
        │     ├── DefaultSplitSerializer<MockSplit>
        │     └── DefaultCheckpointSerializer<MockEnumCheckpoint>
        └── defines Boundedness
              └── batch → BOUNDED
              └── streaming → UNBOUNDED
```

---

## 核心类实现逻辑

### MockSource 主类

**构造函数逻辑：**

```java
public MockSource(SourceConfig config) {
    super(config);

    // 1. 获取运行模式（从 JobConfig 传入）
    RunMode runMode = getRunModeFromJobConfig();

    // 2. Schema 校验
    EtlSchema schema = config.getSchema();
    Preconditions.checkNotNull(schema, "schema is null");
    validateSimpleTypesOnly(schema);

    // 3. 配置参数获取和模式适配
    List<RowData> rows = parseRowsConfig(config);
    Integer numRows = config.getInteger("numRows", 10);
    Long intervalMs = config.getLong("intervalMs", 1000L);

    // 4. 配置冲突警告
    if (runMode == RunMode.BATCH && config.contains("intervalMs")) {
        log.warn("batch 模式下 intervalMs 参数被忽略");
    }
    if (runMode == RunMode.STREAMING && (config.contains("rows") || config.contains("numRows"))) {
        log.warn("streaming 模式下 rows/numRows 参数被忽略");
    }

    // 5. 封装配置对象
    this.mockConfig = MockSourceConfig.builder()
        .runMode(runMode)
        .schema(schema)
        .rows(rows)
        .numRows(runMode == BATCH ? (rows != null ? rows.size() : numRows) : null)
        .intervalMs(runMode == STREAMING ? intervalMs : null)
        .build();

    log.info("创建 MockSource: {}", this.mockConfig);
}
```

**Boundedness 判断：**

```java
@Override
public Boundedness getBoundedness() {
    return mockConfig.getRunMode() == RunMode.BATCH
        ? Boundedness.BOUNDED
        : Boundedness.UNBOUNDED;
}
```

**Schema 简单类型校验：**

```java
private void validateSimpleTypesOnly(EtlSchema schema) {
    for (int i = 0; i < schema.getFieldCount(); i++) {
        TypeInformation<?> type = schema.getFieldType(i);
        if (isComplexType(type)) {
            throw new SchemaConfigException(
                "Mock Source 不支持复杂类型字段 '" + schema.getFieldName(i) + "'。" +
                "只支持简单类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
        }
    }
}

private boolean isComplexType(TypeInformation<?> type) {
    return type instanceof RowTypeInfo
        || type instanceof BasicArrayTypeInfo
        || type instanceof ObjectArrayTypeInfo;
}
```

### MockSplitEnumerator 分片枚举器

```java
public class MockSplitEnumerator
    extends BaseSplitEnumerator<MockSplit, MockEnumCheckpoint> {

    private final MockSourceConfig mockConfig;

    public MockSplitEnumerator(
        SplitEnumeratorContext<MockSplit> context,
        MockSourceConfig mockConfig) {
        super(context);
        this.mockConfig = mockConfig;
    }

    public MockSplitEnumerator(
        SplitEnumeratorContext<MockSplit> context,
        MockEnumCheckpoint checkpoint,
        MockSourceConfig mockConfig) {
        super(context, checkpoint);
        this.mockConfig = mockConfig;
    }

    @Override
    public void start() {
        // 创建固定的单分片
        MockSplit split = new MockSplit("mock-split-0", mockConfig);

        // 添加到待分配队列
        pendingSplits.add(split);

        log.info("Mock Source 创建单分片: {}", split.splitId());

        // 立即通知所有已注册的 Reader 分片已就绪
        context.callAllReadersToRequestSplits();
    }

    @Override
    public MockEnumCheckpoint snapshotState(long checkpointId) {
        return new MockEnumCheckpoint(pendingSplits);
    }
}
```

**关键点：**
- 继承 `BaseSplitEnumerator`，自动处理 `handleSplitRequest()` 和 `addSplitsBack()`
- `start()` 创建单分片后立即调用 `callAllReadersToRequestSplits()`
- 检查点保存 pendingSplits 状态，支持故障恢复

### MockSplitReader 分片读取器（核心逻辑）

```java
public class MockSplitReader extends BaseSplitReader<Row, MockSplit> {

    private final MockSourceConfig mockConfig;
    private final EtlSchema schema;

    // batch 模式状态
    private Iterator<Row> batchDataIterator;
    private int currentRowIndex = 0;

    // streaming 模式状态
    private ScheduledExecutorService scheduler;
    private volatile boolean running = true;
    private AtomicLong rowCounter = new AtomicLong(0);

    public MockSplitReader(MockSourceConfig mockConfig) {
        this.mockConfig = mockConfig;
        this.schema = mockConfig.getSchema();

        // 初始化数据生成器
        if (mockConfig.getRows() != null) {
            // batch 模式 - 固定数据
            batchDataIterator = DataRowGenerator.generateRows(mockConfig.getRows(), schema).iterator();
        } else if (mockConfig.getRunMode() == RunMode.BATCH) {
            // batch 模式 - 随机生成
            batchDataIterator = RandomRowGenerator.generateRows(schema, mockConfig.getNumRows()).iterator();
        } else {
            // streaming 模式 - 定时生成
            scheduler = Executors.newSingleThreadScheduledExecutor();
        }
    }

    @Override
    protected void fetchNextBatch() throws IOException {
        if (mockConfig.getRunMode() == RunMode.BATCH) {
            fetchBatchData();
        } else {
            fetchStreamingData();
        }
    }

    // batch 模式：从 iterator 读取数据
    private void fetchBatchData() throws IOException {
        if (batchDataIterator.hasNext()) {
            Row row = batchDataIterator.next();
            currentRowIndex++;

            // 添加到输出队列
            outputQueue.add(row);

            log.debug("读取第 {} 行数据: {}", currentRowIndex, row);
        } else {
            // 数据读取完毕，标记分片结束
            log.info("Batch 模式数据读取完毕，共 {} 行", currentRowIndex);
            finished = true;
        }
    }

    // streaming 模式：定时生成数据
    private void fetchStreamingData() throws IOException {
        // 启动定时任务
        scheduler.scheduleAtFixedRate(() -> {
            if (!running) {
                return;
            }

            try {
                // 生成随机数据
                Row row = RandomRowGenerator.generateRow(schema);
                rowCounter.incrementAndGet();

                // 添加到输出队列
                outputQueue.add(row);

                log.debug("生成第 {} 行数据: {}", rowCounter.get(), row);
            } catch (Exception e) {
                log.error("生成数据失败", e);
            }
        }, 0, mockConfig.getIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        if (scheduler != null) {
            running = false;
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("MockSplitReader 关闭，streaming 模式共生成 {} 行数据", rowCounter.get());
    }
}
```

**关键点：**
- batch 模式：初始化时生成全部数据，通过 iterator 逐行读取，读完标记 `finished = true`
- streaming 模式：使用 `ScheduledExecutorService` 定时生成数据，`close()` 时停止调度器
- 继承 `BaseSplitReader`，自动处理线程模型（fetchNextBatch 在单独线程中执行）

**Streaming 模式线程模型说明：**

Streaming 模式下，`fetchNextBatch()` 的行为与 batch 模式不同：

1. **单次启动机制**：`fetchNextBatch()` 在第一次被调用时启动 `ScheduledExecutorService`，然后立即返回（不阻塞）
2. **调度器独立运行**：启动后，scheduler 在独立的线程中按 `intervalMs` 定时向 `outputQueue` 添加数据
3. **不设置 finished 标志**：streaming 模式下 `finished` 永不为 `true`，因为数据无限生成
4. **BaseSplitReader 线程模型**：
   - `BaseSplitReader` 的工作线程会周期调用 `fetchNextBatch()`
   - 第一次调用启动 scheduler
   - 后续调用时 scheduler 已启动，`fetchNextBatch()` 直接返回（无操作）
   - 工作线程从 `outputQueue` 取出 scheduler 生成的数据并发射到下游
5. **两种机制的协调**：
   - scheduler 线程：定时生成数据 → 放入 `outputQueue`
   - BaseSplitReader 工作线程：从 `outputQueue` 取数据 → 发射到下游
   - `outputQueue` 作为两个线程之间的缓冲区，自动协调生产/消费速率

**实现细节补充：**
- 在 `fetchStreamingData()` 开始处添加检查：`if (scheduler != null && !scheduler.isShutdown()) return;` 防止重复启动
- 添加注释说明 streaming 模式不设置 `finished = true`
- 在 `close()` 中关闭 scheduler，停止数据生成

### DataRowGenerator 工具类

```java
public class DataRowGenerator {

    public static List<Row> generateRows(List<RowData> rowsData, EtlSchema schema) {
        List<Row> rows = new ArrayList<>();
        for (RowData rowData : rowsData) {
            Row row = generateRow(rowData, schema);
            rows.add(row);
        }
        return rows;
    }

    private static Row generateRow(RowData rowData, EtlSchema schema) {
        // 创建 Row，设置 RowKind
        RowKind rowKind = parseRowKind(rowData.getKind());
        Row row = Row.withKind(rowKind);

        // 设置字段值（严格匹配 schema）
        Map<String, Object> data = rowData.getData();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String fieldName = schema.getFieldName(i);
            Object value = data.get(fieldName);

            // 校验字段存在性
            if (value == null) {
                throw new SchemaConfigException(
                    "rows 配置缺失字段 '" + fieldName + "'，必须匹配 schema 定义的所有字段");
            }

            // 校验类型匹配
            TypeInformation<?> expectedType = schema.getFieldType(i);
            Object convertedValue = convertValue(value, expectedType, fieldName);
            row.setField(i, convertedValue);
        }

        return row;
    }

    private static RowKind parseRowKind(String kind) {
        switch (kind.toUpperCase()) {
            case "INSERT":
                return RowKind.INSERT;
            case "UPDATE_BEFORE":
                return RowKind.UPDATE_BEFORE;
            case "UPDATE_AFTER":
                return RowKind.UPDATE_AFTER;
            case "DELETE":
                return RowKind.DELETE;
            default:
                throw new SchemaConfigException("无效的 RowKind: " + kind);
        }
    }

    private static Object convertValue(Object value, TypeInformation<?> type, String fieldName) {
        // 类型转换逻辑（参考 JsonToRowConverter）
        // INT → Integer
        // LONG → Long
        // DOUBLE → Double
        // DECIMAL → BigDecimal
        // TIMESTAMP → Timestamp
        // STRING → String
        // BOOLEAN → Boolean

        // 类型不匹配时抛异常
    }
}
```

### RandomRowGenerator 工具类

```java
public class RandomRowGenerator {

    private static final Random random = new Random();

    public static List<Row> generateRows(EtlSchema schema, int numRows) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            Row row = generateRow(schema);
            rows.add(row);
        }
        return rows;
    }

    public static Row generateRow(EtlSchema schema) {
        // 随机生成的数据全部为 INSERT 类型
        Row row = Row.withKind(RowKind.INSERT);

        for (int i = 0; i < schema.getFieldCount(); i++) {
            TypeInformation<?> type = schema.getFieldType(i);
            Object value = generateValue(type);
            row.setField(i, value);
        }

        return row;
    }

    private static Object generateValue(TypeInformation<?> type) {
        if (type == Types.STRING) {
            return "field_" + UUID.randomUUID().toString().substring(0, 8);
        } else if (type == Types.BOOLEAN) {
            return random.nextBoolean();
        } else if (type == Types.INT) {
            return random.nextInt(10001); // 0-10000
        } else if (type == Types.LONG) {
            return (long) random.nextInt(10001);
        } else if (type == Types.DOUBLE) {
            return random.nextDouble() * 10000.0;
        } else if (type == Types.BIG_DEC) {
            return BigDecimal.valueOf(random.nextDouble() * 10000.0).setScale(2, RoundingMode.HALF_UP);
        } else if (type == Types.SQL_TIMESTAMP) {
            return new Timestamp(System.currentTimeMillis());
        } else {
            throw new SchemaConfigException("不支持的类型: " + type);
        }
    }
}
```

---

## 数据流转机制

### Batch 模式数据流转

```
JobExecutor.execute()
  └── JobBuilder.build()
        ├── MockSource.createEnumerator()
        │     └── MockSplitEnumerator.start()
        │           └── 创建 MockSplit("mock-split-0")
        │           └── 添加到 pendingSplits 队列
        │           └── callAllReadersToRequestSplits()
        ├── MockSource.createReader()
        │     └── MockSourceReader
        │           └── MockSplitReader (fetchNextBatch 线程)
        │                 ├── DataRowGenerator.generateRows() (rows 配置)
        │                 ├── 或 RandomRowGenerator.generateRows() (numRows)
        │                 └── Iterator<Row> →逐行读取
        │                       ├── Row 1 → outputQueue
        │                       ├── Row 2 → outputQueue
        │                       └── ...
        │                 └── 读完 → finished = true
        ├── MockRecordEmitter.emitRecord()
        │     └── Row → SourceOutput
        ├── DataStream<Row> → Table（注册为 outputTable）
        └── Transform/Sink 从 Table 读取
```

### Streaming 模式数据流转

```
JobExecutor.execute()
  └── JobBuilder.build()
        ├── MockSource.createEnumerator()
        │     └── MockSplitEnumerator.start()
        │           └── 创建 MockSplit("mock-split-0")
        │           └── 添加到 pendingSplits 队列
        │           └── callAllReadersToRequestSplits()
        ├── MockSource.createReader()
        │     └── MockSourceReader
        │           └── MockSplitReader (fetchNextBatch 线程)
        │                 ├── ScheduledExecutorService 启动
        │                 └── scheduleAtFixedRate(intervalMs)
        │                       ├── 间隔 0ms → RandomRowGenerator.generateRow()
        │                       ├── 间隔 intervalMs → RandomRowGenerator.generateRow()
        │                       ├── 间隔 intervalMs → RandomRowGenerator.generateRow()
        │                       └── ... 无限循环
        │                       └── Row → outputQueue (每行)
        ├── MockRecordEmitter.emitRecord()
        │     └── Row → SourceOutput
        ├── DataStream<Row> → Table（注册为 outputTable）
        └── Transform/Sink 从 Table 读取（持续消费）
```

### 数据生成时序图

**Batch 模式：**

```
MockSplitReader        DataRowGenerator      RandomRowGenerator     OutputQueue
    |                        |                      |                   |
    |-- fetchNextBatch() -->|                      |                   |
    |                        |-- generateRows() -->|                   |
    |                        |                      |-- generateRows()|
    |                        |<----- List<Row> -----|                   |
    |<--- Iterator<Row> ------|                      |                   |
    |                        |                      |                   |
    |-- hasNext() -> true    |                      |                   |
    |-- next() -> Row1       |                      |                   |-- add(Row1) -->
    |-- next() -> Row2       |                      |                   |-- add(Row2) -->
    |-- ...                  |                      |                   |-- add(...) -->
    |-- hasNext() -> false   |                      |                   |
    |-- finished = true      |                      |                   |
```

**Streaming 模式：**

```
MockSplitReader        ScheduledExecutor      RandomRowGenerator     OutputQueue
    |                        |                      |                   |
    |-- fetchNextBatch() -->|                      |                   |
    |                        |-- scheduleAtFixedRate|                   |
    |                        |                      |                   |
    |                        |-- intervalMs: 0 --> |-- generateRow() -->|-- add(Row) -->
    |                        |-- intervalMs: 1000--|-- generateRow() -->|-- add(Row) -->
    |                        |-- intervalMs: 1000--|-- generateRow() -->|-- add(Row) -->
    |                        |-- ... 无限循环      |-- generateRow() -->|-- add(Row) -->
    |                        |                      |                   |
    |-- close() ----------->|                      |                   |
    |                        |-- shutdown()        |                   |
```

---

## 测试策略

### 单元测试

**MockSourceTest：**
- 配置解析测试：rows、numRows、intervalMs 参数解析
- Schema 校验测试：简单类型通过、复杂类型抛异常
- 模式适配测试：batch/streaming 模式下参数冲突警告
- Boundedness 测试：batch 返回 BOUNDED，streaming 返回 UNBOUNDED

**MockSplitEnumeratorTest：**
- start() 测试：创建单分片并添加到队列
- snapshotState() 测试：检查点保存和恢复

**MockSplitReaderTest：**
- batch 模式 - rows 配置：读取固定数据，读完标记 finished
- batch 模式 - numRows 随机生成：生成指定行数，读完标记 finished
- streaming 模式：定时生成数据，close() 停止调度器
- 数据完整性测试：Row 字段数量、类型、RowKind 正确性

**DataRowGeneratorTest：**
- RowKind 解析测试：INSERT、UPDATE_BEFORE、UPDATE_AFTER、DELETE
- 类型转换测试：JSON 值 → Flink 类型转换
- 异常测试：缺失字段、类型不匹配、无效 RowKind

**RandomRowGeneratorTest：**
- 单行生成测试：字段数量、类型正确性
- 多行生成测试：生成 numRows 行
- 值范围测试：INT/LONG 0-10000，DOUBLE 0-10000.0

### 集成测试

**完整任务测试：**

使用 MiniCluster 运行完整 ETL 任务：

```java
@Test
public void testBatchMockToConsole() throws Exception {
    String configJson = loadConfig("mock-batch-to-console.json");
    JobConfig jobConfig = ConfigParser.parse(configJson);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    JobExecutor.execute(jobConfig, env);

    env.execute("mock-batch-test");

    // 验证 Console Sink 输出日志
}

@Test
public void testStreamingMockToConsole() throws Exception {
    String configJson = loadConfig("mock-streaming-to-console.json");
    JobConfig jobConfig = ConfigParser.parse(configJson);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    JobExecutor.execute(jobConfig, env);

    // 启动任务并运行 5 秒后取消
    JobClient jobClient = env.executeAsync("mock-streaming-test");
    Thread.sleep(5000);
    jobClient.cancel();

    // 验证 Console Sink 输出日志数量
}
```

### 测试配置文件

**docs/examples/mock-batch-to-console.json：**
```json
{
  "job": {
    "name": "mock-batch-test",
    "mode": "batch"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "test_data",
    "config": {
      "schema": {
        "id": "LONG",
        "value": "INT"
      },
      "rows": [
        { "kind": "INSERT", "data": { "id": 1, "value": 100 } },
        { "kind": "INSERT", "data": { "id": 2, "value": 200 } }
      ]
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "test_data",
    "config": {}
  }]
}
```

**docs/examples/mock-streaming-to-console.json：**
```json
{
  "job": {
    "name": "mock-streaming-test",
    "mode": "streaming"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "stream_data",
    "config": {
      "schema": {
        "id": "LONG",
        "name": "STRING"
      },
      "intervalMs": 500
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "stream_data",
    "config": {}
  }]
}
```

---

## 扩展性考虑

### 可能的扩展点

1. **多分片支持**：
   - 当前单分片设计，未来可扩展支持多 Mock 实例并行
   - 通过配置参数 `parallelism` 控制分片数量
   - 每个分片独立生成数据，提高吞吐量

2. **生成策略扩展**：
   - 当前固定规则生成，未来可支持可配置范围：`valueRanges: {"age": [18, 60]}`
   - 支持字段关联生成：如 `email` 字段基于 `name` 字段生成邮箱格式
   - 支持模板生成：如 `"template": "user_{id}"` 格式化字符串

3. **CDC 流生成**：
   - 当前随机生成全部为 INSERT，未来可支持 CDC 混合生成模式
   - 配置参数：`cdcRatio: {"INSERT": 0.8, "UPDATE": 0.1, "DELETE": 0.1}`
   - 随机生成时按比例选择 RowKind

4. **数据格式扩展**：
   - 当前支持 JSON rows 配置，未来可支持 CSV、YAML 格式
   - 支持从外部文件加载 rows 配置：`rowsFile: "/path/to/data.json"`

5. **Schema 自动推断**：
   - 当前 schema 必填，未来可从 rows 配置自动推断 schema
   - 减少配置工作量，适合快速原型验证

### 扩展实现建议

**多分片实现**：
- 修改 `MockSplitEnumerator`：在 `start()` 时根据 `parallelism` 创建多个分片
- 分片 ID 从 `"mock-split-0"` 改为 `"mock-split-{i}"`（i 从 0 到 parallelism-1）
- 每个 Reader 分配一个分片，独立生成数据

**生成策略扩展**：
- 新增配置类：`ValueGenerationStrategy`
- 新增工具类：`SmartRowGenerator`（根据字段名智能生成）
- 新增工具类：`ConfigurableRowGenerator`（根据配置范围生成）

---

## 附录

### Maven 模块配置

**flink-etl-source-mock/pom.xml：**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>flink-etl-source</artifactId>
        <groupId>com.etl</groupId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>flink-etl-source-mock</artifactId>
    <name>Mock Source Plugin</name>

    <dependencies>
        <!-- 核心框架依赖 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-core</artifactId>
            <version>${project.version}</version>
        </dependency>

        <!-- AutoService 注解 -->
        <dependency>
            <groupId>com.google.auto.service</groupId>
            <artifactId>auto-service</artifactId>
            <version>1.0.1</version>
            <scope>provided</scope>
        </dependency>

        <!-- 测试依赖 -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.9.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

### SPI 配置文件

**META-INF/services/com.etl.core.spi.SourcePlugin：**

```
com.etl.connector.mock.source.MockSourcePlugin
```

---

## 总结

Mock Source 通过简化单分片实现，提供轻量级的测试数据生成能力，无需外部依赖即可快速验证 ETL 任务配置。设计遵循项目现有的 Source 抽象层规范，复用 BaseSplitEnumerator 和 BaseSourceReader 基础设施，保持架构一致性。配置灵活支持固定数据和随机生成两种模式，适配 batch/streaming 运行场景，支持 CDC RowKind 标记，可完整测试 JDBC Sink 的 CDC 写入逻辑。