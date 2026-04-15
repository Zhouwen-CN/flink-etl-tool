# JDBC Sink Executor 抽象层重构设计

**日期**：2026-04-15
**目标**：借鉴 Seatunnel 的 JDBC Sink 设计，引入 Executor 抽象层，优化 CDC/UPSERT 模式的性能和正确性。

---

## 一、背景与动机

### 当前实现的问题

1. **CDC 模式效率不足**：同一主键的多次变更（INSERT → UPDATE → DELETE）会执行多条 SQL，中间状态的 SQL 浪费数据库资源
2. **缺少 UPDATE_BEFORE 过滤**：CDC 流中的 UPDATE_BEFORE 数据没有特殊处理，占用不必要的处理资源
3. **Upsert/Delete 执行顺序隐患**：UPSERT 和 DELETE 操作混在一个 JDBC batch 中执行，可能存在顺序混乱风险
4. **缺少时间阈值刷写**：只有 `batch_size` 触发刷写，低频数据场景下数据可能长时间滞留在缓冲区

### Seatunnel 的优秀设计

Seatunnel JDBC Sink 通过以下机制解决上述问题：

- **Key 归并缓冲**：`LinkedHashMap<RowKey, RowData>` 按主键归并，同一主键的多次操作只保留最终状态
- **UPDATE_BEFORE 跳过**：CDC 模式下直接丢弃 UPDATE_BEFORE 数据
- **分段执行**：UPSERT 和 DELETE 分开 batch 执行，中间有 commit 隔断保证顺序
- **时间阈值刷写**：`batch_interval_ms` 配置定时刷写，确保数据及时写入

---

## 二、设计目标

### 核心目标

1. **引入 Executor 抽象层**：分离刷写逻辑（OutputFormat）和执行逻辑（Executor）
2. **CDC + UPSERT 模式使用 Key 归并**：减少无效 SQL，提升效率
3. **增加时间阈值刷写**：新增 `batchIntervalMs` 配置（默认 0 禁用）
4. **保持向后兼容**：INSERT/CUSTOM 模式行为不变

### 非目标

- Exactly-Once 语义（未来扩展）
- 动态 Schema 演进
- 多表写入支持

---

## 三、架构设计

### 整体架构图

```
写入流程：
Row 数据 → JdbcSinkWriter.write()
  → JdbcOutputFormat.writeRecord()
    → JdbcBatchStatementExecutor.addToBatch()
      → BufferReducedExecutor: LinkedHashMap 归并
    → 检查刷写条件（batch_size 或 batch_interval_ms）
      → flush()
        → BufferReducedExecutor.executeBatch()
          → 分段执行：先 Upsert batch → commit → 再 Delete batch → commit
```

### 类结构

```
com.etl.connector.jdbc.sink/
  ├─ JdbcSinkWriter.java              ← 简化为调用 OutputFormat
  ├─ JdbcOutputFormat.java            ← 新增：管理刷写逻辑（batch + interval）
  ├─ JdbcOutputFormatBuilder.java     ← 新增：根据模式创建 Executor
  ├─ executor/                        ← 新增包
  │   ├─ JdbcBatchStatementExecutor.java   ← 接口
  │   ├─ SimpleBufferedExecutor.java       ← INSERT 模式
  │   ├─ BufferReducedExecutor.java        ← CDC/UPSERT 模式（Key 归并）
  │   └─ SimpleBatchExecutor.java          ← CUSTOM 模式
  │
  └─ config/
      └─ JdbcSinkConfig.java          ← 新增 batchIntervalMs 字段
```

---

## 四、核心组件设计

### 4.1 JdbcBatchStatementExecutor 接口

**职责**：定义 JDBC 批量执行的抽象，不同模式有不同的实现策略。

```java
public interface JdbcBatchStatementExecutor {
    // 初始化 PreparedStatement
    void prepareStatements(Connection connection) throws SQLException;

    // 添加数据到批次
    void addToBatch(Row record) throws SQLException;

    // 执行当前批次
    void executeBatch() throws SQLException;

    // 关闭 Statement
    void closeStatements() throws SQLException;
}
```

### 4.2 BufferReducedExecutor（CDC/UPSERT 核心）

**职责**：Key 归并缓冲 + 分段执行，保证 CDC 数据的正确性和效率。

**核心数据结构**：
```java
public class BufferReducedExecutor implements JdbcBatchStatementExecutor {
    private final JdbcBatchStatementExecutor upsertExecutor;   // SimpleBatchExecutor
    private final JdbcBatchStatementExecutor deleteExecutor;   // SimpleBatchExecutor
    private final Function<Row, Row> keyExtractor;             // 提取主键（基于 keyFields）
    private final Function<Row, Row> valueTransform;           // 数据转换
    private final boolean isCdcMode;                           // CDC 模式标识

    // 核心缓冲结构：LinkedHashMap 保证流入顺序
    private LinkedHashMap<Row, Pair<Boolean, Row>> buffer;     // Boolean = changeFlag
}
```

**addToBatch 逻辑**：
```java
void addToBatch(Row record) throws SQLException {
    RowKind kind = record.getKind();

    // CDC 模式：UPDATE_BEFORE 直接跳过
    if (isCdcMode && kind == RowKind.UPDATE_BEFORE) {
        return;
    }

    Row key = keyExtractor.apply(record);      // 提取主键
    boolean changeFlag = (kind == INSERT || kind == UPDATE_AFTER);
    Row value = valueTransform.apply(record);

    buffer.put(key, Pair.of(changeFlag, value));  // 同 key 自动归并，保留最终状态
}
```

**executeBatch 逻辑**（分段执行保证顺序）：
```java
void executeBatch() throws SQLException {
    Boolean prevFlag = null;
    for (Entry<Row, Pair<Boolean, Row>> entry : buffer.entrySet()) {
        boolean currentFlag = entry.getValue().getLeft();

        if (currentFlag) {  // Upsert
            if (prevFlag != null && !prevFlag) {
                deleteExecutor.executeBatch();  // 先执行完前面的 delete
            }
            upsertExecutor.addToBatch(entry.getValue().getRight());
        } else {  // Delete
            if (prevFlag != null && prevFlag) {
                upsertExecutor.executeBatch();  // 先执行完前面的 upsert
            }
            deleteExecutor.addToBatch(entry.getKey());
        }
        prevFlag = currentFlag;
    }

    // 执行最后的批次
    if (prevFlag != null) {
        if (prevFlag) upsertExecutor.executeBatch();
        else deleteExecutor.executeBatch();
    }

    buffer.clear();
}
```

### 4.3 JdbcOutputFormat（刷写管理）

**职责**：管理批量刷写逻辑，双重检查（batch_size + batch_interval_ms）。

```java
public class JdbcOutputFormat<I> {
    private final JdbcBatchStatementExecutor executor;
    private final Connection connection;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;

    private int batchCount = 0;
    private long lastFlushTimeMs;

    void writeRecord(I record) throws SQLException {
        executor.addToBatch(record);
        batchCount++;

        // 双重检查：数量 或 时间
        if (batchCount > 0 && (isOverBatchSize() || isOverInterval())) {
            flush();
        }
    }

    synchronized void flush() throws IOException {
        if (batchCount == 0) return;

        for (int retry = 0; retry <= maxRetries; retry++) {
            try {
                executor.executeBatch();
                connection.commit();
                batchCount = 0;
                lastFlushTimeMs = System.currentTimeMillis();
                break;
            } catch (SQLException e) {
                if (retry >= maxRetries) {
                    connection.rollback();
                    throw new IOException("Flush failed after " + maxRetries + " retries", e);
                }
                Thread.sleep(1000 * retry);
            }
        }
    }

    private boolean isOverBatchSize() {
        return batchSize > 0 && batchCount >= batchSize;
    }

    private boolean isOverInterval() {
        return batchIntervalMs > 0
            && (System.currentTimeMillis() - lastFlushTimeMs) >= batchIntervalMs;
    }
}
```

---

## 五、模式与 Executor 映射

| 写入模式 | Executor 实现 | Key 归并 | UPDATE_BEFORE 跳过 | 分段执行 |
|---|---|---|---|---|
| `INSERT` | `SimpleBufferedExecutor` | ❌ | ❌ | ❌ |
| `UPSERT` | `BufferReducedExecutor` | ✅ | ❌ | ✅ |
| `CDC` | `BufferReducedExecutor` | ✅ | ✅ | ✅ |
| `CUSTOM` | `SimpleBatchExecutor` | ❌ | ❌ | ❌ |

**Executor 工厂创建逻辑**（JdbcOutputFormatBuilder）：
```java
JdbcBatchStatementExecutor createExecutor(JdbcSinkConfig config, String[] columns) {
    switch (config.getMode()) {
        case INSERT:
            String insertSql = config.getDialect().getInsertSql(config.getTable(), columns);
            return new SimpleBufferedExecutor(insertSql, columns);

        case UPSERT:
            return new BufferReducedExecutor(
                config.getDialect(),
                config.getTable(),
                columns,
                config.getKeyFields(),
                false  // 非 CDC 模式，不跳过 UPDATE_BEFORE
            );

        case CDC:
            return new BufferReducedExecutor(
                config.getDialect(),
                config.getTable(),
                columns,
                config.getKeyFields(),
                true   // CDC 模式，跳过 UPDATE_BEFORE
            );

        case CUSTOM:
            return new SimpleBatchExecutor(config.getSql());
    }
}
```

---

## 六、配置变更

### JdbcSinkConfig 新增字段

```java
@Getter
@Builder
public class JdbcSinkConfig implements Serializable {
    // ... 现有字段保持不变 ...

    /** 批量刷写间隔（毫秒），默认 0 表示禁用 */
    private final Long batchIntervalMs;
}
```

### 配置解析（JdbcSink）

```java
public JdbcSink(SinkConfig config) {
    // ... 现有解析逻辑 ...

    Long batchIntervalMs = config.getLong("batchIntervalMs", 0L);  // 默认禁用

    this.jdbcSinkConfig = JdbcSinkConfig.builder()
            // ... 现有字段 ...
            .batchIntervalMs(batchIntervalMs)
            .build();
}
```

**用户配置示例**：
```json
{
  "sinks": [{
    "type": "jdbc",
    "url": "jdbc:mysql://localhost:3306/test",
    "table": "users",
    "mode": "CDC",
    "batchSize": 1000,
    "batchIntervalMs": 5000  // 每 5 秒刷写一次（可选）
  }]
}
```

---

## 七、Writer 简化

重构后 `JdbcSinkWriter` 变得非常简洁，职责仅为初始化和调用 OutputFormat。

```java
public class JdbcSinkWriter extends AbstractSinkWriter<JdbcSinkConfig> {
    private final JdbcOutputFormat<Row> outputFormat;
    private final Connection connection;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
        super(context, config);

        // 初始化连接
        connection = DriverManager.getConnection(
            config.getUrl(), config.getUsername(), config.getPassword());
        connection.setAutoCommit(false);

        // 创建 OutputFormat
        this.outputFormat = new JdbcOutputFormatBuilder(config, connection).build();
        this.outputFormat.open();
    }

    @Override
    public void write(Row row, Context context) throws IOException {
        outputFormat.writeRecord(row);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        outputFormat.flush();
    }

    @Override
    public void close() throws IOException {
        outputFormat.close();
        connection.close();
    }
}
```

---

## 八、数据流转示例

**CDC 模式完整示例**：

```
输入数据流（按时间顺序）：
1. Row(INSERT, id=1, name='张三', age=20)
2. Row(UPDATE_BEFORE, id=1, name='张三')        ← UPDATE_BEFORE 被跳过
3. Row(UPDATE_AFTER, id=1, name='李四', age=20)
4. Row(INSERT, id=2, name='王五', age=25)
5. Row(DELETE, id=3, name='赵六')
6. Row(UPDATE_AFTER, id=1, name='钱七', age=22)
7. Row(DELETE, id=1)

BufferReducedExecutor.addToBatch() 后 buffer 状态：
  LinkedHashMap {
    key=Row(1) → Pair(true, Row(1, '李四', 20))    ← 被后面的覆盖
    key=Row(2) → Pair(true, Row(2, '王五', 25))
    key=Row(3) → Pair(false, Row(3))              ← DELETE
    key=Row(1) → Pair(false, Row(1))              ← 最终状态是 DELETE
  }

executeBatch() 执行顺序：
1. 遍历到 id=2 (changeFlag=true)：添加到 upsertExecutor
2. 遍历到 id=3 (changeFlag=false)：先执行 upsertExecutor batch → commit
   → UPSERT id=2
3. 继续添加 id=3 到 deleteExecutor
4. 遍历到 id=1 (changeFlag=false)：继续添加到 deleteExecutor
5. 结束：执行 deleteExecutor batch → commit
   → DELETE id=3, DELETE id=1

最终数据库执行的 SQL：
  UPSERT id=2 ('王五', 25)  → 1 条
  DELETE id=3              → 1 条
  DELETE id=1              → 1 条
共 3 条 SQL（原始 7 条数据归并后）
```

---

## 九、错误处理

### 刷写失败重试

`JdbcOutputFormat.flush()` 实现重试机制：

- 重试次数：`maxRetries`（从配置读取，默认 3）
- 重试间隔：指数增长（1000ms × retry次数）
- 失败处理：回滚事务，抛出 IOException → Flink checkpoint 失败 → 任务重启

### 异常传播路径

```
SQLException → IOException → Flink checkpoint 失败 → 任务重启 → 从上次成功的 checkpoint 重放数据
```

---

## 十、测试策略

| 测试场景 | 测试内容 |
|---|---|
| **Key 归并正确性** | 同主键多次操作 → 只保留最终状态 |
| **UPDATE_BEFORE 跳过** | CDC 模式下 UPDATE_BEFORE 不进入 buffer |
| **分段执行顺序** | Upsert → Delete → Upsert 混合时顺序正确 |
| **时间阈值刷写** | batchIntervalMs 配置生效，定时刷写 |
| **配置默认值** | batchIntervalMs=0 时禁用时间阈值 |
| **边界情况** | 空 buffer、单条数据、超大批次 |
| **错误重试** | flush 失败后重试机制生效 |

---

## 十一、实现优先级

1. **核心 Executor**：`JdbcBatchStatementExecutor` 接口 + `BufferReducedExecutor` 实现
2. **刷写管理**：`JdbcOutputFormat` + `JdbcOutputFormatBuilder`
3. **配置变更**：`JdbcSinkConfig` 新增字段 + `JdbcSink` 解析
4. **Writer 简化**：`JdbcSinkWriter` 重构
5. **测试覆盖**：单元测试 + 集成测试

---

## 十二、向后兼容性

- INSERT 模式：行为不变（使用 SimpleBufferedExecutor）
- CUSTOM 模式：行为不变（使用 SimpleBatchExecutor）
- 配置兼容：`batchIntervalMs` 默认值 0，不影响现有配置
- API 兼容：`JdbcSinkWriter` 公开方法签名不变

---

## 十三、未来扩展点

- **Exactly-Once 语义**：新增 `ExactlyOnceExecutor` 实现 XA 事务
- **多表写入**：扩展 `JdbcOutputFormat` 支持多表管理
- **动态 Schema**：扩展 Executor 支持字段变更