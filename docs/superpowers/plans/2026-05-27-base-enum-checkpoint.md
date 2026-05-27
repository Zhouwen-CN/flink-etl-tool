# BaseEnumCheckpoint 重构实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 6 个空壳 `XxxEnumCheckpoint` 子类，将 `snapshotState` 下沉到 `AbstractSplitEnumerator`，并去掉
`AbstractSplitSource` / `AbstractSplitEnumerator` 冗余的 `CheckpointT` 泛型参数。

**Architecture:** 把抽象类 `AbstractEnumCheckpoint` 改造为可实例化的 `BaseEnumCheckpoint`，作为统一的检查点类型。
`AbstractSplitSource` 与 `AbstractSplitEnumerator` 不再以 `CheckpointT` 作为泛型参数，而是固定使用
`BaseEnumCheckpoint<SplitT>`。6 个连接器同步改造，删除空壳子类。

**Tech Stack:** Java 1.8 / Apache Flink 1.15.2 / Maven / Lombok

**Spec:** [
`docs/superpowers/specs/2026-05-27-base-enum-checkpoint-design.md`](../specs/2026-05-27-base-enum-checkpoint-design.md)

---

## Pre-flight 检查

执行计划前，确保工作树干净：

```bash
git status
```

预期：`nothing to commit, working tree clean`（或仅有未追踪的本地文件）。

---

## 任务总览

```
Task 1: 核心层重构 — BaseEnumCheckpoint + AbstractSplitEnumerator + AbstractSplitSource + DefaultCheckpointSerializer
Task 2: jdbc 连接器收敛
Task 3: mqtt 连接器收敛
Task 4: http 连接器收敛
Task 5: localfile 连接器收敛
Task 6: mock 连接器收敛
Task 7: modbus 连接器收敛
Task 8: 全量构建 + 测试验证
```

**关键约束**：Task 1 完成后，整个项目的 6 个连接器会立即编译失败（因为类型签名变了）。这是预期状态，由 Task 2-7 逐个修复。Task
1 提交后**不要单独跑全量编译**，否则会满屏报错；只在 Task 8 做全量验证。

---

## Task 1: 核心层重构

**Files:**

- Create: `flink-etl-core/src/main/java/com/etl/core/source/BaseEnumCheckpoint.java`
- Delete: `flink-etl-core/src/main/java/com/etl/core/source/AbstractEnumCheckpoint.java`
- Modify: `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitEnumerator.java`
- Modify: `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java`
- Modify: `flink-etl-core/src/main/java/com/etl/core/source/serde/DefaultCheckpointSerializer.java`

### Step 1.1: 创建 BaseEnumCheckpoint

- [ ] 创建文件 `flink-etl-core/src/main/java/com/etl/core/source/BaseEnumCheckpoint.java`，完整内容如下：

```java
package com.etl.core.source;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collection;

/**
 * 枚举器检查点
 * 用于保存 SplitEnumerator 的状态，支持故障恢复
 *
 * @param <SplitT> 分片类型
 */
@Getter
public class BaseEnumCheckpoint<SplitT extends BaseSourceSplit> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 待处理的分片集合 */
    protected final Collection<SplitT> pendingSplits;

    public BaseEnumCheckpoint(Collection<SplitT> pendingSplits) {
        this.pendingSplits = pendingSplits;
    }

    @Override
    public String toString() {
        return "BaseEnumCheckpoint{pendingSplits=" + pendingSplits + '}';
    }
}
```

### Step 1.2: 删除 AbstractEnumCheckpoint

- [ ] 删除文件 `flink-etl-core/src/main/java/com/etl/core/source/AbstractEnumCheckpoint.java`

```bash
rm flink-etl-core/src/main/java/com/etl/core/source/AbstractEnumCheckpoint.java
```

### Step 1.3: 重写 AbstractSplitEnumerator

- [ ] 将 `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitEnumerator.java` 完整替换为：

```java
package com.etl.core.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 分片枚举器抽象基类
 * 封装了分片分配的通用逻辑，子类只需关注分片发现
 *
 * <p>该类自动处理：
 * <ul>
 *   <li>handleSplitRequest - 从队列中分配分片给 Reader</li>
 *   <li>addSplitsBack - Reader 失败时回收未处理的分片</li>
 *   <li>snapshotState - 将待处理分片快照为 BaseEnumCheckpoint</li>
 * </ul>
 *
 * @param <SplitT> 分片类型
 */
@Slf4j
public abstract class AbstractSplitEnumerator<SplitT extends BaseSourceSplit>
        implements SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>> {

    /** 待分配的分片队列（线程安全） */
    protected final Queue<SplitT> pendingSplits = new ConcurrentLinkedQueue<>();

    /** 枚举器上下文 */
    protected final SplitEnumeratorContext<SplitT> context;

    public AbstractSplitEnumerator(SplitEnumeratorContext<SplitT> context) {
        this.context = context;
    }

    public AbstractSplitEnumerator(SplitEnumeratorContext<SplitT> context,
                                   BaseEnumCheckpoint<SplitT> checkpoint) {
        this(context);
        if (checkpoint != null && checkpoint.getPendingSplits() != null) {
            pendingSplits.addAll(checkpoint.getPendingSplits());
            log.info("从检查点恢复 {} 个待处理分片", pendingSplits.size());
        }
    }

    /**
     * 处理分片请求
     * 自动从队列中取出分片分配给请求的 Reader，如果队列为空则通知无更多分片
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        SplitT split = pendingSplits.poll();
        if (split != null) {
            log.debug("分配分片 {} 给 Reader {}", split.splitId(), subtaskId);
            context.assignSplit(split, subtaskId);
        } else {
            log.debug("无更多分片，通知 Reader {}", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    /**
     * 将分配失败的分片返回到待处理队列
     * 当 Reader 失败时会调用此方法
     */
    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        log.warn("Reader {} 返回 {} 个未处理的分片", subtaskId, splits.size());
        pendingSplits.addAll(splits);
    }

    /**
     * 子类可覆盖：处理 Reader 注册通知
     */
    @Override
    public void addReader(int subtaskId) {
        log.debug("Reader {} 已注册", subtaskId);
    }

    /**
     * 默认快照实现：将待处理分片打包为 BaseEnumCheckpoint
     * 子类如无特殊状态，无需覆盖
     */
    @Override
    public BaseEnumCheckpoint<SplitT> snapshotState(long checkpointId) {
        List<SplitT> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new BaseEnumCheckpoint<>(pending);
    }

    /**
     * 获取待处理分片数量
     */
    protected int getPendingSplitCount() {
        return pendingSplits.size();
    }

    /**
     * 添加分片到待处理队列
     */
    protected void addPendingSplits(List<SplitT> splits) {
        pendingSplits.addAll(splits);
        log.debug("添加 {} 个分片到待处理队列，当前队列大小: {}", splits.size(), pendingSplits.size());
    }
}
```

### Step 1.4: 重写 AbstractSplitSource

- [ ] 将 `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java` 完整替换为：

```java
package com.etl.core.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

/**
 * 支持分片的 Source 抽象基类
 * 简化 Flink FLIP-27 Source API 的实现
 *
 * @param <SplitT> 分片类型
 */
public abstract class AbstractSplitSource<SplitT extends BaseSourceSplit>
        implements Source<Row, SplitT, BaseEnumCheckpoint<SplitT>>, ResultTypeQueryable<Row> {

    protected final SourceConfig config;

    public AbstractSplitSource(SourceConfig config) {
        this.config = config;
    }

    /**
     * 所有 source 默认的 batchSize
     */
    protected int getDefaultBatchSize() {
        return 100;
    }

    @Override
    public abstract SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>>
            createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>>
            restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext,
                              BaseEnumCheckpoint<SplitT> checkpoint);

    @Override
    public abstract SourceReader<Row, SplitT> createReader(SourceReaderContext readerContext);

    @Override
    public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

    @Override
    public abstract SimpleVersionedSerializer<BaseEnumCheckpoint<SplitT>>
            getEnumeratorCheckpointSerializer();

    /**
     * 默认从 source.schema 中获取，子类可以重写
     */
    @Override
    public TypeInformation<Row> getProducedType() {
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            throw new SchemaConfigException("schema is null");
        }
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}
```

### Step 1.5: 重写 DefaultCheckpointSerializer

- [ ] 将 `flink-etl-core/src/main/java/com/etl/core/source/serde/DefaultCheckpointSerializer.java` 完整替换为：

```java
package com.etl.core.source.serde;

import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.BaseSourceSplit;
import com.etl.core.utils.SerializerUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * 默认的检查点序列化器
 * 使用 JDK 序列化
 *
 * @param <SplitT> 分片类型
 */
public class DefaultCheckpointSerializer<SplitT extends BaseSourceSplit>
        implements SimpleVersionedSerializer<BaseEnumCheckpoint<SplitT>> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BaseEnumCheckpoint<SplitT> checkpoint) throws IOException {
        return SerializerUtils.serialize(checkpoint);
    }

    @Override
    @SuppressWarnings("unchecked")
    public BaseEnumCheckpoint<SplitT> deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("无法读取未来版本的数据，当前版本: " + VERSION + "，数据版本: " + version);
        }
        return (BaseEnumCheckpoint<SplitT>) SerializerUtils.deserialize(serialized);
    }
}
```

### Step 1.6: 编译核心模块验证

- [ ] 单独编译 `flink-etl-core` 模块，确认核心层自身已闭环：

```bash
mvn -pl flink-etl-core -am compile
```

预期：`BUILD SUCCESS`。

> 注意：此时不要跑全量 `mvn compile`，因为 6 个连接器还未调整，肯定会失败。

### Step 1.7: 提交核心层

- [ ] 提交：

```bash
git add flink-etl-core/src/main/java/com/etl/core/source/BaseEnumCheckpoint.java
git add flink-etl-core/src/main/java/com/etl/core/source/AbstractEnumCheckpoint.java
git add flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitEnumerator.java
git add flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java
git add flink-etl-core/src/main/java/com/etl/core/source/serde/DefaultCheckpointSerializer.java
git commit -m "refactor(core): 引入 BaseEnumCheckpoint 并下沉 snapshotState

- 将抽象类 AbstractEnumCheckpoint 改造为可实例化的 BaseEnumCheckpoint
- AbstractSplitEnumerator/AbstractSplitSource 去除 CheckpointT 泛型，固定使用 BaseEnumCheckpoint<SplitT>
- snapshotState 默认实现下沉到 AbstractSplitEnumerator
- DefaultCheckpointSerializer 简化为单泛型

此 commit 后 6 个连接器编译会失败，由后续任务逐个修复。"
```

---

## Task 2: jdbc 连接器收敛

**Files:**

- Delete: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcEnumCheckpoint.java`
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSource.java`
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSplitEnumerator.java`

### Step 2.1: 删除 JdbcEnumCheckpoint

- [ ] 执行：

```bash
rm flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcEnumCheckpoint.java
```

### Step 2.2: 改写 JdbcSource

- [ ] 将 `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSource.java` 完整替换为：

```java
package com.etl.connector.jdbc.source;

import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.core.utils.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 */
@Slf4j
public class JdbcSource extends AbstractSplitSource<JdbcSplit> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSource(SourceConfig config) {
        super(config);
        jdbcSourceConfig = JdbcSourceConfig.fromSourceConfig(config, super.getDefaultBatchSize());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<JdbcSplit, BaseEnumCheckpoint<JdbcSplit>>
    createEnumerator(SplitEnumeratorContext<JdbcSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, jdbcSourceConfig);
    }

    @Override
    public SplitEnumerator<JdbcSplit, BaseEnumCheckpoint<JdbcSplit>>
    restoreEnumerator(SplitEnumeratorContext<JdbcSplit> enumContext,
                      BaseEnumCheckpoint<JdbcSplit> checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, checkpoint, jdbcSourceConfig);
    }

    @Override
    public SourceReader<Row, JdbcSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<AbstractSplitReader<Row, JdbcSplit>> splitReaderSupplier = JdbcSplitReader::new;
        return new JdbcSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<JdbcSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<JdbcSplit>> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        String table = jdbcSourceConfig.getTable();
        String sql = jdbcSourceConfig.getSql();
        String url = jdbcSourceConfig.getUrl();
        String username = jdbcSourceConfig.getUsername();
        String password = jdbcSourceConfig.getPassword();

        return SqlUtils.inferRowType(
                table,
                sql,
                url,
                username,
                password
        );
    }
}
```

### Step 2.3: 改写 JdbcSplitEnumerator（删除 snapshotState）

- [ ] 将 `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSplitEnumerator.java`
  完整替换为：

```java
package com.etl.connector.jdbc.source;

import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import com.etl.connector.jdbc.source.splitter.ChunkSplitter;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.List;

/**
 * JDBC 分片枚举器
 * 继承 AbstractSplitEnumerator，在 start() 中执行分片计算
 *
 * <p>分片计算延迟到 enumerator 启动时执行，而非创建时预计算。
 * 这样可以在运行时动态获取数据范围，支持更灵活的分片策略。
 */
@Slf4j
public class JdbcSplitEnumerator extends AbstractSplitEnumerator<JdbcSplit> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSplitEnumerator(SplitEnumeratorContext<JdbcSplit> context, JdbcSourceConfig jdbcSourceConfig) {
        super(context);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 初始化");
    }

    public JdbcSplitEnumerator(SplitEnumeratorContext<JdbcSplit> context,
                               BaseEnumCheckpoint<JdbcSplit> checkpoint,
                               JdbcSourceConfig jdbcSourceConfig) {
        super(context, checkpoint);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.debug("JDBC SplitEnumerator 从检查点恢复，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public void start() {
        log.info("启动 SplitEnumerator，并行度: {}", context.currentParallelism());

        JdbcSourceConfig config = this.jdbcSourceConfig;
        int parallelism = context.currentParallelism();
        SplitStrategy strategy = config.getSplitStrategy();

        // 1. 创建对应的 Splitter
        ChunkSplitter splitter = ChunkSplitter.create(strategy, config, parallelism);

        // 2. 生成分片
        List<JdbcSplit> splits = splitter.generateSplits();
        log.info("共生成 {} 个分片", splits.size());

        // 3. 添加到待分配列表（由父类处理分配逻辑）
        addPendingSplits(splits);
        log.info("JDBC SplitEnumerator 启动完成");
    }

    @Override
    public void close() throws IOException {
        log.info("JDBC SplitEnumerator 关闭");
    }
}
```

### Step 2.4: 编译 jdbc 模块

- [ ] 执行：

```bash
mvn -pl flink-etl-connector/connector-jdbc -am compile
```

预期：`BUILD SUCCESS`。

### Step 2.5: 提交

- [ ] 执行：

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcEnumCheckpoint.java
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSource.java
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSplitEnumerator.java
git commit -m "refactor(jdbc): 删除 JdbcEnumCheckpoint 并改用 BaseEnumCheckpoint"
```

---

## Task 3: mqtt 连接器收敛

**Files:**

- Delete: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttEnumCheckpoint.java`
- Modify: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSource.java`
- Modify: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitEnumerator.java`

### Step 3.1: 删除 MqttEnumCheckpoint

- [ ] 执行：

```bash
rm flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttEnumCheckpoint.java
```

### Step 3.2: 改写 MqttSource

- [ ] 将 `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSource.java` 完整替换为：

```java
package com.etl.connector.mqtt.source;

import com.etl.connector.mqtt.source.config.MqttSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * MQTT Source 实现
 * 使用 Paho 客户端订阅 MQTT topic，消费 JSON 消息
 */
@Slf4j
public class MqttSource extends AbstractSplitSource<MqttSplit> {

    private final MqttSourceConfig mqttSourceConfig;

    public MqttSource(SourceConfig config) {
        super(config);
        this.mqttSourceConfig = MqttSourceConfig.fromSourceConfig(config);
        log.info("创建 MqttSource: broker={}, topic={}, clientId={}",
                mqttSourceConfig.getBroker(),
                mqttSourceConfig.getTopic(),
                mqttSourceConfig.getClientId());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MqttSplit, BaseEnumCheckpoint<MqttSplit>> createEnumerator(
            SplitEnumeratorContext<MqttSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new MqttSplitEnumerator(enumContext, mqttSourceConfig);
    }

    @Override
    public SplitEnumerator<MqttSplit, BaseEnumCheckpoint<MqttSplit>> restoreEnumerator(
            SplitEnumeratorContext<MqttSplit> enumContext,
            BaseEnumCheckpoint<MqttSplit> checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new MqttSplitEnumerator(enumContext, checkpoint, mqttSourceConfig);
    }

    @Override
    public SourceReader<Row, MqttSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<AbstractSplitReader<Row, MqttSplit>> splitReaderSupplier =
                MqttSplitReader::new;
        return new MqttSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<MqttSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<MqttSplit>> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
```

### Step 3.3: 改写 MqttSplitEnumerator（删除 snapshotState）

- [ ] 将 `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitEnumerator.java`
  完整替换为：

```java
package com.etl.connector.mqtt.source;

import com.etl.connector.mqtt.source.config.MqttSourceConfig;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.Collections;

/**
 * MQTT 分片枚举器
 * 单分片模式，创建一个包含完整配置的分片
 */
@Slf4j
public class MqttSplitEnumerator extends AbstractSplitEnumerator<MqttSplit> {

    private final MqttSourceConfig mqttSourceConfig;

    public MqttSplitEnumerator(
            SplitEnumeratorContext<MqttSplit> context,
            MqttSourceConfig mqttSourceConfig) {
        super(context);
        this.mqttSourceConfig = mqttSourceConfig;
    }

    public MqttSplitEnumerator(
            SplitEnumeratorContext<MqttSplit> context,
            BaseEnumCheckpoint<MqttSplit> checkpoint,
            MqttSourceConfig mqttSourceConfig) {
        super(context, checkpoint);
        this.mqttSourceConfig = mqttSourceConfig;
    }

    @Override
    public void start() {
        log.info("MqttSplitEnumerator 启动，broker: {}, topic: {}",
                mqttSourceConfig.getBroker(), mqttSourceConfig.getTopic());

        MqttSplit split = new MqttSplit("mqtt-split-0", mqttSourceConfig);
        addPendingSplits(Collections.singletonList(split));
        log.info("创建 MQTT 分片: {}", split);
    }

    @Override
    public void close() throws IOException {
        log.info("MqttSplitEnumerator 关闭");
    }
}
```

### Step 3.4: 编译 mqtt 模块

- [ ] 执行：

```bash
mvn -pl flink-etl-connector/connector-mqtt -am compile
```

预期：`BUILD SUCCESS`。

### Step 3.5: 提交

- [ ] 执行：

```bash
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttEnumCheckpoint.java
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSource.java
git add flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/MqttSplitEnumerator.java
git commit -m "refactor(mqtt): 删除 MqttEnumCheckpoint 并改用 BaseEnumCheckpoint"
```

---

## Task 4: http 连接器收敛

**Files:**

- Delete: `flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpEnumCheckpoint.java`
- Modify: `flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpSource.java`
- Modify: `flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpSplitEnumerator.java`

### Step 4.1: 删除 HttpEnumCheckpoint

- [ ] 执行：

```bash
rm flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpEnumCheckpoint.java
```

### Step 4.2: 改写 HttpSource

- [ ] 将 `flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpSource.java` 完整替换为：

```java
package com.etl.connector.http.source;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * HTTP Source 实现
 * 支持 GET/POST 请求获取 JSON 数据
 */
@Slf4j
public class HttpSource extends AbstractSplitSource<HttpSplit> {

    private final HttpSourceConfig httpSourceConfig;

    public HttpSource(SourceConfig config) {
        super(config);
        httpSourceConfig = HttpSourceConfig.fromSourceConfig(config);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<HttpSplit, BaseEnumCheckpoint<HttpSplit>> createEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new HttpSplitEnumerator(enumContext, httpSourceConfig);
    }

    @Override
    public SplitEnumerator<HttpSplit, BaseEnumCheckpoint<HttpSplit>> restoreEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext,
            BaseEnumCheckpoint<HttpSplit> checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new HttpSplitEnumerator(enumContext, checkpoint, httpSourceConfig);
    }

    @Override
    public SourceReader<Row, HttpSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<AbstractSplitReader<Row, HttpSplit>> splitReaderSupplier = HttpSplitReader::new;
        return new HttpSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<HttpSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<HttpSplit>> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
```

### Step 4.3: 改写 HttpSplitEnumerator（删除 snapshotState）

- [ ] 将 `flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpSplitEnumerator.java`
  完整替换为：

```java
package com.etl.connector.http.source;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.Collections;

/**
 * HTTP 分片枚举器
 * 单分片模式，创建一个包含完整配置的分片
 */
@Slf4j
public class HttpSplitEnumerator extends AbstractSplitEnumerator<HttpSplit> {

    private final HttpSourceConfig httpSourceConfig;

    public HttpSplitEnumerator(
            SplitEnumeratorContext<HttpSplit> context,
            HttpSourceConfig httpSourceConfig) {
        super(context);
        this.httpSourceConfig = httpSourceConfig;
    }

    public HttpSplitEnumerator(
            SplitEnumeratorContext<HttpSplit> context,
            BaseEnumCheckpoint<HttpSplit> checkpoint,
            HttpSourceConfig httpSourceConfig) {
        super(context, checkpoint);
        this.httpSourceConfig = httpSourceConfig;
    }

    @Override
    public void start() {
        log.info("HttpSplitEnumerator 启动，URL: {}", httpSourceConfig.getUrl());

        HttpSplit split = new HttpSplit("http-split-0", httpSourceConfig);
        addPendingSplits(Collections.singletonList(split));
        log.info("创建 HTTP 分片: {}", split);
    }

    @Override
    public void close() throws IOException {
        log.info("HttpSplitEnumerator 关闭");
    }
}
```

### Step 4.4: 编译 http 模块

- [ ] 执行：

```bash
mvn -pl flink-etl-connector/connector-http -am compile
```

预期：`BUILD SUCCESS`。

### Step 4.5: 提交

- [ ] 执行：

```bash
git add flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpEnumCheckpoint.java
git add flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpSource.java
git add flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/HttpSplitEnumerator.java
git commit -m "refactor(http): 删除 HttpEnumCheckpoint 并改用 BaseEnumCheckpoint"
```

---

## Task 5: localfile 连接器收敛

**Files:**

- Delete:
  `flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileEnumCheckpoint.java`
- Modify:
  `flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileSource.java`
- Modify:
  `flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileSplitEnumerator.java`

### Step 5.1: 删除 LocalFileEnumCheckpoint

- [ ] 执行：

```bash
rm flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileEnumCheckpoint.java
```

### Step 5.2: 改写 LocalFileSource

- [ ] 将 `flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileSource.java`
  完整替换为：

```java
package com.etl.connector.localfile.source;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * 本地文件 Source 实现
 * 支持通配符匹配文件，每个文件对应一个分片
 *
 * <p>使用组件：
 * <ul>
 *   <li>{@link LocalFileSplitEnumerator} - 分片枚举器</li>
 *   <li>{@link LocalFileSourceReader} - 源阅读器</li>
 *   <li>默认序列化器 - 使用 JDK 原生序列化</li>
 *   <li>直接输出 Flink Row 类型</li>
 * </ul>
 *
 * <p>字段名和类型从 source.schema 配置中获取
 */
@Slf4j
public class LocalFileSource extends AbstractSplitSource<LocalFileSplit> {

    private final LocalFileSourceConfig localFileSourceConfig;

    public LocalFileSource(SourceConfig config) {
        super(config);
        this.localFileSourceConfig = LocalFileSourceConfig.fromSourceConfig(config, super.getDefaultBatchSize());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<LocalFileSplit, BaseEnumCheckpoint<LocalFileSplit>> createEnumerator(
            SplitEnumeratorContext<LocalFileSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, localFileSourceConfig);
    }

    @Override
    public SplitEnumerator<LocalFileSplit, BaseEnumCheckpoint<LocalFileSplit>> restoreEnumerator(
            SplitEnumeratorContext<LocalFileSplit> enumContext,
            BaseEnumCheckpoint<LocalFileSplit> checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new LocalFileSplitEnumerator(enumContext, checkpoint, localFileSourceConfig);
    }

    @Override
    public SourceReader<Row, LocalFileSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<AbstractSplitReader<Row, LocalFileSplit>> splitReaderSupplier = LocalFileSplitReader::new;
        return new LocalFileSourceReader(
                splitReaderSupplier,
                readerContext
        );
    }

    @Override
    public SimpleVersionedSerializer<LocalFileSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<LocalFileSplit>> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
```

### Step 5.3: 改写 LocalFileSplitEnumerator（删除 snapshotState，保留所有 scanFiles 等私有方法）

- [ ] 将
  `flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileSplitEnumerator.java`
  完整替换为：

```java
package com.etl.connector.localfile.source;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import com.etl.core.exception.SourceConfigException;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * 本地文件分片枚举器
 * 继承 AbstractSplitEnumerator，自动处理分片分配逻辑
 *
 * <p>职责：
 * <ul>
 *   <li>扫描文件系统，匹配通配符路径</li>
 *   <li>创建文件分片并分配给 Reader</li>
 * </ul>
 *
 * <p>注意：字段名和类型从 source.schema 配置中获取，无需从文件推断
 */
@Slf4j
public class LocalFileSplitEnumerator extends AbstractSplitEnumerator<LocalFileSplit> {

    private final LocalFileSourceConfig localFileSourceConfig;

    public LocalFileSplitEnumerator(
            SplitEnumeratorContext<LocalFileSplit> context,
            LocalFileSourceConfig localFileSourceConfig) {
        super(context);
        this.localFileSourceConfig = localFileSourceConfig;
    }

    public LocalFileSplitEnumerator(
            SplitEnumeratorContext<LocalFileSplit> context,
            BaseEnumCheckpoint<LocalFileSplit> checkpoint,
            LocalFileSourceConfig localFileSourceConfig) {
        super(context, checkpoint);
        this.localFileSourceConfig = localFileSourceConfig;
    }

    /**
     * 查找路径中第一个通配符（*）的位置
     * 支持 * 和 ** 通配符，不支持 ? 通配符
     */
    private static int findWildcardIndex(String pathPattern) {
        return pathPattern.indexOf('*');
    }

    @Override
    public void start() {
        String pathPattern = localFileSourceConfig.getPathPattern();
        boolean recursive = localFileSourceConfig.isRecursive();
        log.info("LocalFileSplitEnumerator 启动，路径模式: {}，是否递归: {}", pathPattern, recursive);

        List<File> matchedFiles = scanFiles(pathPattern, recursive);
        if (matchedFiles.isEmpty()) {
            throw new SourceConfigException("未找到匹配的文件: " + pathPattern);
        }

        log.info("找到 {} 个匹配的文件", matchedFiles.size());

        List<LocalFileSplit> splits = new ArrayList<>();
        for (File file : matchedFiles) {
            splits.add(new LocalFileSplit(file.getAbsolutePath(), localFileSourceConfig));
        }

        addPendingSplits(splits);
        log.info("创建了 {} 个文件分片", splits.size());
    }

    private List<File> scanFiles(String pathPattern, boolean recursive) {
        List<File> result = new ArrayList<>();

        Path basePath = getBasePath(pathPattern);
        String globPattern = getGlobPattern(pathPattern);

        log.debug("基础路径: {}, glob 模式: {}", basePath, globPattern);

        if (basePath == null) {
            throw new SourceConfigException("无法确定文件扫描的基础路径: " + pathPattern);
        }

        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);

        try {
            if (recursive) {
                try (Stream<Path> pathStream = Files.walk(basePath)) {
                    pathStream.filter(Files::isRegularFile)
                            .filter(path -> matcher.matches(basePath.relativize(path)))
                            .forEach(path -> result.add(path.toFile()));
                }
            } else {
                try (Stream<Path> pathStream = Files.list(basePath)) {
                    pathStream.filter(Files::isRegularFile)
                            .filter(path -> matcher.matches(basePath.relativize(path)))
                            .forEach(path -> result.add(path.toFile()));
                }
            }
        } catch (IOException e) {
            throw new SourceConfigException("扫描文件失败: " + e.getMessage(), e);
        }

        return result;
    }

    private Path getBasePath(String pathPattern) {
        int wildcardIndex = findWildcardIndex(pathPattern);

        if (wildcardIndex == -1) {
            Path parent = Paths.get(pathPattern).getParent();
            return parent != null ? parent : Paths.get(".");
        }

        String basePath = pathPattern.substring(0, wildcardIndex);
        Path path = Paths.get(basePath);

        if (basePath.endsWith("/") || basePath.endsWith("\\")) {
            return path;
        }

        return path.getParent() != null ? path.getParent() : Paths.get(".");
    }

    private String getGlobPattern(String pathPattern) {
        int wildcardIndex = findWildcardIndex(pathPattern);

        if (wildcardIndex == -1) {
            return Paths.get(pathPattern).getFileName().toString();
        }

        int lastSeparatorIndex = pathPattern.lastIndexOf('/', wildcardIndex);
        if (lastSeparatorIndex == -1) {
            lastSeparatorIndex = pathPattern.lastIndexOf('\\', wildcardIndex);
        }

        return pathPattern.substring(lastSeparatorIndex + 1);
    }

    @Override
    public void close() throws IOException {
        log.info("LocalFileSplitEnumerator 关闭");
    }
}
```

### Step 5.4: 编译 localfile 模块

- [ ] 执行：

```bash
mvn -pl flink-etl-connector/connector-localfile -am compile
```

预期：`BUILD SUCCESS`。

### Step 5.5: 提交

- [ ] 执行：

```bash
git add flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileEnumCheckpoint.java
git add flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileSource.java
git add flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/LocalFileSplitEnumerator.java
git commit -m "refactor(localfile): 删除 LocalFileEnumCheckpoint 并改用 BaseEnumCheckpoint"
```

---

## Task 6: mock 连接器收敛

**Files:**

- Delete: `flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockEnumCheckpoint.java`
- Modify: `flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockSource.java`
- Modify: `flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockSplitEnumerator.java`

### Step 6.1: 删除 MockEnumCheckpoint

- [ ] 执行：

```bash
rm flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockEnumCheckpoint.java
```

### Step 6.2: 改写 MockSource

- [ ] 将 `flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockSource.java` 完整替换为：

```java
package com.etl.connector.mock.source;

import com.etl.connector.mock.source.config.MockSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Mock Source 主类
 * <p>
 * 始终以 CONTINUOUS_UNBOUNDED 模式运行，行为由用户配置决定：
 * <ul>
 *   <li>配置了 data 或 numRows：数据读取完毕后程序自然停止</li>
 *   <li>未配置 data 和 numRows：按 intervalMs（默认 1000ms）持续生成数据</li>
 * </ul>
 */
@Slf4j
public class MockSource extends AbstractSplitSource<MockSplit> {

    private final MockSourceConfig mockConfig;
    private final boolean bounded;

    public MockSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        super(config);
        this.mockConfig = MockSourceConfig.fromSourceConfig(config, runtimeMode);
        this.bounded = runtimeMode == RuntimeExecutionMode.BATCH;
    }

    @Override
    public Boundedness getBoundedness() {
        if (bounded) {
            return Boundedness.BOUNDED;
        }
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MockSplit, BaseEnumCheckpoint<MockSplit>> createEnumerator(
            SplitEnumeratorContext<MockSplit> enumContext) {
        log.info("创建 MockSplitEnumerator");
        return new MockSplitEnumerator(enumContext, mockConfig);
    }

    @Override
    public SplitEnumerator<MockSplit, BaseEnumCheckpoint<MockSplit>> restoreEnumerator(
            SplitEnumeratorContext<MockSplit> enumContext,
            BaseEnumCheckpoint<MockSplit> checkpoint) {
        log.info("从检查点恢复 MockSplitEnumerator");
        return new MockSplitEnumerator(enumContext, checkpoint, mockConfig);
    }

    @Override
    public SourceReader<Row, MockSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 MockSourceReader");

        Supplier<AbstractSplitReader<Row, MockSplit>> splitReaderSupplier = MockSplitReader::new;

        return new MockSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<MockSplit>> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
```

### Step 6.3: 改写 MockSplitEnumerator（删除 snapshotState）

- [ ] 将 `flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockSplitEnumerator.java`
  完整替换为：

```java
package com.etl.connector.mock.source;

import com.etl.connector.mock.source.config.MockSourceConfig;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

/**
 * Mock Source 分片枚举器
 * 在 start() 时创建单个 MockSplit 并分配到队列
 */
@Slf4j
public class MockSplitEnumerator extends AbstractSplitEnumerator<MockSplit> {

    private final MockSourceConfig mockConfig;

    public MockSplitEnumerator(
            SplitEnumeratorContext<MockSplit> context,
            MockSourceConfig mockConfig) {
        super(context);
        this.mockConfig = mockConfig;
    }

    public MockSplitEnumerator(
            SplitEnumeratorContext<MockSplit> context,
            BaseEnumCheckpoint<MockSplit> checkpoint,
            MockSourceConfig mockConfig) {
        super(context, checkpoint);
        this.mockConfig = mockConfig;
    }

    @Override
    public void start() {
        MockSplit split = new MockSplit(mockConfig);
        pendingSplits.add(split);
        log.info("Mock Source 创建单分片: {}", split.splitId());
    }

    @Override
    public void close() {
        log.info("MockSplitEnumerator 关闭");
    }
}
```

### Step 6.4: 编译 mock 模块

- [ ] 执行：

```bash
mvn -pl flink-etl-connector/connector-mock -am compile
```

预期：`BUILD SUCCESS`。

### Step 6.5: 提交

- [ ] 执行：

```bash
git add flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockEnumCheckpoint.java
git add flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockSource.java
git add flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/MockSplitEnumerator.java
git commit -m "refactor(mock): 删除 MockEnumCheckpoint 并改用 BaseEnumCheckpoint"
```

---

## Task 7: modbus 连接器收敛

**Files:**

- Delete: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusEnumCheckpoint.java`
- Modify: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSource.java`
- Modify:
  `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitEnumerator.java`

### Step 7.1: 删除 ModbusEnumCheckpoint

- [ ] 执行：

```bash
rm flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusEnumCheckpoint.java
```

### Step 7.2: 改写 ModbusSource

- [ ] 将 `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSource.java` 完整替换为：

```java
package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Modbus Source 主类
 * <p>
 * 通过 Modbus TCP 协议读取 Holding Registers。
 * 输出固定 Schema: Row(address: INT, value: INT)
 */
@Slf4j
public class ModbusSource extends AbstractSplitSource<ModbusSplit> {

    private final ModbusSourceConfig modbusConfig;
    private final boolean bounded;

    public ModbusSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        super(config);
        this.modbusConfig = ModbusSourceConfig.fromSourceConfig(config, runtimeMode);
        this.bounded = runtimeMode == RuntimeExecutionMode.BATCH;
    }

    @Override
    public Boundedness getBoundedness() {
        if (bounded) {
            return Boundedness.BOUNDED;
        }
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(
                new String[]{"address", "value"},
                Types.INT, Types.INT
        );
    }

    @Override
    public SplitEnumerator<ModbusSplit, BaseEnumCheckpoint<ModbusSplit>> createEnumerator(
            SplitEnumeratorContext<ModbusSplit> enumContext) {
        log.info("创建 ModbusSplitEnumerator");
        return new ModbusSplitEnumerator(enumContext, modbusConfig);
    }

    @Override
    public SplitEnumerator<ModbusSplit, BaseEnumCheckpoint<ModbusSplit>> restoreEnumerator(
            SplitEnumeratorContext<ModbusSplit> enumContext,
            BaseEnumCheckpoint<ModbusSplit> checkpoint) {
        log.info("从检查点恢复 ModbusSplitEnumerator");
        return new ModbusSplitEnumerator(enumContext, checkpoint, modbusConfig);
    }

    @Override
    public SourceReader<Row, ModbusSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 ModbusSourceReader");
        Supplier<AbstractSplitReader<Row, ModbusSplit>> splitReaderSupplier = ModbusSplitReader::new;
        return new ModbusSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<ModbusSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<ModbusSplit>> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
```

### Step 7.3: 改写 ModbusSplitEnumerator（删除 snapshotState）

- [ ] 将 `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitEnumerator.java`
  完整替换为：

```java
package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

/**
 * Modbus Source 分片枚举器
 * 在 start() 时创建单个 ModbusSplit 并分配到队列
 */
@Slf4j
public class ModbusSplitEnumerator extends AbstractSplitEnumerator<ModbusSplit> {

    private final ModbusSourceConfig modbusConfig;

    public ModbusSplitEnumerator(
            SplitEnumeratorContext<ModbusSplit> context,
            ModbusSourceConfig modbusConfig) {
        super(context);
        this.modbusConfig = modbusConfig;
    }

    public ModbusSplitEnumerator(
            SplitEnumeratorContext<ModbusSplit> context,
            BaseEnumCheckpoint<ModbusSplit> checkpoint,
            ModbusSourceConfig modbusConfig) {
        super(context, checkpoint);
        this.modbusConfig = modbusConfig;
    }

    @Override
    public void start() {
        ModbusSplit split = new ModbusSplit(modbusConfig);
        pendingSplits.add(split);
        log.info("Modbus Source 创建单分片: {}", split.splitId());
    }

    @Override
    public void close() {
        log.info("ModbusSplitEnumerator 关闭");
    }
}
```

### Step 7.4: 编译 modbus 模块

- [ ] 执行：

```bash
mvn -pl flink-etl-connector/connector-modbus -am compile
```

预期：`BUILD SUCCESS`。

### Step 7.5: 提交

- [ ] 执行：

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusEnumCheckpoint.java
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSource.java
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitEnumerator.java
git commit -m "refactor(modbus): 删除 ModbusEnumCheckpoint 并改用 BaseEnumCheckpoint"
```

---

## Task 8: 全量构建 + 测试验证

### Step 8.1: 全量编译

- [ ] 在项目根目录执行：

```bash
mvn clean compile
```

预期：`BUILD SUCCESS`，所有模块全部编译通过。
**若失败**：检查报错信息，确认是否有遗漏的 `XxxEnumCheckpoint` 引用没改到。

### Step 8.2: 残留引用扫描

- [ ] 执行（使用 Grep 工具）：

搜索模式：`EnumCheckpoint`，glob：`**/*.java`

预期匹配仅出现在以下文件中：

- `flink-etl-core/src/main/java/com/etl/core/source/BaseEnumCheckpoint.java`
- `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitEnumerator.java`
- `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java`
- `flink-etl-core/src/main/java/com/etl/core/source/serde/DefaultCheckpointSerializer.java`
- 6 个连接器各自的 `XxxSource.java`、`XxxSplitEnumerator.java`（其中匹配的应该都是 `BaseEnumCheckpoint`）

**若有任何 `XxxEnumCheckpoint`（带连接器前缀）残留**：说明某个文件没改到，回到对应 Task 修复后重新执行 Step 8.1。

### Step 8.3: 全量测试

- [ ] 执行：

```bash
mvn test
```

预期：`BUILD SUCCESS`，所有现有测试通过。

**若有测试失败**：

- 重构未引入新行为，理论上不应失败
- 若失败，仔细比对失败测试关注的逻辑是否在重构中被破坏（例如 snapshotState 行为改变）
- 修复后重新执行

### Step 8.4: 最终打包验证

- [ ] 执行：

```bash
mvn clean package -DskipTests
```

预期：`BUILD SUCCESS`，`flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar` 生成。

### Step 8.5: 验证完成（不再额外提交）

Task 8 是验证步骤，不产生代码变更。不需要 commit。

如果验证全部通过，至此重构完成，共产生 7 次 commit：

- `refactor(core): ...`
- `refactor(jdbc): ...` × 1
- `refactor(mqtt): ...` × 1
- `refactor(http): ...` × 1
- `refactor(localfile): ...` × 1
- `refactor(mock): ...` × 1
- `refactor(modbus): ...` × 1

---

## 完工检查清单

- [ ] `mvn clean compile` 通过
- [ ] `mvn test` 通过
- [ ] `mvn clean package -DskipTests` 通过
- [ ] Grep `XxxEnumCheckpoint`（带前缀）在 `src/main/java` 中无任何匹配
- [ ] 6 个 `XxxEnumCheckpoint.java` 已删除
- [ ] 7 次 commit 形成清晰的提交链
