# BaseEnumCheckpoint 重构设计

> **状态**：已批准
> **日期**：2026-05-27
> **范围**：`flink-etl-core` + 全部 6 个 Source 连接器（jdbc/mqtt/http/localfile/mock/modbus）

## 背景

当前框架为每个 Source 都要求一个具体的 `XxxEnumCheckpoint` 类用于保存 SplitEnumerator 状态。检视现状：

| 类 | 实际内容 |
|----|---------|
| `JdbcEnumCheckpoint` / `MqttEnumCheckpoint` / `HttpEnumCheckpoint` / `LocalFileEnumCheckpoint` / `MockEnumCheckpoint` / `ModbusEnumCheckpoint` | 仅一个 `serialVersionUID` + 转发给 `super(pendingSplits)` 的构造函数；**没有任何独有字段或方法** |

并且 6 个 `XxxSplitEnumerator.snapshotState(long)` 实现也完全同构：

```java
List<XxxSplit> pending = new ArrayList<>(pendingSplits);
log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());  // Modbus/Mock 漏打
return new XxxEnumCheckpoint(pending);
```

这 6 个子类只是"为了给类起一个具体名字"而存在，是典型的可消除噪音。

## 目标

1. 消除 6 个空壳 `XxxEnumCheckpoint` 子类。
2. 把 `snapshotState(long)` 下沉到 `AbstractSplitEnumerator`，6 个子类不再重复实现。
3. 简化 `AbstractSplitSource` / `AbstractSplitEnumerator` 的泛型签名，去掉冗余的 `CheckpointT` 类型参数。
4. 保证现有 checkpoint/restore 行为不变（at-least-once、pendingSplits 在故障恢复时回填）。

## 非目标

- 不改变 Source 的对外配置或行为
- 不改变序列化协议（仍是 JDK 序列化 + `serialVersionUID = 1L`）
- 不为 Checkpoint 添加新字段（如 offset 等）—— YAGNI，等真有连接器需要时再扩展

## 设计

### 1. `AbstractEnumCheckpoint` → `BaseEnumCheckpoint`

原 `abstract class AbstractEnumCheckpoint` 没有任何抽象方法，仅是为了"不可直接实例化"。重命名为 `BaseEnumCheckpoint`，去掉 `abstract` 修饰，使其成为可实例化的具体类。

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

### 2. `AbstractSplitEnumerator` 消除 CheckpointT 泛型，下沉 snapshotState

```java
public abstract class AbstractSplitEnumerator<SplitT extends BaseSourceSplit>
        implements SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>> {

    protected final Queue<SplitT> pendingSplits = new ConcurrentLinkedQueue<>();
    protected final SplitEnumeratorContext<SplitT> context;

    public AbstractSplitEnumerator(SplitEnumeratorContext<SplitT> context) { ... }

    public AbstractSplitEnumerator(SplitEnumeratorContext<SplitT> context,
                                   BaseEnumCheckpoint<SplitT> checkpoint) { ... }

    // ↓ 下沉：所有 Source 通用
    @Override
    public BaseEnumCheckpoint<SplitT> snapshotState(long checkpointId) {
        List<SplitT> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new BaseEnumCheckpoint<>(pending);
    }

    // handleSplitRequest / addSplitsBack / addReader 保持不变
}
```

**注意**：原本 Modbus / Mock 的 `snapshotState` 没有打日志，下沉后**统一打 info 级别**。这是有意为之 —— checkpoint 是低频事件，每次产生一行日志便于运维排查，符合其余 4 个连接器原有惯例。

### 3. `AbstractSplitSource` 消除 CheckpointT 泛型

```java
public abstract class AbstractSplitSource<SplitT extends BaseSourceSplit>
        implements Source<Row, SplitT, BaseEnumCheckpoint<SplitT>>, ResultTypeQueryable<Row> {

    // 构造、getProducedType 等保持不变

    @Override
    public abstract SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>>
            createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>>
            restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext,
                              BaseEnumCheckpoint<SplitT> checkpoint);

    @Override
    public abstract SimpleVersionedSerializer<BaseEnumCheckpoint<SplitT>>
            getEnumeratorCheckpointSerializer();
    // ...
}
```

**关键约束**：`SplitT` 的上界由原来的 `SourceSplit` 收紧为 `BaseSourceSplit`，与 `BaseEnumCheckpoint<SplitT>` 的约束对齐。检查现状，6 个 Source 的 SplitT 均已 extends `BaseSourceSplit`，无破坏性变更。

### 4. `DefaultCheckpointSerializer` 简化

```java
public class DefaultCheckpointSerializer<SplitT extends BaseSourceSplit>
        implements SimpleVersionedSerializer<BaseEnumCheckpoint<SplitT>> {
    // 实现内部不变（仍走 JDK 序列化）
}
```

去掉冗余的 `CheckpointT` 泛型参数。

### 5. 6 个连接器同步收敛

每个连接器需做的变更（以 Mock 为例）：

| 文件 | 变更 |
|------|------|
| `MockEnumCheckpoint.java` | **删除** |
| `MockSource.java` | `extends AbstractSplitSource<MockSplit, MockEnumCheckpoint>` → `extends AbstractSplitSource<MockSplit>`；签名中 `MockEnumCheckpoint` 全部替换为 `BaseEnumCheckpoint<MockSplit>`；`DefaultCheckpointSerializer<>` 泛型参数变为 `<MockSplit>` |
| `MockSplitEnumerator.java` | `extends AbstractSplitEnumerator<MockSplit, MockEnumCheckpoint>` → `extends AbstractSplitEnumerator<MockSplit>`；构造函数参数 `MockEnumCheckpoint` → `BaseEnumCheckpoint<MockSplit>`；**删除整个 `snapshotState` 方法** |

其余 5 个连接器（Jdbc / Mqtt / Http / LocalFile / Modbus）做同构变更。

## 数据流 / 兼容性

### Checkpoint 兼容性
**不兼容旧 checkpoint 数据。** 序列化类名从 `com.etl.connector.xxx.source.XxxEnumCheckpoint` 变为 `com.etl.core.source.BaseEnumCheckpoint`，JDK 反序列化按全限定类名匹配，旧 checkpoint 反序列化会抛 `ClassNotFoundException`。

由于本项目为内部工具、当前阶段没有需要保留的线上 checkpoint，**不提供迁移路径**：升级时需要从干净状态启动 Job。

### 运行时行为
- `snapshotState` 内部逻辑不变（snapshot pendingSplits 集合）
- 构造函数恢复逻辑不变（`pendingSplits.addAll(checkpoint.getPendingSplits())`）
- 日志输出统一为 info 级别（Modbus / Mock 由静默变为打日志，这是预期改进）

## 影响面清单

```
flink-etl-core/
  src/main/java/com/etl/core/source/
    AbstractEnumCheckpoint.java          删除（重命名）
    BaseEnumCheckpoint.java              新增（由 Abstract 改造而来）
    AbstractSplitEnumerator.java         修改：去 CheckpointT 泛型 + 下沉 snapshotState
    AbstractSplitSource.java             修改：去 CheckpointT 泛型，SplitT 上界收紧
    serde/DefaultCheckpointSerializer.java  修改:去 CheckpointT 泛型

flink-etl-connector/connector-jdbc/      删除 JdbcEnumCheckpoint;改 JdbcSource + JdbcSplitEnumerator
flink-etl-connector/connector-mqtt/      删除 MqttEnumCheckpoint;改 MqttSource + MqttSplitEnumerator
flink-etl-connector/connector-http/      删除 HttpEnumCheckpoint;改 HttpSource + HttpSplitEnumerator
flink-etl-connector/connector-localfile/ 删除 LocalFileEnumCheckpoint;改 LocalFileSource + LocalFileSplitEnumerator
flink-etl-connector/connector-mock/      删除 MockEnumCheckpoint;改 MockSource + MockSplitEnumerator
flink-etl-connector/connector-modbus/    删除 ModbusEnumCheckpoint;改 ModbusSource + ModbusSplitEnumerator
```

**不需要改的**：
- `BaseSourceSplit`、`AbstractSplitReader`、`AbstractSourceReader`、`BaseRecordEmitter` 等 Source 抽象层其他类
- 所有 Sink / Transform / UDF 相关代码
- 配置类、连接器配置 json、`PLUGINS.md`

## 测试策略

- **编译**：`mvn clean compile`，6 个连接器 + core 全部通过
- **现有单元测试**：`mvn test`，关注 `flink-etl-core` 和各连接器内部对 Checkpoint/Enumerator 的单测，预期全部通过
- 不需新增测试：本次为纯重构，无新行为；snapshotState 行为由原有连接器测试覆盖

## 风险与缓解

| 风险 | 缓解 |
|------|------|
| Checkpoint 序列化不兼容 | 已在"兼容性"小节明确：从干净状态启动；不提供迁移 |
| 某个连接器对 Checkpoint 实际有特殊扩展（漏看） | 已逐个 Read 6 个 `XxxEnumCheckpoint`，确认均为空壳 |
| `AbstractSplitSource` 收紧 SplitT 上界破坏现有 Source | 已 grep 验证 6 个 Source 的 SplitT 均 extends BaseSourceSplit |
| Modbus / Mock 增加 checkpoint 日志带来噪音 | checkpoint 是低频事件，info 日志可接受；统一行为符合架构期望 |

## 后续可能的扩展

如未来某连接器（如 Kafka Source）确实需要在 Checkpoint 里携带额外状态（如 offset），可以：

- 仍让该连接器继承 `BaseEnumCheckpoint<XxxSplit>` 并添加字段；
- 或为 `AbstractSplitEnumerator` 重新引入 `CheckpointT` 泛型，但作为可选扩展点（默认仍走 `BaseEnumCheckpoint`）。

当前 6 个连接器均不需要，不预先设计。
