# BaseSplitReader 重构为 AbstractSplitReader

## 背景

`com.etl.core.source.BaseSplitReader` 当前是一个接口，有 7 个实现（JDBC、LocalFile、Modbus、Mock、HTTP、MQTT 等）。
其中 6 个实现都存在相同的字段声明和样板代码：

```java
private final Queue<XxxSplit> pendingSplits = new ArrayDeque<>();
private final Set<String> finishedSplits = new HashSet<>();

@Override
public void handleSplitsChanges(SplitsChange<XxxSplit> splitsChanges) {
    pendingSplits.addAll(splitsChanges.splits());
    log.debug("接收到 {} 个新分片", splitsChanges.splits().size());
}
```

## 改动方案

1. 将 `BaseSplitReader` 重命名为 `AbstractSplitReader`，由 `interface` 改为 `abstract class`。
2. 下沉 `pendingSplits` 字段到父类，声明为 `protected`：
   ```java
   protected final Queue<SplitT> pendingSplits = new ArrayDeque<>();
   ```
3. 下沉 `handleSplitsChanges` 默认实现到父类。
4. `finishedSplits` **不下沉**：MQTT 这类流式 Reader 不需要该字段，强行下沉是错误的抽象。
5. 保留 `wakeUp()`、`close()` 的默认空实现（原接口 default 方法 → 抽象类普通方法）。
6. `fetch()` 保持抽象，子类实现各自的读取逻辑。

## 命名理由

与项目已有重构保持一致：`BaseSplitState → AbstractSplitState`、`BaseSourceReader → AbstractSourceReader`、
`BaseSplitEnumerator → AbstractSplitEnumerator`、`BaseEnumCheckpoint → AbstractEnumCheckpoint`。

## 影响范围

- `flink-etl-core`：`BaseSplitReader.java` → `AbstractSplitReader.java`
- 7 个 connector 模块的 `XxxSplitReader.java`：
  - 改 `implements BaseSplitReader<...>` 为 `extends AbstractSplitReader<...>`
  - 删除自身的 `pendingSplits` 字段声明和相关 import
  - 删除自身的 `handleSplitsChanges` 方法（MQTT 因实现不同，保留自身实现并加 `@Override`）

## 范围外

- 不动 `fetch()` 模板方法
- 不抽 `AbstractBoundedSplitReader` 中间类（YAGNI）
- 不提供 `pollNextSplit()` 等工具方法（暴露字段即可）
