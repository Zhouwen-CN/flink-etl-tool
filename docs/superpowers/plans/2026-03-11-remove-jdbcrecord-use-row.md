# 移除 JdbcRecord 优化计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 删除 `JdbcRecord` 包装类，让 JDBC Source 直接使用 Flink 的 `Row` 类型作为输出

**Architecture:** 当前 `JdbcRecord` 是一个不必要的中间包装层，包含 `Row` 和 `splitId`。但 `splitId` 在下游处理中未被使用。移除这个包装层可以：
1. 减少对象创建开销
2. 简化类型系统
3. 与 Flink 生态更好集成

**Tech Stack:** Java 11, Apache Flink 1.19.0

---

## 变更文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `JdbcRecord.java` | 删除 | 不再需要包装类 |
| `JdbcRecordEmitter.java` | 修改 | 改为 `RowRecordEmitter`，直接发射 Row |
| `JdbcSplitReader.java` | 修改 | 直接返回 Row 而非 JdbcRecord |
| `JdbcSourceReader.java` | 修改 | 类型参数从 JdbcRecord 改为 Row |
| `JdbcSource.java` | 修改 | 输出类型从 JdbcRecord 改为 Row |

---

## Task 1: 修改 JdbcRecordEmitter 为 RowRecordEmitter

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcRecordEmitter.java`

- [ ] **Step 1: 重命名文件并修改内容**

将 `JdbcRecordEmitter.java` 重命名为 `RowRecordEmitter.java`，并修改为直接发射 Row：

```java
package com.etl.source.jdbc;

import com.etl.source.jdbc.RangeSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Row 记录发射器
 * 将 Row 直接发射到下游
 *
 * <p>支持：
 * <ul>
 *   <li>事件时间戳提取（如果 Row 中包含时间字段）</li>
 *   <li>读取统计信息更新</li>
 * </ul>
 */
public class RowRecordEmitter implements RecordEmitter<Row, Row, RangeSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(RowRecordEmitter.class);

    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, RangeSplitState splitState) throws Exception {
        // 发射记录到下游
        output.collect(record);

        // 更新状态
        splitState.addRecordsRead(1);
    }
}
```

- [ ] **Step 2: 删除旧的 JdbcRecordEmitter.java 文件**

```bash
rm flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcRecordEmitter.java
```

---

## Task 2: 修改 JdbcSplitReader 直接返回 Row

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitReader.java`

- [ ] **Step 1: 修改类型参数和 import**

修改类声明，将 `JdbcRecord` 改为 `Row`：

```java
package com.etl.source.jdbc;

import com.etl.source.jdbc.RangeSplit;
import com.etl.core.source.BaseSplitReader;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

/**
 * JDBC 分片读取器
 * 实现阻塞式数据读取，配合 BaseSourceReader 使用
 *
 * <p>设计说明：
 * <ul>
 *   <li>每个分片创建独立的数据库连接</li>
 *   <li>使用 fetch() 方法一次性读取一个分片的所有数据</li>
 *   <li>支持流式读取（通过 fetchSize 控制）</li>
 *   <li>直接返回 Flink Row 类型，无需额外包装</li>
 * </ul>
 */
public class JdbcSplitReader implements BaseSplitReader<Row, RangeSplit> {
    // ... 其余代码保持不变，只修改涉及 JdbcRecord 的部分
}
```

- [ ] **Step 2: 修改 fetch() 方法**

修改 `fetch()` 方法中的 `RecordsBySplits.Builder` 类型：

```java
@Override
public RecordsWithSplitIds<Row> fetch() throws IOException {
    RangeSplit split = pendingSplits.poll();

    if (split == null) {
        // 没有待处理的分片，返回空结果
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
        builder.addFinishedSplits(finishedSplits);
        return builder.build();
    }

    logger.info("开始读取分片: {}", split.splitId());

    try {
        return fetchDataForSplit(split);
    } catch (SQLException e) {
        throw new IOException("读取分片失败: " + split.splitId(), e);
    }
}
```

- [ ] **Step 3: 修改 fetchDataForSplit() 方法**

移除 `JdbcRecord` 包装，直接添加 `Row`：

```java
/**
 * 读取单个分片的数据
 */
private RecordsWithSplitIds<Row> fetchDataForSplit(RangeSplit split) throws SQLException {
    RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

    try (Connection conn = DriverManager.getConnection(url, username, password);
         Statement stmt = conn.createStatement()) {

        // 设置 fetchSize 实现流式读取
        if (fetchSize != null) {
            stmt.setFetchSize(fetchSize);
        }
        if (queryTimeout != null) {
            stmt.setQueryTimeout(queryTimeout);
        }

        // 构建分片查询 SQL
        String querySql = dialect.buildSplitQuery(table, sql, splitColumn,
                split.getStart(), split.getEnd());
        logger.debug("执行查询: {}", querySql);

        try (ResultSet rs = stmt.executeQuery(querySql)) {
            while (rs.next()) {
                Row row = dialect.createRow(rs);
                builder.add(split.splitId(), row);
            }
        }
    }

    // 标记分片完成
    finishedSplits.add(split.splitId());
    logger.info("分片 {} 读取完成", split.splitId());

    return builder.build();
}
```

---

## Task 3: 修改 JdbcSourceReader 类型参数

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSourceReader.java`

- [ ] **Step 1: 修改类型参数和 import**

将所有 `JdbcRecord` 替换为 `Row`：

```java
package com.etl.source.jdbc;

import com.etl.source.jdbc.RangeSplit;
import com.etl.source.jdbc.RangeSplitState;
import com.etl.core.source.BaseSourceReader;
import com.etl.core.source.BaseSplitReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Supplier;

/**
 * JDBC Source Reader
 * 继承 BaseSourceReader，自动处理线程模型和状态管理
 *
 * <p>优化后代码行数：~50 行（优化前：~160 行）
 * <p>消除的重复代码：线程管理、状态追踪、pollNext 逻辑
 * <p>直接输出 Flink Row 类型，无需额外包装
 *
 * <p>子类需要实现的方法：
 * <ul>
 *   <li>{@link #initializedState(RangeSplit)} - 初始化分片状态</li>
 *   <li>{@link #toSplitType(String, RangeSplitState)} - 状态转换为分片</li>
 *   <li>{@link #onSplitFinished(Map)} - 分片完成回调</li>
 * </ul>
 */
public class JdbcSourceReader extends BaseSourceReader<Row, Row, RangeSplit, RangeSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSourceReader.class);

    // ... 其余字段保持不变

    /**
     * 构造函数
     */
    public JdbcSourceReader(
            Supplier<BaseSplitReader<Row, RangeSplit>> splitReaderSupplier,
            Configuration config,
            SourceReaderContext context,
            String url, String username, String password,
            String table, String sql, String splitColumn,
            Integer fetchSize, Integer queryTimeout,
            JdbcDialect dialect) {
        super(splitReaderSupplier, new RowRecordEmitter(), config, context);
        // ... 其余赋值保持不变
    }

    // ... 其余方法保持不变

    /**
     * 创建 JdbcSplitReader 供应器
     */
    public static Supplier<BaseSplitReader<Row, RangeSplit>> createSplitReaderSupplier(
            String url, String username, String password,
            String table, String sql, String splitColumn,
            Integer fetchSize, Integer queryTimeout,
            JdbcDialect dialect) {
        return () -> new JdbcSplitReader(url, username, password, table, sql,
                splitColumn, fetchSize, queryTimeout, dialect);
    }
}
```

---

## Task 4: 修改 JdbcSource 输出类型

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 1: 修改类型参数和 import**

```java
package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.source.jdbc.AbstractRangeSplitSource;
import com.etl.source.jdbc.RangeEnumCheckpoint;
import com.etl.source.jdbc.RangeSplit;
import serde.com.etl.core.source.DefaultCheckpointSerializer;
import serde.com.etl.core.source.DefaultSplitSerializer;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 *
 * <p>优化后使用新的抽象类：
 * <ul>
 *   <li>{@link JdbcSplitEnumerator} - 继承 BaseSplitEnumerator</li>
 *   <li>{@link JdbcSourceReader} - 继承 BaseSourceReader</li>
 *   <li>默认序列化器 - 无需手写</li>
 *   <li>直接输出 Flink Row 类型</li>
 * </ul>
 */
public class JdbcSource extends AbstractRangeSplitSource<Row> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);

    // ... 其余代码保持不变

    @Override
    public SourceReader<Row, RangeSplit> createReader(SourceReaderContext readerContext) {
        logger.info("创建 SourceReader");

        // 创建 SplitReader 供应器
        var splitReaderSupplier = JdbcSourceReader.createSplitReaderSupplier(
                url, username, password, table, sql,
                splitColumn, fetchSize, queryTimeout, dialect);

        // 创建 Reader
        return new JdbcSourceReader(
                splitReaderSupplier,
                new Configuration(),
                readerContext,
                url, username, password, table, sql,
                splitColumn, fetchSize, queryTimeout, dialect);
    }

    // ... 其余方法保持不变
}
```

---

## Task 5: 删除 JdbcRecord 类

**Files:**
- Delete: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcRecord.java`

- [ ] **Step 1: 删除文件**

```bash
rm flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcRecord.java
```

---

## Task 6: 编译验证

- [ ] **Step 1: 编译项目**

```bash
mvn clean compile
```

预期输出：`BUILD SUCCESS`

- [ ] **Step 2: 运行测试**

```bash
mvn test
```

预期输出：所有测试通过

- [ ] **Step 3: 打包验证**

```bash
mvn clean package -DskipTests
```

预期输出：成功生成 JAR 文件

---

## Task 7: 提交变更

- [ ] **Step 1: 提交代码**

```bash
git add .
git commit -m "refactor: 删除 JdbcRecord 包装类，直接使用 Flink Row 类型

- 删除 JdbcRecord.java 包装类
- 重命名 JdbcRecordEmitter 为 RowRecordEmitter
- 修改 JdbcSplitReader 直接返回 Row
- 修改 JdbcSourceReader 类型参数
- 修改 JdbcSource 输出类型为 Row

优化效果：
- 减少对象创建开销
- 简化类型系统
- 与 Flink 生态更好集成"
```

---

## 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 下游依赖 JdbcRecord | 低 | 检查确认无外部依赖 |
| 序列化兼容性 | 低 | 仅限内存优化，不涉及持久化 |
| 类型安全 | 低 | Row 是 Flink 标准类型 |

---

## 验收标准

1. 所有编译通过
2. 所有测试通过
3. 成功打包可执行 JAR
4. 代码审查确认无 JdbcRecord 残留引用