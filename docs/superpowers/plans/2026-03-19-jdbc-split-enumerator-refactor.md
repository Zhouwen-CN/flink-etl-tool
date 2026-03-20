# JDBC Source 分片行为迁移实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将分片计算逻辑迁移到 `JdbcSplitEnumerator#start` 中，删除中间层 `AbstractRangeSplitSource`，`JdbcSource` 直接继承 `AbstractSplitSource`。

**Architecture:**
- 当前：`JdbcSource` → `AbstractRangeSplitSource` → `AbstractSplitSource`，分片在 Source 创建 Enumerator 时预计算
- 目标：`JdbcSource` → `AbstractSplitSource`，分片在 Enumerator 启动时延迟计算

**Tech Stack:** Java 11, Apache Flink 1.19.0, Lombok

---

## 文件结构

**新增文件：**
- `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitConfig.java` - 分片配置数据类

**修改文件：**
- `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java` - 添加分片计算逻辑
- `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java` - 直接继承 AbstractSplitSource

**删除文件：**
- `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/AbstractRangeSplitSource.java`

---

## Task 1: 创建分片配置数据类

**Files:**
- Create: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitConfig.java`

- [ ] **Step 1: 创建 JdbcSplitConfig 数据类**

```java
package com.etl.source.jdbc;

import lombok.Builder;
import lombok.Getter;

/**
 * JDBC 分片配置
 * 用于传递分片所需的所有参数到 JdbcSplitEnumerator
 */
@Getter
@Builder
public class JdbcSplitConfig {
    /** 数据库连接 URL */
    private final String url;
    /** 用户名 */
    private final String username;
    /** 密码 */
    private final String password;
    /** 表名 */
    private final String table;
    /** 自定义 SQL */
    private final String sql;
    /** 分片列名 */
    private final String splitColumn;
    /** 数据库方言 */
    private final JdbcDialect dialect;
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitConfig.java
git commit -m "feat(jdbc): 添加 JdbcSplitConfig 分片配置数据类"
```

---

## Task 2: 重构 JdbcSplitEnumerator

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java`

- [ ] **Step 1: 添加分片计算逻辑到 JdbcSplitEnumerator**

完整重构后的代码：

```java
package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitEnumerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC 分片枚举器
 * 继承 BaseSplitEnumerator，在 start() 中执行分片计算
 *
 * <p>分片计算延迟到 enumerator 启动时执行，而非创建时预计算。
 * 这样可以在运行时动态获取数据范围，支持更灵活的分片策略。
 */
@Slf4j
public class JdbcSplitEnumerator extends BaseSplitEnumerator<RangeSplit, RangeEnumCheckpoint> {

    /** 分片配置 */
    private final JdbcSplitConfig jdbcSourceConfig;

    /**
     * 构造函数（首次创建，无预计算分片）
     *
     * @param context 枚举器上下文
     * @param jdbcSourceConfig 分片配置
     */
    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context, JdbcSplitConfig jdbcSourceConfig) {
        super(context);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 初始化，延迟分片计算");
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context 枚举器上下文
     * @param checkpoint 检查点
     * @param jdbcSourceConfig 分片配置（用于恢复后可能的新分片计算）
     */
    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context,
                                RangeEnumCheckpoint checkpoint,
                                JdbcSplitConfig jdbcSourceConfig) {
        super(context, checkpoint);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 从检查点恢复，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public void start() {
        log.info("JDBC SplitEnumerator 启动，开始计算分片");

        // 查询分片列范围
        Range<Long> range = getSplitColumnRange();
        log.info("分片列范围: [{}, {}]", range.getMinimum(), range.getMaximum());

        // 计算分片
        int parallelism = context.currentParallelism();
        List<RangeSplit> splits = calculateSplits(jdbcSourceConfig.getSplitColumn(), range, parallelism);

        // 添加到待处理队列
        addPendingSplits(splits);

        log.info("JDBC SplitEnumerator 启动完成，分片数: {}", splits.size());
    }

    /**
     * 查询数据库获取分片列的范围
     *
     * @return 分片列的范围
     */
    private Range<Long> getSplitColumnRange() {
        String querySql = jdbcSourceConfig.getDialect().buildRangeQuery(
                jdbcSourceConfig.getTable(), jdbcSourceConfig.getSql(), jdbcSourceConfig.getSplitColumn());
        log.info("查询分片范围: {}", querySql);

        try (Connection conn = DriverManager.getConnection(
                jdbcSourceConfig.getUrl(), jdbcSourceConfig.getUsername(), jdbcSourceConfig.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql)) {

            if (rs.next()) {
                long min = rs.getLong(1);
                long max = rs.getLong(2);
                return Range.between(min, max);
            }
            return Range.between(0L, 0L);
        } catch (SQLException e) {
            throw new RuntimeException("获取分片范围失败: " + e.getMessage(), e);
        }
    }

    /**
     * 根据范围和并行度计算所有分片
     *
     * @param splitColumn 分片键
     * @param range       数据范围
     * @param parallelism 并行度（分片数量）
     * @return 分片列表
     */
    private static List<RangeSplit> calculateSplits(String splitColumn, Range<Long> range, int parallelism) {
        List<RangeSplit> splits = new ArrayList<>();

        long start = range.getMinimum();
        long end = range.getMaximum();

        log.info("计算分片: range=[{}, {}], parallelism={}", start, end, parallelism);

        if (start > end) {
            log.warn("数据范围为空，不创建分片");
            return splits;
        }

        long totalRecords = end - start + 1;
        int actualSplitCount = (int) Math.min(parallelism, totalRecords);

        if (actualSplitCount < parallelism) {
            log.info("数据量({})小于并行度({})，实际分片数调整为 {}",
                    totalRecords, parallelism, actualSplitCount);
        }

        long splitSize = (totalRecords + actualSplitCount - 1) / actualSplitCount;

        long currentStart = start;
        for (int i = 0; i < actualSplitCount && currentStart <= end; i++) {
            long currentEnd = Math.min(currentStart + splitSize - 1, end);
            splits.add(new RangeSplit(splitColumn, currentStart, currentEnd));
            currentStart = currentEnd + 1;
        }

        log.info("共计算出 {} 个分片", splits.size());
        return splits;
    }

    @Override
    public RangeEnumCheckpoint snapshotState(long checkpointId) {
        List<RangeSplit> pending = List.copyOf(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new RangeEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("JDBC SplitEnumerator 关闭");
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java
git commit -m "refactor(jdbc): 将分片计算逻辑迁移到 JdbcSplitEnumerator#start"
```

---

## Task 3: 重构 JdbcSource 直接继承 AbstractSplitSource

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 1: 修改 JdbcSource 继承关系**

将 `extends AbstractRangeSplitSource` 改为 `extends AbstractSplitSource<RangeSplit, RangeEnumCheckpoint>`，删除 `getSplitColumnRange()` 方法，简化 `createEnumerator` 和 `restoreEnumerator`：

```java
package com.etl.source.jdbc;

import com.etl.core.localFileSourceConfig.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.source.jdbc.dialect.MySQLDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.*;
import java.util.List;
import java.util.function.Supplier;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 *
 * <p>直接继承 AbstractSplitSource，分片逻辑由 JdbcSplitEnumerator 处理。
 */
@Slf4j
public class JdbcSource extends AbstractSplitSource<RangeSplit, RangeEnumCheckpoint> {

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final String splitColumn;
    private final int batchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;

    public JdbcSource(SourceConfig localFileSourceConfig, JdbcDialect dialect) {
        super(localFileSourceConfig);
        String url = localFileSourceConfig.getString("url");
        Preconditions.checkNotNull(url, "url is null");

        // mysql 需要加上这个参数，batchSize 参数才能生效
        if (dialect instanceof MySQLDialect) {
            if(!url.contains("useCursorFetch=true")){
                if(url.contains("?")){
                    url += "&useCursorFetch=true";
                }else{
                    url += "?useCursorFetch=true";
                }
            }
        }
        this.url = url;
        this.username = localFileSourceConfig.getString("username");
        this.password = localFileSourceConfig.getString("password");
        this.table = localFileSourceConfig.getString("table");
        this.splitColumn = localFileSourceConfig.getString("splitColumn");
        Preconditions.checkNotNull(this.splitColumn, "splitColumn is null");
        this.sql = localFileSourceConfig.getString("sql");
        this.batchSize = localFileSourceConfig.getInteger("batchSize", getDefaultBatchSize());
        this.queryTimeout = localFileSourceConfig.getInteger("queryTimeout");
        this.dialect = dialect;

        log.info("创建 JdbcSource: table={}, sql={}, splitColumn={}", table, sql, splitColumn);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
        log.info("创建 SplitEnumerator");

        JdbcSplitConfig jdbcSourceConfig = JdbcSplitConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .splitColumn(splitColumn)
                .dialect(dialect)
                .build();

        return new JdbcSplitEnumerator(enumContext, jdbcSourceConfig);
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    restoreEnumerator(SplitEnumeratorContext<RangeSplit> enumContext,
                      RangeEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");

        JdbcSplitConfig jdbcSourceConfig = JdbcSplitConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .splitColumn(splitColumn)
                .dialect(dialect)
                .build();

        return new JdbcSplitEnumerator(enumContext, checkpoint, jdbcSourceConfig);
    }

    @Override
    public SourceReader<Row, RangeSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");

        // 创建 SplitReader 供应器
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, RangeSplit>>) () ->
                new JdbcSplitReader(url, username, password, table, sql,
                        splitColumn, batchSize, queryTimeout, dialect);

        // 创建 Reader
        return new JdbcSourceReader(
                splitReaderSupplier,
                readerContext
        );
    }

    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        // 使用默认序列化器
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<RangeEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        // 使用默认序列化器
        return new DefaultCheckpointSerializer<>();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        // 如果配置了 schema，使用父类实现
        if (getConfig().getSchema() != null) {
            return super.getProducedType();
        }

        // 否则从数据库推断
        String sampleQuery = dialect.buildSampleQuery(table, sql);
        log.info("推断 Schema: {}", sampleQuery);

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleQuery)) {
            return dialect.inferType(rs.getMetaData());
        } catch (SQLException e) {
            throw new RuntimeException("从数据库推断 Schema 失败: " + e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java
git commit -m "refactor(jdbc): JdbcSource 直接继承 AbstractSplitSource"
```

---

## Task 4: 删除 AbstractRangeSplitSource

**Files:**
- Delete: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/AbstractRangeSplitSource.java`

- [ ] **Step 1: 删除 AbstractRangeSplitSource 文件**

Run: `rm flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/AbstractRangeSplitSource.java`
Expected: 文件已删除

- [ ] **Step 2: 编译验证**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add -A flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/AbstractRangeSplitSource.java
git commit -m "refactor(jdbc): 删除 AbstractRangeSplitSource 中间层"
```

---

## Task 5: 更新 CLAUDE.md 文档

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: 更新文档中的 AbstractRangeSplitSource 引用**

将文档中所有 `AbstractRangeSplitSource` 引用更新或删除：

1. **架构概览部分**：删除 `AbstractRangeSplitSource` 相关描述
2. **Source 抽象层架构部分**：更新继承层级描述
3. **关键抽象类部分**：删除 `AbstractRangeSplitSource` 条目

关键修改点：
- 删除 "AbstractRangeSplitSource: 范围分片 Source（jdbc 模块），子类只需实现 `getSplitColumnRange()` 方法"
- 更新 "JdbcSource: JDBC Source 实现，继承 `AbstractRangeSplitSource`" → "JdbcSource: JDBC Source 实现，直接继承 `AbstractSplitSource`"

- [ ] **Step 2: 提交**

```bash
git add CLAUDE.md
git commit -m "docs: 更新架构文档，删除 AbstractRangeSplitSource 引用"
```

---

## Task 6: 集成测试验证

**Files:**
- 运行现有的示例配置进行验证

- [ ] **Step 1: 打包项目**

Run: `mvn clean package -DskipTests -q`
Expected: BUILD SUCCESS

- [ ] **Step 2: 运行 MySQL 到 Console 示例**

Run: `java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-console.json`
Expected: 正常执行，日志显示分片计算在 JdbcSplitEnumerator#start 中执行

- [ ] **Step 3: 验证日志输出**

检查日志中是否包含：
- "JDBC SplitEnumerator 启动，开始计算分片"
- "查询分片范围: ..."
- "分片列范围: [..., ...]"
- "JDBC SplitEnumerator 启动完成，分片数: X"

---

## Task 7: 最终验证

- [ ] **Step 1: 整体编译验证**

Run: `mvn clean compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 2: 查看变更文件**

Run: `git status`
Expected: 显示所有修改的文件已提交

---

## 变更摘要

| 文件 | 变更类型 | 描述 |
|------|----------|------|
| `JdbcSplitConfig.java` | 新增 | 分片配置数据类 |
| `JdbcSplitEnumerator.java` | 修改 | 添加分片计算逻辑到 start()，包含 calculateSplits 私有方法 |
| `JdbcSource.java` | 修改 | 直接继承 AbstractSplitSource，删除 getSplitColumnRange() |
| `AbstractRangeSplitSource.java` | 删除 | 中间抽象层，功能已迁移 |
| `CLAUDE.md` | 修改 | 删除 AbstractRangeSplitSource 引用，更新架构描述 |

**代码行数变化预估：**
- 新增：约 55 行（JdbcSplitConfig + JdbcSplitEnumerator 增强）
- 删除：约 75 行（删除 AbstractRangeSplitSource + JdbcSource 简化）
- 净减：约 20 行

**架构简化效果：**
- 继承层级：3 层 → 2 层
- 分片计算：预计算 → 延迟计算
- 职责更清晰：Source 负责创建组件，Enumerator 负责分片计算