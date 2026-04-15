# 通用 JDBC Source 重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 删除 Dialect 概念，简化为通用 JDBC Source。

**Architecture:**

1. 创建 `JdbcSplitHelper` 工具类 - 只放分片相关函数
2. `inferType` 直接写在 `JdbcSource#getProducedType` 中
3. `createRow` 直接写在 `JdbcSplitReader#fetchBatch` 中
4. 删除 `JdbcDialect` 接口、`MySQLDialect` 实现、`flink-etl-source-mysql` 模块

**Tech Stack:** Java 11, Flink 1.19.0, SPI (AutoService)

---

## 文件变更概览

| 操作 | 文件路径                        | 说明                              |
|----|-----------------------------|---------------------------------|
| 创建 | `JdbcSplitHelper.java`      | 分片工具类（3个函数）                     |
| 修改 | `JdbcSource.java`           | 移除 dialect，内联 inferType         |
| 修改 | `JdbcSplitEnumerator.java`  | 使用 JdbcSplitHelper              |
| 修改 | `JdbcSplitReader.java`      | 使用 JdbcSplitHelper，内联 createRow |
| 修改 | `JdbcSourceConfig.java`     | 移除 dialect 字段                   |
| 创建 | `JdbcSourcePlugin.java`     | 通用 JDBC Source 插件               |
| 删除 | `dialect/JdbcDialect.java`  | 删除方言接口                          |
| 删除 | `dialect/MySQLDialect.java` | 删除 MySQL 方言                     |
| 删除 | `flink-etl-source-mysql/`   | 删除整个模块                          |
| 修改 | `flink-etl-source/pom.xml`  | 移除 mysql 模块                     |
| 修改 | `flink-etl-client/pom.xml`  | 更新依赖                            |
| 修改 | `docs/examples/*.json`      | 更新配置示例                          |
| 修改 | `docs/plugins.md`           | 更新插件文档                          |

---

### Task 1: 创建 JdbcSplitHelper

**Files:**

- Create: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitHelper.java`

- [ ] **Step 1: 创建分片工具类**

只包含 3 个分片相关的静态方法：

```java
package com.etl.source.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * JDBC 分片工具类
 * 提供分片计算和 SQL 构建的静态方法
 */
@Slf4j
public final class JdbcSplitHelper {

    private JdbcSplitHelper() {
        // 工具类不允许实例化
    }

    /**
     * 计算数值范围分片
     *
     * @param splitColumn 分片列名
     * @param min         分片列最小值
     * @param max         分片列最大值
     * @param parallelism 并行度（期望的分片数量）
     * @return 分片列表
     */
    public static List<RangeSplit> calculateSplits(String splitColumn, long min, long max, int parallelism) {
        List<RangeSplit> splits = new ArrayList<>();

        log.info("计算分片: range=[{}, {}], parallelism={}", min, max, parallelism);

        if (min > max) {
            log.warn("数据范围为空，不创建分片");
            return splits;
        }

        long totalRecords = max - min + 1;
        int actualSplitCount = (int) Math.min(parallelism, totalRecords);

        if (actualSplitCount < parallelism) {
            log.info("数据量({})小于并行度({})，实际分片数调整为 {}",
                    totalRecords, parallelism, actualSplitCount);
        }

        long splitSize = (totalRecords + actualSplitCount - 1) / actualSplitCount;

        long currentStart = min;
        for (int i = 0; i < actualSplitCount && currentStart <= max; i++) {
            long currentEnd = Math.min(currentStart + splitSize - 1, max);
            splits.add(new RangeSplit(splitColumn, currentStart, currentEnd));
            currentStart = currentEnd + 1;
        }

        log.info("共计算出 {} 个分片", splits.size());
        return splits;
    }

    /**
     * 构建范围查询 SQL（获取分片列的 MIN 和 MAX 值）
     *
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @return 查询 SQL
     */
    public static String buildRangeQuery(String table, String sql, String splitColumn) {
        if (table != null) {
            return String.format("SELECT MIN(%s), MAX(%s) FROM %s", splitColumn, splitColumn, table);
        } else {
            return String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t", splitColumn, splitColumn, sql);
        }
    }

    /**
     * 构建分片数据查询 SQL
     *
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param start       起始值
     * @param end         结束值
     * @return 查询 SQL
     */
    public static String buildSplitQuery(String table, String sql, String splitColumn, long start, long end) {
        if (table != null) {
            return String.format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d", table, splitColumn, start, end);
        } else {
            return String.format("SELECT * FROM (%s) AS t WHERE %s BETWEEN %d AND %d", sql, splitColumn, start, end);
        }
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitHelper.java
git commit -m "feat: 创建 JdbcSplitHelper 分片工具类"
```

---

### Task 2: 更新 JdbcSourceConfig

**Files:**

- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java`

- [ ] **Step 1: 移除 dialect 字段**

```java
package com.etl.jdbc.source.config;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/**
 * JDBC Source 配置
 */
@Getter
@Builder
public class JdbcSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

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
    /** 批大小，默认100 */
    private final Integer batchSize;
    /** 查询超时 */
    private final Integer queryTimeout;
    // 删除 dialect 字段
}
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java
git commit -m "refactor: JdbcSourceConfig 移除 dialect 字段"
```

---

### Task 3: 重构 JdbcSource

**Files:**

- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 1: 重构 JdbcSource，移除 dialect，内联 inferType 逻辑**

```java
package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.TypeConverter;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import config.source.com.etl.connector.jdbc.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.*;
import java.util.function.Supplier;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 */
@Slf4j
public class JdbcSource extends AbstractSplitSource<RangeSplit, RangeEnumCheckpoint> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSource(SourceConfig config) {
        super(config);
        String url = config.getString("url");
        Preconditions.checkNotNull(url, "url is null");

        // MySQL 需要添加 useCursorFetch 参数，使 batchSize 生效
        if (url.contains("jdbc:mysql:") && !url.contains("useCursorFetch=true")) {
            url = url.contains("?") ? url + "&useCursorFetch=true" : url + "?useCursorFetch=true";
            log.info("MySQL URL 添加 useCursorFetch 参数");
        }

        String username = config.getString("username");
        String password = config.getString("password");

        String table = config.getString("table");
        String sql = config.getString("sql");

        String splitColumn = config.getString("splitColumn");
        Preconditions.checkNotNull(splitColumn, "splitColumn is null");

        Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        Integer queryTimeout = config.getInteger("queryTimeout");

        this.jdbcSourceConfig = JdbcSourceConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .splitColumn(splitColumn)
                .batchSize(batchSize)
                .queryTimeout(queryTimeout)
                .build();

        log.info("创建 JdbcSource: {}", this.jdbcSourceConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, jdbcSourceConfig);
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    restoreEnumerator(SplitEnumeratorContext<RangeSplit> enumContext,
                      RangeEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, checkpoint, jdbcSourceConfig);
    }

    @Override
    public SourceReader<Row, RangeSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, RangeSplit>>) () ->
                new JdbcSplitReader(jdbcSourceConfig);
        return new JdbcSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<RangeEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        String table = jdbcSourceConfig.getTable();
        String sql = jdbcSourceConfig.getSql();
        String url = jdbcSourceConfig.getUrl();
        String username = jdbcSourceConfig.getUsername();
        String password = jdbcSourceConfig.getPassword();

        // 构建示例查询 SQL
        String sampleQuery;
        if (table != null) {
            sampleQuery = "SELECT * FROM " + table + " WHERE 1=0";
        } else {
            sampleQuery = "SELECT * FROM (" + sql + ") AS t WHERE 1=0";
        }
        log.info("推断 Schema: {}", sampleQuery);

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleQuery)) {

            // 从 ResultSetMetaData 推断类型
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            String[] names = new String[columnCount];
            TypeInformation<?>[] types = new TypeInformation<?>[columnCount];

            for (int i = 1; i <= columnCount; i++) {
                int index = i - 1;
                names[index] = metaData.getColumnLabel(i);
                types[index] = TypeConverter.fromSqlType(metaData.getColumnType(i));
            }

            return Types.ROW_NAMED(names, types);

        } catch (SQLException e) {
            throw new RuntimeException("从数据库推断 Schema 失败: " + e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java
git commit -m "refactor: JdbcSource 移除 dialect 参数，内联 inferType 逻辑"
```

---

### Task 4: 重构 JdbcSplitEnumerator

**Files:**

- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java`

- [ ] **Step 1: 使用 JdbcSplitHelper，移除 calculateSplits 方法**

```java
package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitEnumerator;
import config.source.com.etl.connector.jdbc.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.sql.*;
import java.util.List;

/**
 * JDBC 分片枚举器
 */
@Slf4j
public class JdbcSplitEnumerator extends BaseSplitEnumerator<RangeSplit, RangeEnumCheckpoint> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context, JdbcSourceConfig jdbcSourceConfig) {
        super(context);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 初始化");
    }

    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context,
                               RangeEnumCheckpoint checkpoint,
                               JdbcSourceConfig jdbcSourceConfig) {
        super(context, checkpoint);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 从检查点恢复，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public void start() {
        log.info("JDBC SplitEnumerator 启动，开始计算分片");

        String url = jdbcSourceConfig.getUrl();
        String username = jdbcSourceConfig.getUsername();
        String password = jdbcSourceConfig.getPassword();
        String table = jdbcSourceConfig.getTable();
        String sql = jdbcSourceConfig.getSql();
        String splitColumn = jdbcSourceConfig.getSplitColumn();

        // 查询分片列范围
        Range<Long> range = getSplitColumnRange(url, username, password, table, sql, splitColumn);
        log.info("分片列范围: [{}, {}]", range.getMinimum(), range.getMaximum());

        // 使用 JdbcSplitHelper 计算分片
        int parallelism = context.currentParallelism();
        List<RangeSplit> splits = JdbcSplitHelper.calculateSplits(
                splitColumn, range.getMinimum(), range.getMaximum(), parallelism);

        addPendingSplits(splits);
        log.info("JDBC SplitEnumerator 启动完成，分片数: {}", splits.size());
    }

    private Range<Long> getSplitColumnRange(String url, String username, String password,
                                            String table, String sql, String splitColumn) {
        String querySql = JdbcSplitHelper.buildRangeQuery(table, sql, splitColumn);
        log.info("查询分片范围: {}", querySql);

        try (Connection conn = DriverManager.getConnection(url, username, password);
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

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java
git commit -m "refactor: JdbcSplitEnumerator 使用 JdbcSplitHelper"
```

---

### Task 5: 重构 JdbcSplitReader

**Files:**

- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitReader.java`

- [ ] **Step 1: 使用 JdbcSplitHelper，内联 createRow 逻辑**

```java
package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitReader;
import config.source.com.etl.connector.jdbc.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

/**
 * JDBC 分片读取器
 */
@Slf4j
public class JdbcSplitReader implements BaseSplitReader<Row, RangeSplit> {

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final String splitColumn;
    private final int batchSize;
    private final Integer queryTimeout;

    private final Queue<RangeSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    private RangeSplit currentSplit;
    private Connection currentConnection;
    private Statement currentStatement;
    private ResultSet currentResultSet;
    private boolean hasNextRecord;
    private int currentOffset;

    public JdbcSplitReader(JdbcSourceConfig config) {
        this.url = config.getUrl();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.table = config.getTable();
        this.sql = config.getSql();
        this.splitColumn = config.getSplitColumn();
        this.batchSize = config.getBatchSize();
        this.queryTimeout = config.getQueryTimeout();
    }

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        if (currentSplit == null) {
            RangeSplit split = pendingSplits.poll();
            if (split == null) {
                RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }
            startNewSplit(split);
        }
        return fetchBatch();
    }

    private void startNewSplit(RangeSplit split) throws IOException {
        log.info("开始读取分片: {}", split.splitId());

        try {
            currentConnection = DriverManager.getConnection(url, username, password);
            currentStatement = currentConnection.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
            );

            currentStatement.setFetchSize(batchSize);
            if (queryTimeout != null) {
                currentStatement.setQueryTimeout(queryTimeout);
            }

            String querySql = JdbcSplitHelper.buildSplitQuery(table, sql, splitColumn,
                    split.getStart(), split.getEnd());
            log.debug("执行查询: {}", querySql);

            currentResultSet = currentStatement.executeQuery(querySql);
            hasNextRecord = currentResultSet.next();
            currentOffset = 0;
            currentSplit = split;

        } catch (SQLException e) {
            closeCurrentSplit();
            throw new IOException("读取分片失败: " + split.splitId(), e);
        }
    }

    private RecordsWithSplitIds<Row> fetchBatch() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        try {
            int recordsInBatch = 0;
            ResultSetMetaData metaData = currentResultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (hasNextRecord && recordsInBatch < batchSize) {
                // 内联 createRow 逻辑
                Row row = new Row(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.setField(i - 1, currentResultSet.getObject(i));
                }

                builder.add(currentSplit.splitId(), row);
                recordsInBatch++;
                currentOffset++;

                hasNextRecord = currentResultSet.next();
            }

            if (!hasNextRecord) {
                finishedSplits.add(currentSplit.splitId());
                log.info("分片 {} 读取完成，共 {} 条记录", currentSplit.splitId(), currentOffset);
                closeCurrentSplit();
            }

        } catch (SQLException e) {
            closeCurrentSplit();
            throw new IOException("读取分片失败: " + currentSplit.splitId(), e);
        }

        return builder.build();
    }

    private void closeCurrentSplit() {
        closeQuietly(currentResultSet, "ResultSet");
        closeQuietly(currentStatement, "Statement");
        closeQuietly(currentConnection, "Connection");

        currentResultSet = null;
        currentStatement = null;
        currentConnection = null;
        currentSplit = null;
        hasNextRecord = false;
    }

    private void closeQuietly(AutoCloseable resource, String resourceName) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("关闭 {} 失败", resourceName, e);
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RangeSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个新分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        closeCurrentSplit();
        log.info("JdbcSplitReader 关闭");
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitReader.java
git commit -m "refactor: JdbcSplitReader 使用 JdbcSplitHelper，内联 createRow 逻辑"
```

---

### Task 6: 删除 dialect 目录

**Files:**

- Delete: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/`

- [ ] **Step 1: 删除 dialect 目录**

```bash
rm -rf flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add -A flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/
git commit -m "refactor: 删除 dialect 目录"
```

---

### Task 7: 创建通用 JdbcSourcePlugin

**Files:**

- Create: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSourcePlugin.java`

- [ ] **Step 1: 创建通用 JDBC Source 插件**

```java
package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;

/**
 * 通用 JDBC Source 插件
 * 支持所有 JDBC 数据库
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class JdbcSourcePlugin implements SourcePlugin {

    @Override
    public String getType() {
        return "jdbc";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        log.info("创建 JDBC Source");
        return new JdbcSource(config);
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSourcePlugin.java
git commit -m "feat: 创建通用 JdbcSourcePlugin"
```

---

### Task 8: 删除 flink-etl-source-mysql 模块

**Files:**

- Delete: `flink-etl-source/flink-etl-source-mysql/`
- Modify: `flink-etl-source/pom.xml`

- [ ] **Step 1: 删除 mysql 模块目录**

```bash
rm -rf flink-etl-source/flink-etl-source-mysql
```

- [ ] **Step 2: 修改 flink-etl-source/pom.xml**

```xml

<modules>
    <module>flink-etl-source-jdbc</module>
    <!-- 删除: <module>flink-etl-source-mysql</module> -->
    <module>flink-etl-source-localfile</module>
</modules>
```

- [ ] **Step 3: 验证编译通过**

Run: `mvn compile -pl flink-etl-source -am`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-source/pom.xml
git add -A flink-etl-source/flink-etl-source-mysql/
git commit -m "refactor: 删除 flink-etl-source-mysql 模块"
```

---

### Task 9: 更新 client 模块依赖

**Files:**

- Modify: `flink-etl-client/pom.xml`

- [ ] **Step 1: 更新依赖**

```xml
<!-- Source 插件 -->
<dependency>
    <groupId>com.etl</groupId>
    <!-- 改为 jdbc 模块 -->
    <artifactId>flink-etl-source-jdbc</artifactId>
    <version>${project.version}</version>
</dependency>
<dependency>
<groupId>com.etl</groupId>
<artifactId>flink-etl-source-localfile</artifactId>
<version>${project.version}</version>
</dependency>

        <!-- Sink 插件（保持不变） -->
<dependency>
<groupId>com.etl</groupId>
<artifactId>flink-etl-sink-console</artifactId>
<version>${project.version}</version>
</dependency>
<dependency>
<groupId>com.etl</groupId>
<artifactId>flink-etl-sink-mysql</artifactId>
<version>${project.version}</version>
</dependency>

        <!-- MySQL 驱动 -->
<dependency>
<groupId>com.mysql</groupId>
<artifactId>mysql-connector-j</artifactId>
</dependency>
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-client -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-client/pom.xml
git commit -m "refactor: client 依赖改为 flink-etl-source-jdbc"
```

---

### Task 10: 更新示例配置文件

**Files:**

- Modify: `docs/examples/mysql-to-console.json`
- Modify: `docs/examples/mysql-custom-sql-to-console.json`
- Modify: `docs/examples/mysql-to-mysql.json`

- [ ] **Step 1: 更新所有示例文件**

将 `"type": "mysql"` 改为 `"type": "jdbc"`。

- [ ] **Step 2: Commit**

```bash
git add docs/examples/
git commit -m "docs: 更新示例配置使用 jdbc 类型"
```

---

### Task 11: 更新 docs/plugins.md

**Files:**

- Modify: `docs/plugins.md`

- [ ] **Step 1: 将 MySQL Source 章节改为 JDBC Source**

更新文档：

1. 目录中 "MySQL Source" 改为 "JDBC Source"
2. 章节标题改为 "JDBC Source"
3. 说明支持所有 JDBC 数据库
4. 示例中 `"type": "mysql"` 改为 `"type": "jdbc"`

- [ ] **Step 2: Commit**

```bash
git add docs/plugins.md
git commit -m "docs: 更新 plugins.md，MySQL Source 改为 JDBC Source"
```

---

### Task 12: 更新 CLAUDE.md 文档

**Files:**

- Modify: `CLAUDE.md`

- [ ] **Step 1: 更新模块结构和配置说明**

更新文档，移除 mysql 模块，说明 jdbc 类型的通用性。

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: 更新 CLAUDE.md"
```

---

### Task 13: 验证完整流程

- [ ] **Step 1: 完整编译**

Run: `mvn clean compile`
Expected: BUILD SUCCESS

- [ ] **Step 2: 完整打包**

Run: `mvn clean package -DskipTests`
Expected: BUILD SUCCESS

---

## 变更影响

**用户配置变更：**

- 原来：`"type": "mysql"`
- 现在：`"type": "jdbc"`

**扩展性：**

- 新增分片类型（如 UUID、字符串主键）只需在 `JdbcSplitHelper` 中添加新方法
- 或创建新的 Helper 类（如 `JdbcUuidSplitHelper`）