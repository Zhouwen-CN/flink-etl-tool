# JDBC Source splitColumn 可选配置实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 JDBC Source 的 splitColumn 配置项改为可选，未配置时打印警告日志并生成单个全表扫描分片。同时增加类型校验，确保 splitColumn 为支持的类型（当前仅数值类型）。

**Architecture:**
1. 修改 RangeSplit 直接存储查询 SQL，简化设计
2. 新增 SplitStrategy 枚举定义分片策略，预留扩展点
3. 修改 JdbcSource 移除 splitColumn 必填校验，添加类型校验
4. 调整分片逻辑支持无分片列的全表扫描模式

**Tech Stack:** Java 1.8, Flink 1.15.2, Lombok

---

## 文件结构

| 文件 | 操作 | 说明 |
|------|------|------|
| `SplitStrategy.java` | 新建 | 分片策略枚举，定义支持的分片类型，预留扩展点 |
| `SqlUtils.java` | 修改 | 新增 getColumnType 方法获取列类型 |
| `RangeSplit.java` | 修改 | 简化为直接存储查询 SQL |
| `JdbcSplitHelper.java` | 修改 | 新增全表扫描和类型校验方法 |
| `JdbcSource.java` | 修改 | 移除 splitColumn 必填校验，添加类型校验 |
| `JdbcSourceConfig.java` | 修改 | 更新 splitColumn 字段注释，新增 splitStrategy 字段 |
| `JdbcSplitEnumerator.java` | 修改 | 根据 splitColumn 是否为空决定分片策略 |
| `JdbcSplitReader.java` | 修改 | 直接执行 split.getQuerySql() |
| `PLUGINS.md` | 修改 | 更新文档，splitColumn 改为非必填，说明类型限制 |

---

## Task 1: 新建 SplitStrategy 枚举定义分片策略

**Files:**
- Create: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/SplitStrategy.java`

- [ ] **Step 1: 创建 SplitStrategy 枚举**

```java
package com.etl.source.jdbc;

import java.sql.Types;

/**
 * 分片策略枚举
 * 定义支持的分片类型和对应的 JDBC 类型
 *
 * <p>设计说明：
 * <ul>
 *   <li>使用枚举而非接口，便于定义固定的分片类型和类型判断逻辑</li>
 *   <li>每个策略定义支持的 JDBC 类型集合，便于扩展新类型</li>
 *   <li>未来可通过新增枚举值支持字符串哈希分片、日期范围分片等</li>
 * </ul>
 */
public enum SplitStrategy {

    /**
     * 数值范围分片
     * 支持所有数值类型：TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL 等
     */
    NUMERIC("数值范围分片", new int[]{
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.FLOAT,
            Types.REAL,
            Types.DOUBLE,
            Types.NUMERIC,
            Types.DECIMAL
    }),

    /**
     * 全表扫描（无分片）
     * 当 splitColumn 未配置时使用
     */
    FULL_TABLE_SCAN("全表扫描", new int[]{});

    private final String description;
    private final int[] supportedJdbcTypes;

    SplitStrategy(String description, int[] supportedJdbcTypes) {
        this.description = description;
        this.supportedJdbcTypes = supportedJdbcTypes;
    }

    /**
     * 检查 JDBC 类型是否支持当前分片策略
     *
     * @param jdbcType JDBC 类型常量（来自 java.sql.Types）
     * @return true 表示支持
     */
    public boolean supports(int jdbcType) {
        for (int supportedType : supportedJdbcTypes) {
            if (supportedType == jdbcType) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取支持的 JDBC 类型名称列表（用于错误提示）
     *
     * @return 类型名称列表
     */
    public String getSupportedTypeNames() {
        if (this == FULL_TABLE_SCAN) {
            return "无";
        }
        return "TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC";
    }

    public String getDescription() {
        return description;
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/SplitStrategy.java
git commit -m "feat(jdbc-source): 新增 SplitStrategy 枚举定义分片策略，预留扩展点"
```

---

## Task 2: 修改 SqlUtils 新增获取列类型方法

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java`

- [ ] **Step 1: 添加 getColumnType 方法**

在 `SqlUtils.java` 末尾添加：

```java
/**
 * 获取指定列的 JDBC 类型
 *
 * @param table       表名（可能为 null）
 * @param sql         自定义 SQL（可能为 null）
 * @param columnName  列名
 * @param url         数据库连接 URL
 * @param username    用户名
 * @param password    密码
 * @return JDBC 类型常量（来自 java.sql.Types）
 * @throws RuntimeException 如果列不存在或查询失败
 */
public static int getColumnType(String table, String sql, String columnName,
                                 String url, String username, String password) {
    // 构建查询语句
    String sampleQuery;
    if (table != null) {
        sampleQuery = "SELECT " + quoteIdentifier(columnName, url) + " FROM " + table + " WHERE 1=0";
    } else {
        sampleQuery = "SELECT " + quoteIdentifier(columnName, url) + " FROM (" + sql + ") AS t WHERE 1=0";
    }

    try (Connection conn = DriverManager.getConnection(url, username, password);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sampleQuery)) {

        ResultSetMetaData metaData = rs.getMetaData();
        if (metaData.getColumnCount() < 1) {
            throw new RuntimeException("无法获取列 '" + columnName + "' 的类型信息");
        }
        return metaData.getColumnType(1);

    } catch (SQLException e) {
        throw new RuntimeException("获取列 '" + columnName + "' 的类型失败: " + e.getMessage(), e);
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn compile -pl flink-etl-core -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java
git commit -m "feat(core): SqlUtils 新增 getColumnType 方法"
```

---

## Task 3: 简化 RangeSplit 直接存储查询 SQL

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/RangeSplit.java`

- [ ] **Step 1: 重构 RangeSplit 类**

```java
package com.etl.source.jdbc;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.Getter;


/**
 * JDBC 分片
 * 存储分片 ID 和该分片的查询 SQL
 *
 * <p>设计说明：
 * <ul>
 *   <li>直接存储查询 SQL，职责清晰（Enumerator 负责生成 SQL，Reader 只执行）</li>
 *   <li>支持任意复杂的分片条件，便于扩展新分片类型</li>
 *   <li>分片 ID 用于状态管理和调试</li>
 * </ul>
 */
@Getter
public class RangeSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    /** 分片 ID，用于状态管理和调试 */
    private final String splitId;

    /** 该分片的查询 SQL */
    private final String querySql;

    /**
     * 构造函数
     *
     * @param splitId  分片 ID
     * @param querySql 该分片的查询 SQL
     */
    public RangeSplit(String splitId, String querySql) {
        this.splitId = splitId;
        this.querySql = querySql;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "RangeSplit{" +
                "splitId='" + splitId + '\'' +
                ", querySql='" + querySql + '\'' +
                '}';
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/RangeSplit.java
git commit -m "refactor(jdbc-source): RangeSplit 简化为直接存储查询 SQL"
```

---

## Task 4: 修改 JdbcSplitHelper 支持全表查询和类型校验

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/utils/JdbcSplitHelper.java`

- [ ] **Step 1: 重构 JdbcSplitHelper，简化分片生成逻辑**

完整替换文件内容：

```java
package com.etl.jdbc.source.utils;

import com.etl.core.utils.SqlUtils;
import source.com.etl.connector.jdbc.RangeSplit;
import enums.source.com.etl.connector.jdbc.SplitStrategy;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JDBC 分片工具类
 * 提供分片计算、SQL 构建、类型校验等静态方法
 */
@Slf4j
public final class JdbcSplitHelper {

    private JdbcSplitHelper() {
        // 工具类不允许实例化
    }

    /**
     * 创建全表扫描分片
     *
     * @param url    数据库连接 URL（用于标识符转义）
     * @param table  表名（可能为 null）
     * @param sql    自定义 SQL（可能为 null）
     * @return 包含单个全表扫描分片的列表
     */
    public static List<RangeSplit> createFullTableScanSplits(String url, String table, String sql) {
        String querySql = buildFullTableQuery(url, table, sql);
        return Collections.singletonList(new RangeSplit("full_table_scan", querySql));
    }

    /**
     * 计算数值范围分片
     *
     * @param url         数据库连接 URL
     * @param username    用户名
     * @param password    密码
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param parallelism 并行度（期望的分片数量）
     * @return 分片列表
     */
    public static List<RangeSplit> calculateNumericSplits(String url, String username, String password,
                                                          String table, String sql, String splitColumn,
                                                          int parallelism) {
        // 1. 查询分片列范围
        long[] range = querySplitColumnRange(url, username, password, table, sql, splitColumn);
        long min = range[0];
        long max = range[1];

        // 空表检查
        if (min > max) {
            log.warn("数据范围为空，不创建分片");
            return new ArrayList<>();
        }

        log.info("分片列范围: [{}, {}]", min, max);

        // 2. 计算分片
        List<RangeSplit> splits = new ArrayList<>();
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
            String querySql = buildRangeQuery(url, table, sql, splitColumn, currentStart, currentEnd);
            String splitId = splitColumn + "_" + currentStart + "_" + currentEnd;
            splits.add(new RangeSplit(splitId, querySql));
            currentStart = currentEnd + 1;
        }

        log.info("共计算出 {} 个分片", splits.size());
        return splits;
    }

    /**
     * 查询分片列的范围（MIN 和 MAX）
     *
     * @return [min, max]，空表返回 [0, -1]
     */
    private static long[] querySplitColumnRange(String url, String username, String password,
                                                String table, String sql, String splitColumn) {
        String quotedColumn = SqlUtils.quoteIdentifier(splitColumn, url);
        String rangeQuery;
        if (table != null) {
            String quotedTable = SqlUtils.quoteIdentifier(table, url);
            rangeQuery = String.format("SELECT MIN(%s), MAX(%s) FROM %s", quotedColumn, quotedColumn, quotedTable);
        } else {
            rangeQuery = String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t", quotedColumn, quotedColumn, sql);
        }

        log.info("查询分片范围: {}", rangeQuery);

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(rangeQuery)) {

            if (rs.next()) {
                // 检查是否为 NULL（空表时 MIN/MAX 返回 NULL）
                if (rs.getObject(1) != null) {
                    return new long[]{rs.getLong(1), rs.getLong(2)};
                }
            }
            return new long[]{0, -1}; // 空表

        } catch (SQLException e) {
            throw new RuntimeException("获取分片范围失败: " + e.getMessage(), e);
        }
    }

    /**
     * 构建全表查询 SQL（不带分片条件）
     */
    private static String buildFullTableQuery(String url, String table, String sql) {
        if (table != null) {
            return "SELECT * FROM " + SqlUtils.quoteIdentifier(table, url);
        } else {
            return "SELECT * FROM (" + sql + ") AS t";
        }
    }

    /**
     * 构建范围查询 SQL
     */
    private static String buildRangeQuery(String url, String table, String sql,
                                          String splitColumn, long start, long end) {
        String quotedColumn = SqlUtils.quoteIdentifier(splitColumn, url);
        if (table != null) {
            String quotedTable = SqlUtils.quoteIdentifier(table, url);
            return String.format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d",
                    quotedTable, quotedColumn, start, end);
        } else {
            return String.format("SELECT * FROM (%s) AS t WHERE %s BETWEEN %d AND %d",
                    sql, quotedColumn, start, end);
        }
    }

    /**
     * 校验分片列类型是否支持分片
     *
     * @param url         数据库连接 URL
     * @param username    用户名
     * @param password    密码
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param strategy    分片策略
     * @throws IllegalArgumentException 如果分片列类型不支持
     */
    public static void validateSplitColumnType(String url, String username, String password,
                                               String table, String sql, String splitColumn,
                                               SplitStrategy strategy) {
        int jdbcType = SqlUtils.getColumnType(table, sql, splitColumn, url, username, password);

        if (!strategy.supports(jdbcType)) {
            throw new IllegalArgumentException(
                    String.format("分片列 '%s' 的类型不支持 %s。支持的类型: %s",
                            splitColumn,
                            strategy.getDescription(),
                            strategy.getSupportedTypeNames())
            );
        }

        log.info("分片列 '{}' 类型校验通过，使用策略: {}", splitColumn, strategy.getDescription());
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/utils/JdbcSplitHelper.java
git commit -m "refactor(jdbc-source): JdbcSplitHelper 简化分片生成逻辑，直接生成带 SQL 的分片"
```

---

## Task 5: 修改 JdbcSourceConfig 添加 splitStrategy 字段

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java`

- [ ] **Step 1: 更新 splitColumn 注释并添加 splitStrategy 字段**

```java
package com.etl.jdbc.source.config;

import enums.source.com.etl.connector.jdbc.SplitStrategy;
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
    /** 分片列名（可选），不配置则使用单分片全表扫描模式 */
    private final String splitColumn;
    /** 分片策略，根据 splitColumn 是否配置决定 */
    private final SplitStrategy splitStrategy;
    /** 批大小，默认100 */
    private final Integer batchSize;
    /** 查询超时 */
    private final Integer queryTimeout;
}
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java
git commit -m "feat(jdbc-source): JdbcSourceConfig 新增 splitStrategy 字段"
```

---

## Task 6: 修改 JdbcSource 移除必填校验并添加类型校验

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 1: 修改 JdbcSource 构造函数**

找到第45-62行，替换为：

```java
String splitColumn = config.getString("splitColumn");
SplitStrategy splitStrategy;

if (splitColumn == null) {
    // 未配置 splitColumn，使用全表扫描模式
    log.warn("未配置 splitColumn，将使用单分片全表扫描模式，无法并行读取。建议配置 splitColumn 以启用并行分片读取。");
    splitStrategy = SplitStrategy.FULL_TABLE_SCAN;
} else {
    // 配置了 splitColumn，校验类型并使用数值分片策略
    splitStrategy = SplitStrategy.NUMERIC;

    // 校验分片列类型
    JdbcSplitHelper.validateSplitColumnType(
            url,
            username,
            password,
            table,
            sql,
            splitColumn,
            splitStrategy);
}

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
        .splitStrategy(splitStrategy)
        .batchSize(batchSize)
        .queryTimeout(queryTimeout)
        .build();
```

需要在文件头部添加 import：

```java
import enums.source.com.etl.connector.jdbc.SplitStrategy;
import utils.source.com.etl.connector.jdbc.JdbcSplitHelper;
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java
git commit -m "feat(jdbc-source): splitColumn 改为可选，新增类型校验"
```

---

## Task 7: 修改 JdbcSplitEnumerator 使用新的分片方法

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java`

- [ ] **Step 1: 修改 start() 方法的分片逻辑**

```java
@Override
public void start() {
    log.info("JDBC SplitEnumerator 启动，开始计算分片");

    List<RangeSplit> splits;

    // 根据分片策略决定分片方式
    if (jdbcSourceConfig.getSplitStrategy() == SplitStrategy.FULL_TABLE_SCAN) {
        // 全表扫描模式，生成单个分片
        log.warn("使用单分片全表扫描模式");
        splits = JdbcSplitHelper.createFullTableScanSplits(
                jdbcSourceConfig.getUrl(),
                jdbcSourceConfig.getTable(),
                jdbcSourceConfig.getSql());
    } else {
        // 数值范围分片模式
        splits = JdbcSplitHelper.calculateNumericSplits(
                jdbcSourceConfig.getUrl(),
                jdbcSourceConfig.getUsername(),
                jdbcSourceConfig.getPassword(),
                jdbcSourceConfig.getTable(),
                jdbcSourceConfig.getSql(),
                jdbcSourceConfig.getSplitColumn(),
                context.currentParallelism());
    }

    addPendingSplits(splits);
    log.info("JDBC SplitEnumerator 启动完成，分片数: {}", splits.size());
}
```

需要在文件头部添加 import：

```java
import enums.source.com.etl.connector.jdbc.SplitStrategy;
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java
git commit -m "refactor(jdbc-source): Enumerator 使用新的分片方法"
```

---

## Task 8: 简化 JdbcSplitReader 直接执行分片 SQL

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitReader.java`

- [ ] **Step 1: 简化 JdbcSplitReader，直接使用 split.getQuerySql()**

找到第105-111行（构建分片查询 SQL 的逻辑），删除不需要的成员变量并简化逻辑。

修改 `startNewSplit` 方法：

```java
/**
 * 开始读取新分片
 */
private void startNewSplit(RangeSplit split) throws IOException {
    log.info("开始读取分片: {}", split.splitId());

    try {
        // 创建连接
        currentConnection = DriverManager.getConnection(url, username, password);
        currentStatement = currentConnection.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY
        );

        // 设置 fetchSize
        currentStatement.setFetchSize(batchSize);
        if (queryTimeout != null) {
            currentStatement.setQueryTimeout(queryTimeout);
        }

        // 直接使用分片中的查询 SQL
        String querySql = split.getQuerySql();
        log.debug("执行查询: {}", querySql);

        // 执行查询
        currentResultSet = currentStatement.executeQuery(querySql);
        hasNextRecord = currentResultSet.next();
        currentOffset = 0;
        currentSplit = split;

    } catch (SQLException e) {
        // 出错时关闭资源
        closeCurrentSplit();
        throw new IOException("读取分片失败: " + split.splitId(), e);
    }
}
```

删除不再需要的成员变量（第39行 `splitColumn`）：

```java
// 删除这行
private final String splitColumn;
```

修改构造函数，删除 splitColumn 的初始化：

```java
public JdbcSplitReader(JdbcSourceConfig config) {
    this.url = config.getUrl();
    this.username = config.getUsername();
    this.password = config.getPassword();
    this.table = config.getTable();
    this.sql = config.getSql();
    // 删除: this.splitColumn = config.getSplitColumn();
    this.batchSize = config.getBatchSize();
    this.queryTimeout = config.getQueryTimeout();
}
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn compile -pl flink-etl-source/flink-etl-source-jdbc -am -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitReader.java
git commit -m "refactor(jdbc-source): SplitReader 简化为直接执行分片 SQL"
```

---

## Task 9: 更新文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 更新 JDBC Source 配置参数表**

找到第36行，将 `splitColumn` 从"是"改为"否"：

```markdown
| `splitColumn` | 否 | - | 分片列名，支持数值类型（TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/DECIMAL）。不配置则使用单分片全表扫描 |
```

- [ ] **Step 2: 在配置示例后添加无 splitColumn 的示例**

在"自定义 SQL 查询"示例后添加：

```markdown
**无分片列配置（单线程全表扫描）：**

```json
{
  "sources": [{
    "type": "jdbc",
    "outputTable": "users",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "users",
      "batchSize": 1000
    }
  }]
}
```

> **注意：**
> - 未配置 `splitColumn` 时将使用单分片全表扫描模式，无法并行读取数据
> - 对于大数据量表，建议配置 `splitColumn` 以启用并行分片读取
> - `splitColumn` 仅支持数值类型（TINYINT, SMALLINT, INT, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC），配置非数值类型列会报错
```

- [ ] **Step 3: Commit**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 JDBC Source 文档，说明 splitColumn 可选和类型限制"
```

---

## Task 10: 集成测试验证

- [ ] **Step 1: 编译整个项目**

Run: `cd d:/work/idea/flink-etl-tool && mvn clean compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 2: 打包项目**

Run: `cd d:/work/idea/flink-etl-tool && mvn clean package -DskipTests -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 验证功能（可选，需要数据库环境）**

运行现有示例验证：
```bash
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/batch-mysql2console.json
```

Expected: 任务正常执行

---

## 验收标准

1. ✅ 不配置 splitColumn 时，程序正常启动，打印警告日志
2. ✅ 不配置 splitColumn 时，生成单个全表扫描分片
3. ✅ 配置 splitColumn 为数值类型时，按原逻辑进行范围分片
4. ✅ 配置 splitColumn 为非数值类型时，抛出明确的错误提示
5. ✅ RangeSplit 简化为直接存储查询 SQL
6. ✅ SplitStrategy 枚举预留扩展点，便于未来支持其他类型分片
7. ✅ 文档已更新，说明 splitColumn 为可选配置及类型限制
8. ✅ 编译通过，无新增编译错误

---

## 扩展性说明

`SplitStrategy` 枚举设计支持未来扩展：

```java
// 示例：未来支持字符串哈希分片
STRING_HASH("字符串哈希分片", new int[]{
    Types.CHAR,
    Types.VARCHAR,
    Types.LONGVARCHAR
}),

// 示例：未来支持日期范围分片
DATE_RANGE("日期范围分片", new int[]{
    Types.DATE,
    Types.TIMESTAMP,
    Types.TIMESTAMP_WITH_TIMEZONE
});
```

每个策略可以有自己的分片计算方法（在 JdbcSplitHelper 中添加对应的 `createXxxSplits` 方法）。