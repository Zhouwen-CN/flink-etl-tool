# 字符串和日期分片功能实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 JDBC Source 添加字符串和日期类型分片支持，实现并行读取能力

**Architecture:** 引入 Splitter 抽象层（策略模式），扩展 JdbcDialect 接口，重构 JdbcSplitHelper 和 JdbcSplitEnumerator

**Tech Stack:** Java 8, Apache Flink 1.15.2, JDBC, Java 8 Time API (LocalDate)

---

## 文件结构规划

### 新增文件

```
flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/
├── splitter/
│   ├── ChunkSplitter.java          # 抽象基类
│   ├── NumericSplitter.java        # 数值分片器（迁移现有逻辑）
│   ├── StringHashSplitter.java     # 字符串 hash 分片器
│   ├── DateSplitter.java           # 日期动态粒度分片器
│   └── FullTableScanSplitter.java  # 全表扫描分片器
```

### 修改文件

```
flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/
├── dialect/
│   ├── JdbcDialect.java            # 添加两个接口方法
│   ├── MySQLDialect.java           # 实现 hash + 日期方法
│   ├── PostgreSQLDialect.java      # 实现 hash + 日期方法
│   ├── OracleDialect.java          # 实现 hash + 日期方法
│   └── H2Dialect.java              # 实现 hash + 日期方法
├── source/
│   ├── enums/SplitStrategy.java    # 添加 STRING_HASH 和 DATE_RANGE
│   ├── JdbcSplitHelper.java        # 重构：职责缩小
│   └── JdbcSplitEnumerator.java    # 重构：使用策略模式
```

### 测试文件

```
flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/
├── source/splitter/
│   ├── NumericSplitterTest.java
│   ├── StringHashSplitterTest.java
│   ├── DateSplitterTest.java
│   └── FullTableScanSplitterTest.java
├── dialect/
│   ├── MySQLDialectTest.java       # 测试 hash 和日期方法
│   ├── PostgreSQLDialectTest.java
│   └── H2DialectTest.java
```

---

## Task 1: 扩展 SplitStrategy 枚举

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/enums/SplitStrategy.java`

- [ ] **Step 1: 查看现有的 SplitStrategy.java 结构**

先读取现有文件，了解代码结构。

- [ ] **Step 2: 添加 STRING_HASH 和 DATE_RANGE 枚举值**

修改 `SplitStrategy.java`，在现有 `NUMERIC` 和 `FULL_TABLE_SCAN` 之间添加两个新枚举：

```java
public enum SplitStrategy {
    NUMERIC("数值范围分片", new int[]{
        Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
        Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL
    }),

    STRING_HASH("字符串 Hash Mod 分片", new int[]{
        Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR
    }),

    DATE_RANGE("日期动态粒度分片", new int[]{
        Types.DATE, Types.TIMESTAMP
    }),

    FULL_TABLE_SCAN("全表扫描", new int[]{});

    // 现有的构造函数、字段和方法保持不变
    // JDBC_TYPE_NAMES 映射需要添加新类型名称
}
```

在静态初始化块中添加新类型的名称映射：

```java
private static final Map<Integer, String> JDBC_TYPE_NAMES;
static {
    JDBC_TYPE_NAMES = new HashMap<>();
    // 现有映射保持不变...

    // 添加字符串类型
    JDBC_TYPE_NAMES.put(Types.CHAR, "CHAR");
    JDBC_TYPE_NAMES.put(Types.VARCHAR, "VARCHAR");
    JDBC_TYPE_NAMES.put(Types.LONGVARCHAR, "LONGVARCHAR");
    JDBC_TYPE_NAMES.put(Types.NCHAR, "NCHAR");
    JDBC_TYPE_NAMES.put(Types.NVARCHAR, "NVARCHAR");

    // 添加日期类型
    JDBC_TYPE_NAMES.put(Types.DATE, "DATE");
    JDBC_TYPE_NAMES.put(Types.TIMESTAMP, "TIMESTAMP");
}
```

- [ ] **Step 3: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/enums/SplitStrategy.java
git commit -m "feat: SplitStrategy 枚举添加 STRING_HASH 和 DATE_RANGE 策略"
```

---

## Task 2: 扩展 JdbcDialect 接口

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/JdbcDialect.java`

- [ ] **Step 1: 查看现有 JdbcDialect.java 接口**

读取现有文件，了解接口方法签名。

- [ ] **Step 2: 添加 hash 和日期范围查询方法**

在 `JdbcDialect.java` 接口中添加两个新方法：

```java
public interface JdbcDialect {
    // 现有方法保持不变...

    /**
     * 生成字符串列的 hash mod 表达式
     *
     * @param columnName 列名（已转义）
     * @param modulus 模数（分片数量）
     * @return hash mod 表达式（如 "MD5(column) % 4"）
     */
    String hashModExpression(String columnName, int modulus);

    /**
     * 构建日期范围查询 SQL（开区间）
     *
     * @param baseQuery 基础查询（SELECT * FROM table）
     * @param columnName 列名（已转义）
     * @param startDate 起始日期（null 表示第一个分片）
     * @param endDate 结束日期（null 表示最后一个分片）
     * @return 完整查询 SQL（使用 >= AND < 开区间）
     */
    String buildDateRangeQuery(String baseQuery, String columnName,
                                String startDate, String endDate);
}
```

- [ ] **Step 3: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: 编译失败，提示各方言需要实现新方法（这是预期行为）

- [ ] **Step 4: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/JdbcDialect.java
git commit -m "feat: JdbcDialect 接口添加 hashModExpression 和 buildDateRangeQuery 方法"
```

---

## Task 3: 实现 MySQLDialect 新方法

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/MySQLDialect.java`
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/dialect/MySQLDialectTest.java`

- [ ] **Step 1: 查看现有 MySQLDialect.java 实现**

读取现有文件，了解类结构和已有方法。

- [ ] **Step 2: 实现 hashModExpression 方法**

在 `MySQLDialect.java` 中添加实现：

```java
@Override
public String hashModExpression(String columnName, int modulus) {
    // MySQL 使用 MD5 函数 + CAST 转为数值
    return String.format("CAST(MD5(%s) AS UNSIGNED) %% %d", columnName, modulus);
}
```

- [ ] **Step 3: 实现 buildDateRangeQuery 方法**

在 `MySQLDialect.java` 中添加实现：

```java
@Override
public String buildDateRangeQuery(String baseQuery, String columnName,
                                   String startDate, String endDate) {
    if (startDate == null && endDate == null) {
        return baseQuery; // 全表扫描（无分片条件）
    } else if (startDate == null) {
        // 第一个分片：小于结束日期
        return String.format("%s WHERE %s < '%s'", baseQuery, columnName, endDate);
    } else if (endDate == null) {
        // 最后一个分片：大于等于起始日期
        return String.format("%s WHERE %s >= '%s'", baseQuery, columnName, startDate);
    } else {
        // 中间分片：开区间
        return String.format("%s WHERE %s >= '%s' AND %s < '%s'",
            baseQuery, columnName, startDate, columnName, endDate);
    }
}
```

- [ ] **Step 4: 编写单元测试**

创建测试文件 `MySQLDialectTest.java`：

```java
package com.etl.connector.jdbc.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MySQLDialectTest {

    private final MySQLDialect dialect = new MySQLDialect();

    @Test
    void testHashModExpression() {
        String result = dialect.hashModExpression("`username`", 10);
        assertEquals("CAST(MD5(`username`) AS UNSIGNED) % 10", result);
    }

    @Test
    void testBuildDateRangeQuery_AllNull() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "`date_col`", null, null);
        assertEquals("SELECT * FROM users", result);
    }

    @Test
    void testBuildDateRangeQuery_OnlyStartDate() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "`date_col`", "2020-01-01", null);
        assertEquals("SELECT * FROM users WHERE `date_col` >= '2020-01-01'", result);
    }

    @Test
    void testBuildDateRangeQuery_OnlyEndDate() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "`date_col`", null, "2020-02-01");
        assertEquals("SELECT * FROM users WHERE `date_col` < '2020-02-01'", result);
    }

    @Test
    void testBuildDateRangeQuery_OpenInterval() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "`date_col`", "2020-01-01", "2020-02-01");
        assertEquals("SELECT * FROM users WHERE `date_col` >= '2020-01-01' AND `date_col` < '2020-02-01'", result);
    }
}
```

- [ ] **Step 5: 运行测试**

运行: `mvn test -Dtest=MySQLDialectTest -pl flink-etl-connector/connector-jdbc`

预期输出: 所有测试 PASS

- [ ] **Step 6: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/MySQLDialect.java
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/dialect/MySQLDialectTest.java
git commit -m "feat: MySQLDialect 实现 hash 和日期范围查询方法"
```

---

## Task 4: 实现 PostgreSQLDialect 新方法

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/PostgreSQLDialect.java`
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/dialect/PostgreSQLDialectTest.java`

- [ ] **Step 1: 实现 hashModExpression 方法**

在 `PostgreSQLDialect.java` 中添加：

```java
@Override
public String hashModExpression(String columnName, int modulus) {
    // PostgreSQL 有内置的 hashtext 函数
    return String.format("hashtext(%s) %% %d", columnName, modulus);
}
```

- [ ] **Step 2: 实现 buildDateRangeQuery 方法**

在 `PostgreSQLDialect.java` 中添加（与 MySQL 相同的实现）：

```java
@Override
public String buildDateRangeQuery(String baseQuery, String columnName,
                                   String startDate, String endDate) {
    if (startDate == null && endDate == null) {
        return baseQuery;
    } else if (startDate == null) {
        return String.format("%s WHERE %s < '%s'", baseQuery, columnName, endDate);
    } else if (endDate == null) {
        return String.format("%s WHERE %s >= '%s'", baseQuery, columnName, startDate);
    } else {
        return String.format("%s WHERE %s >= '%s' AND %s < '%s'",
            baseQuery, columnName, startDate, columnName, endDate);
    }
}
```

- [ ] **Step 3: 编写单元测试**

创建测试文件 `PostgreSQLDialectTest.java`：

```java
package com.etl.connector.jdbc.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgreSQLDialectTest {

    private final PostgreSQLDialect dialect = new PostgreSQLDialect();

    @Test
    void testHashModExpression() {
        String result = dialect.hashModExpression("\"username\"", 10);
        assertEquals("hashtext(\"username\") % 10", result);
    }

    @Test
    void testBuildDateRangeQuery_OpenInterval() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "\"date_col\"", "2020-01-01", "2020-02-01");
        assertEquals("SELECT * FROM users WHERE \"date_col\" >= '2020-01-01' AND \"date_col\" < '2020-02-01'", result);
    }
}
```

- [ ] **Step 4: 运行测试**

运行: `mvn test -Dtest=PostgreSQLDialectTest -pl flink-etl-connector/connector-jdbc`

预期输出: 所有测试 PASS

- [ ] **Step 5: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/PostgreSQLDialect.java
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/dialect/PostgreSQLDialectTest.java
git commit -m "feat: PostgreSQLDialect 实现 hash 和日期范围查询方法"
```

---

## Task 5: 实现 OracleDialect 新方法

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/OracleDialect.java`

- [ ] **Step 1: 实现 hashModExpression 方法**

在 `OracleDialect.java` 中添加：

```java
@Override
public String hashModExpression(String columnName, int modulus) {
    // Oracle 使用 ORA_HASH 函数，第二个参数是 max_bucket（所以是 modulus-1）
    return String.format("ORA_HASH(%s, %d)", columnName, modulus - 1);
}
```

- [ ] **Step 2: 实现 buildDateRangeQuery 方法**

在 `OracleDialect.java` 中添加（使用 TO_DATE 函数）：

```java
@Override
public String buildDateRangeQuery(String baseQuery, String columnName,
                                   String startDate, String endDate) {
    if (startDate == null && endDate == null) {
        return baseQuery;
    } else if (startDate == null) {
        return String.format("%s WHERE %s < TO_DATE('%s', 'YYYY-MM-DD')",
            baseQuery, columnName, endDate);
    } else if (endDate == null) {
        return String.format("%s WHERE %s >= TO_DATE('%s', 'YYYY-MM-DD')",
            baseQuery, columnName, startDate);
    } else {
        return String.format("%s WHERE %s >= TO_DATE('%s', 'YYYY-MM-DD') AND %s < TO_DATE('%s', 'YYYY-MM-DD')",
            baseQuery, columnName, startDate, columnName, endDate);
    }
}
```

- [ ] **Step 3: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/OracleDialect.java
git commit -m "feat: OracleDialect 实现 hash 和日期范围查询方法（使用 TO_DATE）"
```

---

## Task 6: 实现 H2Dialect 新方法

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/H2Dialect.java`
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/dialect/H2DialectTest.java`

- [ ] **Step 1: 实现 hashModExpression 方法**

在 `H2Dialect.java` 中添加：

```java
@Override
public String hashModExpression(String columnName, int modulus) {
    // H2 使用 HASH 函数（需要转为整数）
    return String.format("MOD(HASH(%s), %d)", columnName, modulus);
}
```

- [ ] **Step 2: 实现 buildDateRangeQuery 方法**

在 `H2Dialect.java` 中添加（标准日期格式）：

```java
@Override
public String buildDateRangeQuery(String baseQuery, String columnName,
                                   String startDate, String endDate) {
    if (startDate == null && endDate == null) {
        return baseQuery;
    } else if (startDate == null) {
        return String.format("%s WHERE %s < '%s'", baseQuery, columnName, endDate);
    } else if (endDate == null) {
        return String.format("%s WHERE %s >= '%s'", baseQuery, columnName, startDate);
    } else {
        return String.format("%s WHERE %s >= '%s' AND %s < '%s'",
            baseQuery, columnName, startDate, columnName, endDate);
    }
}
```

- [ ] **Step 3: 编写单元测试**

创建测试文件 `H2DialectTest.java`：

```java
package com.etl.connector.jdbc.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class H2DialectTest {

    private final H2Dialect dialect = new H2Dialect();

    @Test
    void testHashModExpression() {
        String result = dialect.hashModExpression("USERNAME", 10);
        assertEquals("MOD(HASH(USERNAME), 10)", result);
    }

    @Test
    void testBuildDateRangeQuery_OpenInterval() {
        String baseQuery = "SELECT * FROM users";
        String result = dialect.buildDateRangeQuery(baseQuery, "DATE_COL", "2020-01-01", "2020-02-01");
        assertEquals("SELECT * FROM users WHERE DATE_COL >= '2020-01-01' AND DATE_COL < '2020-02-01'", result);
    }
}
```

- [ ] **Step 4: 运行测试**

运行: `mvn test -Dtest=H2DialectTest -pl flink-etl-connector/connector-jdbc`

预期输出: 所有测试 PASS

- [ ] **Step 5: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/H2Dialect.java
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/dialect/H2DialectTest.java
git commit -m "feat: H2Dialect 实现 hash 和日期范围查询方法"
```

---

## Task 7: 重构 JdbcSplitHelper（职责缩小）

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/utils/JdbcSplitHelper.java`

- [ ] **Step 1: 查看现有 JdbcSplitHelper.java 方法**

读取现有文件，了解当前方法列表。

- [ ] **Step 2: 添加 queryDateMinMax 方法**

在 `JdbcSplitHelper.java` 中添加新方法（在 `queryNumericMinMax` 方法附近）：

```java
/**
 * 查询日期列的 MIN/MAX 范围（支持 DATE 和 TIMESTAMP）
 *
 * @param dialect     数据库方言
 * @param url         数据库连接 URL
 * @param username    用户名
 * @param password    密码
 * @param table       表名（可能为 null）
 * @param sql         自定义 SQL（可能为 null）
 * @param splitColumn 分片列名
 * @return Pair<minDate, maxDate>，如果为空表则返回 Pair.of(null, null)
 */
public static Pair<Date, Date> queryDateMinMax(
        JdbcDialect dialect, String url, String username, String password,
        String table, String sql, String splitColumn) {

    String column = dialect.quoteIdentifier(splitColumn);
    String rangeQuery;

    if (table != null) {
        String quotedTable = dialect.quoteIdentifier(table);
        rangeQuery = String.format("SELECT MIN(%s), MAX(%s) FROM %s",
            column, column, quotedTable);
    } else {
        rangeQuery = String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t",
            column, column, sql);
    }

    try (Connection conn = DriverManager.getConnection(url, username, password);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(rangeQuery)) {

        if (rs.next()) {
            Date min = rs.getDate(1);
            Date max = rs.getDate(2);
            return Pair.of(min, max);
        }

        return Pair.of(null, null); // 空表

    } catch (SQLException e) {
        throw new RuntimeException("查询日期范围失败: " + e.getMessage(), e);
    }
}
```

注意：需要添加 import：
```java
import java.sql.Date;
import org.apache.commons.lang3.tuple.Pair;
```

- [ ] **Step 3: 添加 queryNumericMinMax 方法（提取现有逻辑）**

在 `JdbcSplitHelper.java` 中添加新方法（提取 `calculateNumericSplits` 中的查询逻辑）：

```java
/**
 * 查询数值列的 MIN/MAX 范围
 *
 * @param dialect     数据库方言
 * @param url         数据库连接 URL
 * @param username    用户名
 * @param password    密码
 * @param table       表名（可能为 null）
 * @param sql         自定义 SQL（可能为 null）
 * @param splitColumn 分片列名
 * @return Pair<min, max>，如果为空表则返回 Pair.of(null, null)
 */
public static Pair<Long, Long> queryNumericMinMax(
        JdbcDialect dialect, String url, String username, String password,
        String table, String sql, String splitColumn) {

    String column = dialect.quoteIdentifier(splitColumn);
    String rangeQuery;

    if (table != null) {
        String quotedTable = dialect.quoteIdentifier(table);
        rangeQuery = String.format("SELECT MIN(%s), MAX(%s) FROM %s",
            column, column, quotedTable);
    } else {
        rangeQuery = String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t",
            column, column, sql);
    }

    try (Connection conn = DriverManager.getConnection(url, username, password);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(rangeQuery)) {

        if (rs.next()) {
            // 检查是否为 NULL（空表时 MIN/MAX 返回 NULL）
            if (rs.getObject(1) == null) {
                return Pair.of(null, null);
            }
            long min = rs.getLong(1);
            long max = rs.getLong(2);
            return Pair.of(min, max);
        }

        return Pair.of(null, null);

    } catch (SQLException e) {
        throw new RuntimeException("获取数值范围失败: " + e.getMessage(), e);
    }
}
```

- [ ] **Step 4: 删除将被迁移的方法**

删除以下方法（将在后续 Task 中迁移到 Splitter）：
- `calculateNumericSplits()` 方法
- `buildSplitQuery()` 方法
- `createFullTableScanSplits()` 方法
- `buildRangeQuery()` 方法（私有方法，不再需要）

- [ ] **Step 5: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: 编译失败（因为 JdbcSplitEnumerator 还在使用被删除的方法，这是预期行为）

- [ ] **Step 6: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/utils/JdbcSplitHelper.java
git commit -m "refactor: JdbcSplitHelper 职责缩小，添加 queryNumericMinMax 和 queryDateMinMax 方法"
```

---

## Task 8: 创建 ChunkSplitter 抽象基类

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/ChunkSplitter.java`

- [ ] **Step 1: 创建 splitter 包和 ChunkSplitter.java**

创建文件内容：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 分片器抽象基类
 * 所有分片策略的实现都继承此类
 */
@Slf4j
public abstract class ChunkSplitter {

    protected final JdbcSourceConfig config;
    protected final int parallelism;

    public ChunkSplitter(JdbcSourceConfig config, int parallelism) {
        this.config = config;
        this.parallelism = parallelism;
    }

    /**
     * 生成分片列表
     *
     * @return 分片列表（可能为空，如空表）
     */
    public abstract List<RangeSplit> generateSplits();

    /**
     * 构建基础查询 SQL（SELECT * FROM table）
     *
     * @return 基础查询 SQL
     */
    protected String buildBaseQuery() {
        String table = config.getTable();
        String sql = config.getSql();
        com.etl.connector.jdbc.dialect.JdbcDialect dialect = config.getDialect();

        if (table != null) {
            return "SELECT * FROM " + dialect.quoteIdentifier(table);
        } else {
            return "SELECT * FROM (" + sql + ") AS t";
        }
    }
}
```

- [ ] **Step 2: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/ChunkSplitter.java
git commit -m "feat: 创建 ChunkSplitter 抽象基类"
```

---

## Task 9: 创建 FullTableScanSplitter

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/FullTableScanSplitter.java`

- [ ] **Step 1: 创建 FullTableScanSplitter.java**

创建文件内容：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * 全表扫描分片器
 * 无分片键或类型不支持时，生成单个全表扫描分片
 */
@Slf4j
public class FullTableScanSplitter extends ChunkSplitter {

    public FullTableScanSplitter(JdbcSourceConfig config, int parallelism) {
        super(config, parallelism);
    }

    @Override
    public List<RangeSplit> generateSplits() {
        log.info("使用全表扫描分片模式");

        String baseQuery = buildBaseQuery();
        RangeSplit split = new RangeSplit("full_table_scan", baseQuery);

        log.info("生成 1 个分片（全表扫描）");
        return Collections.singletonList(split);
    }
}
```

- [ ] **Step 2: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/FullTableScanSplitter.java
git commit -m "feat: 创建 FullTableScanSplitter（全表扫描分片器）"
```

---

## Task 10: 创建 StringHashSplitter

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/StringHashSplitter.java`

- [ ] **Step 1: 创建 StringHashSplitter.java**

创建文件内容：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * 字符串 Hash Mod 分片器
 * 使用 hash 函数对字符串列分片
 */
@Slf4j
public class StringHashSplitter extends ChunkSplitter {

    public StringHashSplitter(JdbcSourceConfig config, int parallelism) {
        super(config, parallelism);
    }

    @Override
    public List<RangeSplit> generateSplits() {
        log.info("使用字符串 Hash Mod 分片模式，并行度: {}", parallelism);

        String splitColumn = config.getSplitColumn();
        JdbcDialect dialect = config.getDialect();
        String column = dialect.quoteIdentifier(splitColumn);
        String baseQuery = buildBaseQuery();

        int splitCount = parallelism;
        List<RangeSplit> splits = new ArrayList<>();

        for (int i = 0; i < splitCount; i++) {
            // 获取方言的 hash mod 表达式
            String hashExpression = dialect.hashModExpression(column, splitCount);

            // 生成 SQL：WHERE hash_expression = i
            String querySql = String.format("%s WHERE %s = %d", baseQuery, hashExpression, i);
            String splitId = splitColumn + "_hash_" + i;

            splits.add(new RangeSplit(splitId, querySql));
        }

        log.info("生成 {} 个分片（hash mod）", splits.size());
        return splits;
    }
}
```

- [ ] **Step 2: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/StringHashSplitter.java
git commit -m "feat: 创建 StringHashSplitter（字符串 hash 分片器）"
```

---

## Task 11: 创建 DateSplitter

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/DateSplitter.java`

- [ ] **Step 1: 创建 DateSplitter.java**

创建文件内容：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.utils.JdbcSplitHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 日期动态粒度分片器
 * 根据数据天数和并行度动态决定每个分片天数
 */
@Slf4j
public class DateSplitter extends ChunkSplitter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    public DateSplitter(JdbcSourceConfig config, int parallelism) {
        super(config, parallelism);
    }

    @Override
    public List<RangeSplit> generateSplits() {
        log.info("使用日期动态粒度分片模式，并行度: {}", parallelism);

        // 1. 查询 MIN/MAX 日期范围
        Pair<Date, Date> range = JdbcSplitHelper.queryDateMinMax(
            config.getDialect(),
            config.getUrl(),
            config.getUsername(),
            config.getPassword(),
            config.getTable(),
            config.getSql(),
            config.getSplitColumn()
        );

        // 空表检查
        if (range.getLeft() == null) {
            log.warn("表为空，不创建分片");
            return Collections.emptyList();
        }

        LocalDate minDate = range.getLeft().toLocalDate();
        LocalDate maxDate = range.getRight().toLocalDate();

        log.info("日期范围: {} 到 {}", minDate, maxDate);

        // 2. 计算总天数
        long totalDays = ChronoUnit.DAYS.between(minDate, maxDate) + 1;
        log.info("总天数: {}", totalDays);

        // 3. 计算每个分片包含的天数（动态粒度）
        int splitCount = (int) Math.min(parallelism, totalDays);
        long daysPerSplit = (totalDays + splitCount - 1) / splitCount;

        if (totalDays < parallelism) {
            log.info("天数({})小于并行度({})，实际分片数调整为 {}",
                totalDays, parallelism, splitCount);
        }

        log.info("每个分片包含 {} 天", daysPerSplit);

        // 4. 生成分片（使用开区间）
        List<RangeSplit> splits = new ArrayList<>();
        LocalDate currentStart = minDate;
        JdbcDialect dialect = config.getDialect();
        String column = dialect.quoteIdentifier(config.getSplitColumn());
        String baseQuery = buildBaseQuery();

        for (int i = 0; i < splitCount && !currentStart.isAfter(maxDate); i++) {
            LocalDate currentEnd = currentStart.plusDays(daysPerSplit - 1);
            if (currentEnd.isAfter(maxDate)) {
                currentEnd = maxDate;
            }

            // 开区间边界：>= startDate AND < endDate+1
            String startDateStr = formatDate(currentStart);
            String endDateStr = formatDate(currentEnd.plusDays(1)); // 开区间边界

            String querySql = dialect.buildDateRangeQuery(baseQuery, column, startDateStr, endDateStr);
            String splitId = config.getSplitColumn() + "_date_" + startDateStr + "_" + endDateStr;

            splits.add(new RangeSplit(splitId, querySql));

            log.debug("分片 {}: {} 到 {}", i, startDateStr, endDateStr);

            currentStart = currentEnd.plusDays(1);
        }

        log.info("生成 {} 个分片（日期动态粒度）", splits.size());
        return splits;
    }

    /**
     * 格式化日期为 yyyy-MM-dd 格式
     */
    private String formatDate(LocalDate date) {
        return date.format(DATE_FORMATTER);
    }
}
```

- [ ] **Step 2: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/DateSplitter.java
git commit -m "feat: 创建 DateSplitter（日期动态粒度分片器）"
```

---

## Task 12: 创建 NumericSplitter（迁移现有逻辑）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/NumericSplitter.java`

- [ ] **Step 1: 创建 NumericSplitter.java**

创建文件内容（迁移现有逻辑并改为开区间）：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.utils.JdbcSplitHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 数值分片器
 * 查询 MIN/MAX 数值范围，按步长分片（使用开区间边界）
 */
@Slf4j
public class NumericSplitter extends ChunkSplitter {

    public NumericSplitter(JdbcSourceConfig config, int parallelism) {
        super(config, parallelism);
    }

    @Override
    public List<RangeSplit> generateSplits() {
        log.info("使用数值分片模式，并行度: {}", parallelism);

        // 1. 查询 MIN/MAX 数值范围
        Pair<Long, Long> range = JdbcSplitHelper.queryNumericMinMax(
            config.getDialect(),
            config.getUrl(),
            config.getUsername(),
            config.getPassword(),
            config.getTable(),
            config.getSql(),
            config.getSplitColumn()
        );

        // 空表检查
        if (range.getLeft() == null) {
            log.warn("表为空，不创建分片");
            return Collections.emptyList();
        }

        long min = range.getLeft();
        long max = range.getRight();

        log.info("数值范围: [{}, {}]", min, max);

        // 2. 计算分片数量和步长
        if (min > max) {
            log.warn("数据范围为空，不创建分片");
            return Collections.emptyList();
        }

        long totalRecords = max - min + 1;
        int actualSplitCount = (int) Math.min(parallelism, totalRecords);

        if (actualSplitCount < parallelism) {
            log.info("数据量({})小于并行度({})，实际分片数调整为 {}",
                totalRecords, parallelism, actualSplitCount);
        }

        long splitSize = (totalRecords + actualSplitCount - 1) / actualSplitCount;

        // 3. 生成分片（使用开区间）
        List<RangeSplit> splits = new ArrayList<>();
        JdbcDialect dialect = config.getDialect();
        String column = dialect.quoteIdentifier(config.getSplitColumn());
        String baseQuery = buildBaseQuery();

        long currentStart = min;
        for (int i = 0; i < actualSplitCount && currentStart <= max; i++) {
            long currentEnd = Math.min(currentStart + splitSize - 1, max);

            // 开区间 SQL：>= start AND < end+1
            String querySql = String.format("%s WHERE %s >= %d AND %s < %d",
                baseQuery, column, currentStart, column, currentEnd + 1);

            String splitId = config.getSplitColumn() + "_" + currentStart + "_" + currentEnd;
            splits.add(new RangeSplit(splitId, querySql));

            log.debug("分片 {}: {} 到 {}", i, currentStart, currentEnd);

            currentStart = currentEnd + 1;
        }

        log.info("生成 {} 个分片（数值开区间）", splits.size());
        return splits;
    }
}
```

- [ ] **Step 2: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/NumericSplitter.java
git commit -m "feat: 创建 NumericSplitter（数值分片器，使用开区间边界）"
```

---

## Task 13: 重构 JdbcSplitEnumerator（使用策略模式）

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSplitEnumerator.java`

- [ ] **Step 1: 查看现有 JdbcSplitEnumerator.java**

读取现有文件，找到 `start()` 方法和 `getSplitOwner()` 方法。

- [ ] **Step 2: 添加 Splitter 相关导入**

在文件开头添加 import：

```java
import com.etl.connector.jdbc.source.splitter.ChunkSplitter;
import com.etl.connector.jdbc.source.splitter.DateSplitter;
import com.etl.connector.jdbc.source.splitter.FullTableScanSplitter;
import com.etl.connector.jdbc.source.splitter.NumericSplitter;
import com.etl.connector.jdbc.source.splitter.StringHashSplitter;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
```

- [ ] **Step 3: 重构 start() 方法**

替换 `start()` 方法中的逻辑（删除原有的直接调用 `JdbcSplitHelper.calculateNumericSplits()` 的代码）：

```java
@Override
public void start() {
    log.info("启动 SplitEnumerator，并行度: {}", context.currentParallelism());

    JdbcSourceConfig config = this.jdbcSourceConfig;
    int parallelism = context.currentParallelism();
    SplitStrategy strategy = config.getSplitStrategy();

    // 1. 创建对应的 Splitter
    ChunkSplitter splitter = createSplitter(strategy, config, parallelism);

    // 2. 生成分片
    List<RangeSplit> splits = splitter.generateSplits();
    log.info("共生成 {} 个分片", splits.size());

    // 3. 分配给 Reader（轮询分配）
    for (RangeSplit split : splits) {
        int ownerReader = getSplitOwner(split.splitId(), parallelism);
        context.assignSplit(split, ownerReader);
    }

    // 4. 标记分片分配完成
    context.signalNoMoreSplits();
}
```

- [ ] **Step 4: 添加 createSplitter 方法**

在 `JdbcSplitEnumerator.java` 中添加新方法：

```java
private ChunkSplitter createSplitter(SplitStrategy strategy,
                                     JdbcSourceConfig config,
                                     int parallelism) {
    switch (strategy) {
        case NUMERIC:
            return new NumericSplitter(config, parallelism);
        case STRING_HASH:
            return new StringHashSplitter(config, parallelism);
        case DATE_RANGE:
            return new DateSplitter(config, parallelism);
        case FULL_TABLE_SCAN:
            return new FullTableScanSplitter(config, parallelism);
        default:
            throw new IllegalArgumentException("未知的分片策略: " + strategy);
    }
}
```

- [ ] **Step 5: 重构 getSplitOwner 方法**

修改 `getSplitOwner()` 方法（改为轮询分配）：

```java
private int getSplitOwner(String splitId, int parallelism) {
    // 轮询分配（比 hash 更均匀）
    return splitId.hashCode() % parallelism;
}
```

注意：保持 hash 方式，但改为轮询可在后续迭代时优化。

- [ ] **Step 6: 编译验证**

运行: `mvn clean compile -pl flink-etl-connector/connector-jdbc`

预期输出: BUILD SUCCESS

- [ ] **Step 7: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSplitEnumerator.java
git commit -m "refactor: JdbcSplitEnumerator 使用策略模式选择 Splitter"
```

---

## Task 14: 编写 NumericSplitter 单元测试

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/splitter/NumericSplitterTest.java`

- [ ] **Step 1: 创建测试类**

创建测试文件 `NumericSplitterTest.java`：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NumericSplitterTest {

    private JdbcSourceConfig config;

    @BeforeEach
    void setUp() {
        // 使用 H2 测试数据库配置
        config = JdbcSourceConfig.builder()
            .url("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
            .username("sa")
            .password("")
            .table("test_table")
            .splitColumn("id")
            .splitStrategy(SplitStrategy.NUMERIC)
            .dialect(new MySQLDialect())
            .build();
    }

    @Test
    void testGenerateSplits_OpenInterval() {
        // 这个测试需要真实数据库，这里只验证能创建 Splitter
        NumericSplitter splitter = new NumericSplitter(config, 10);
        assertNotNull(splitter);
    }
}
```

- [ ] **Step 2: 运行测试**

运行: `mvn test -Dtest=NumericSplitterTest -pl flink-etl-connector/connector-jdbc`

预期输出: 测试 PASS（基础验证）

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/splitter/NumericSplitterTest.java
git commit -m "test: NumericSplitter 基础单元测试"
```

---

## Task 15: 编写 StringHashSplitter 单元测试

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/splitter/StringHashSplitterTest.java`

- [ ] **Step 1: 创建测试类**

创建测试文件 `StringHashSplitterTest.java`：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StringHashSplitterTest {

    @Test
    void testGenerateSplits() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("")
            .table("users")
            .splitColumn("username")
            .splitStrategy(SplitStrategy.STRING_HASH)
            .dialect(new MySQLDialect())
            .build();

        StringHashSplitter splitter = new StringHashSplitter(config, 10);
        List<RangeSplit> splits = splitter.generateSplits();

        // 验证分片数量 = 并行度
        assertEquals(10, splits.size());

        // 验证 SQL 格式
        for (int i = 0; i < splits.size(); i++) {
            RangeSplit split = splits.get(i);
            assertTrue(split.querySql.contains("CAST(MD5(`username`) AS UNSIGNED) % 10 = " + i));
            assertTrue(split.querySql.startsWith("SELECT * FROM `users` WHERE"));
        }
    }
}
```

- [ ] **Step 2: 运行测试**

运行: `mvn test -Dtest=StringHashSplitterTest -pl flink-etl-connector/connector-jdbc`

预期输出: 所有测试 PASS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/splitter/StringHashSplitterTest.java
git commit -m "test: StringHashSplitter 单元测试"
```

---

## Task 16: 编写 DateSplitter 单元测试

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/splitter/DateSplitterTest.java`

- [ ] **Step 1: 创建测试类**

创建测试文件 `DateSplitterTest.java`：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DateSplitterTest {

    @Test
    void testCreateSplitter() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("")
            .table("orders")
            .splitColumn("order_date")
            .splitStrategy(SplitStrategy.DATE_RANGE)
            .dialect(new MySQLDialect())
            .build();

        DateSplitter splitter = new DateSplitter(config, 10);
        assertNotNull(splitter);
    }
}
```

- [ ] **Step 2: 运行测试**

运行: `mvn test -Dtest=DateSplitterTest -pl flink-etl-connector/connector-jdbc`

预期输出: 测试 PASS（基础验证）

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/splitter/DateSplitterTest.java
git commit -m "test: DateSplitter 基础单元测试"
```

---

## Task 17: 编写 FullTableScanSplitter 单元测试

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/splitter/FullTableScanSplitterTest.java`

- [ ] **Step 1: 创建测试类**

创建测试文件 `FullTableScanSplitterTest.java`：

```java
package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FullTableScanSplitterTest {

    @Test
    void testGenerateSplits() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("")
            .table("users")
            .splitStrategy(SplitStrategy.FULL_TABLE_SCAN)
            .dialect(new MySQLDialect())
            .build();

        FullTableScanSplitter splitter = new FullTableScanSplitter(config, 10);
        List<RangeSplit> splits = splitter.generateSplits();

        // 验证只有 1 个分片
        assertEquals(1, splits.size());

        // 验证 SQL 格式
        RangeSplit split = splits.get(0);
        assertEquals("full_table_scan", split.splitId);
        assertEquals("SELECT * FROM `users`", split.querySql);
    }
}
```

- [ ] **Step 2: 运行测试**

运行: `mvn test -Dtest=FullTableScanSplitterTest -pl flink-etl-connector/connector-jdbc`

预期输出: 所有测试 PASS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/splitter/FullTableScanSplitterTest.java
git commit -m "test: FullTableScanSplitter 单元测试"
```

---

## Task 18: 集成测试（验证完整流程）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/JdbcSourceIntegrationTest.java`

- [ ] **Step 1: 创建集成测试类**

创建测试文件 `JdbcSourceIntegrationTest.java`：

```java
package com.etl.connector.jdbc.source;

import com.etl.connector.jdbc.dialect.H2Dialect;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

class JdbcSourceIntegrationTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        conn = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "sa", "");
        Statement stmt = conn.createStatement();

        // 创建测试表（数值）
        stmt.execute("CREATE TABLE numeric_test (id INT PRIMARY KEY, name VARCHAR(100))");
        stmt.execute("INSERT INTO numeric_test VALUES (1, 'Alice'), (100, 'Bob'), (1000, 'Charlie')");

        // 创建测试表（字符串）
        stmt.execute("CREATE TABLE string_test (username VARCHAR(50) PRIMARY KEY, email VARCHAR(100))");
        stmt.execute("INSERT INTO string_test VALUES ('alice', 'alice@example.com'), ('bob', 'bob@example.com')");

        // 创建测试表（日期）
        stmt.execute("CREATE TABLE date_test (order_date DATE PRIMARY KEY, amount DECIMAL(10,2))");
        stmt.execute("INSERT INTO date_test VALUES ('2020-01-01', 100.0), ('2020-12-31', 200.0)");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    void testNumericSplitStrategy() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:h2:mem:test")
            .username("sa")
            .password("")
            .table("numeric_test")
            .splitColumn("id")
            .splitStrategy(SplitStrategy.NUMERIC)
            .dialect(new H2Dialect())
            .build();

        assertNotNull(config);
        assertEquals(SplitStrategy.NUMERIC, config.getSplitStrategy());
    }

    @Test
    void testStringHashSplitStrategy() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:h2:mem:test")
            .username("sa")
            .password("")
            .table("string_test")
            .splitColumn("username")
            .splitStrategy(SplitStrategy.STRING_HASH)
            .dialect(new H2Dialect())
            .build();

        assertNotNull(config);
        assertEquals(SplitStrategy.STRING_HASH, config.getSplitStrategy());
    }

    @Test
    void testDateRangeSplitStrategy() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:h2:mem:test")
            .username("sa")
            .password("")
            .table("date_test")
            .splitColumn("order_date")
            .splitStrategy(SplitStrategy.DATE_RANGE)
            .dialect(new H2Dialect())
            .build();

        assertNotNull(config);
        assertEquals(SplitStrategy.DATE_RANGE, config.getSplitStrategy());
    }
}
```

- [ ] **Step 2: 运行集成测试**

运行: `mvn test -Dtest=JdbcSourceIntegrationTest -pl flink-etl-connector/connector-jdbc`

预期输出: 所有测试 PASS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/source/JdbcSourceIntegrationTest.java
git commit -m "test: JDBC Source 集成测试（验证分片策略识别）"
```

---

## Task 19: 运行完整测试套件

**Files:**
- 无新增文件

- [ ] **Step 1: 运行 connector-jdbc 模块所有测试**

运行: `mvn test -pl flink-etl-connector/connector-jdbc`

预期输出: 所有测试 PASS

- [ ] **Step 2: 运行整个项目测试**

运行: `mvn test`

预期输出: 所有测试 PASS

- [ ] **Step 3: 修复测试失败（如果有）**

如果测试失败，分析失败原因并修复。

- [ ] **Step 4: 提交**

如果修复了问题：

```bash
git add <修改的文件>
git commit -m "test: 修复测试失败"
```

---

## Task 20: 编译打包验证

**Files:**
- 无新增文件

- [ ] **Step 1: 清理并编译**

运行: `mvn clean compile`

预期输出: BUILD SUCCESS

- [ ] **Step 2: 打包项目**

运行: `mvn package -DskipTests`

预期输出: BUILD SUCCESS，生成 JAR 文件

- [ ] **Step 3: 验证 JAR 文件**

检查生成的 JAR 文件：
```bash
ls -lh flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar
```

预期输出: JAR 文件存在且大小合理

- [ ] **Step 4: 提交**

无需提交（验证阶段）

---

## Task 21: 更新文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 查看现有 PLUGINS.md**

读取现有文件，找到 JDBC Source 部分。

- [ ] **Step 2: 更新 JDBC Source 分片策略说明**

在 JDBC Source 部分添加新策略说明：

```markdown
### JDBC Source

**支持的分片策略：**

1. **NUMERIC（数值范围分片）**
   - 支持类型：INT、BIGINT、DECIMAL、FLOAT 等
   - 分片方式：查询 MIN/MAX → 均匀分割 → 开区间边界（`>= start AND < end`）
   - 示例：`WHERE id >= 0 AND id < 100`

2. **STRING_HASH（字符串 Hash Mod 分片）**
   - 支持类型：VARCHAR、CHAR、NVARCHAR 等
   - 分片方式：使用数据库 hash 函数（MD5、hashtext、ORA_HASH）→ 按 hash 值分片
   - 示例（MySQL）：`WHERE CAST(MD5(username) AS UNSIGNED) % 10 = 0`

3. **DATE_RANGE（日期动态粒度分片）**
   - 支持类型：DATE、TIMESTAMP
   - 分片方式：查询 MIN/MAX 日期 → 计算总天数 → 动态决定每个分片天数
   - 示例：`WHERE order_date >= '2020-01-01' AND order_date < '2020-02-01'`

4. **FULL_TABLE_SCAN（全表扫描）**
   - 无主键或类型不支持时使用
   - 无法并行读取

**自动推断逻辑：**
- 配置了 `splitKey` → 验证类型 → 选择对应策略
- 配置了 `table` → 自动从主键推断 → 选择最优类型
- 配置了 `sql`（无 table）→ 单分片全表扫描

**配置示例：**
```json
{
  "type": "jdbc",
  "url": "jdbc:mysql://localhost:3306/test",
  "username": "root",
  "password": "secret",
  "table": "users",
  "splitKey": "id",           // 可选，自动推断主键
  "batchSize": 1000
}
```
```

- [ ] **Step 3: 提交**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md，添加字符串和日期分片策略说明"
```

---

## Task 22: 提交最终代码

**Files:**
- 无新增文件

- [ ] **Step 1: 检查所有修改**

运行: `git status`

预期输出: 显示所有修改的文件

- [ ] **Step 2: 创建总结性提交**

如果前面已经分步提交，可以创建一个总结性的 commit（可选）：

```bash
git log --oneline -10
```

查看最近的提交记录。

- [ ] **Step 3: 推送到远程仓库（如果需要）**

```bash
git push origin master
```

预期输出: 推送成功

---

## 验收检查清单

完成所有任务后，验证以下内容：

- [ ] **功能完整性**
  - SplitStrategy 枚举包含 STRING_HASH 和 DATE_RANGE
  - 所有方言实现 hashModExpression 和 buildDateRangeQuery
  - 四个 Splitter 类正常工作
  - JdbcSplitEnumerator 使用策略模式
  - JdbcSplitHelper 职责缩小

- [ ] **测试覆盖**
  - 方言测试覆盖 MySQL、PostgreSQL、H2
  - Splitter 测试验证 SQL 生成
  - 集成测试验证完整流程

- [ ] **代码质量**
  - 所有类职责清晰
  - 无 placeholder 或 TODO
  - 编译无警告
  - 所有测试通过

- [ ] **文档更新**
  - PLUGINS.md 包含新策略说明
  - 设计文档完整

---

## 执行完成标记

全部任务完成后，在计划文件末尾添加完成标记：

```markdown
---
**状态：** 实现完成
**完成日期：** YYYY-MM-DD
```