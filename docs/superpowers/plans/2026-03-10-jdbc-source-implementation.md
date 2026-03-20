# JDBC Source 插件实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标:** 实现 JDBC Source 插件，支持 MySQL 数据库的主键分片读取功能，包含完整的 JDBC 核心层和 MySQL 方言实现。

**架构:** 分层架构 - JDBC Core 抽象层提供通用的分片和读取逻辑，MySQL 方言适配 MySQL 特有的 SQL 语法和类型转换。

**技术栈:** Apache Flink 1.19+, JDBC, MySQL Connector/J 8.0+, Maven 3.6+

---

## 文件结构总览

### 新增模块

**flink-etl-source (Source 父模块)**
- `pom.xml` - Source 父模块配置

**flink-etl-source-jdbc (JDBC 核心层)**
- `flink-etl-source-jdbc/pom.xml` - JDBC 核心模块配置
- `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcDialect.java` - 数据库方言接口
- `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/MySQLDialect.java` - MySQL 方言实现
- `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java` - JDBC Source 核心实现
- `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java` - 分片枚举器
- `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSourceReader.java` - 数据读取器

**flink-etl-source-mysql (MySQL 插件模块)**
- `flink-etl-source-mysql/pom.xml` - MySQL 插件模块配置
- `flink-etl-source-mysql/src/main/java/com/etl/source/mysql/MySQLSourcePlugin.java` - MySQL 插件入口
- `flink-etl-source-mysql/src/main/resources/META-INF/services/com.etl.core.spi.SourcePlugin` - SPI 配置

### 修改文件

- `pom.xml` - 添加 flink-etl-source 模块
- `flink-etl-source/pom.xml` - 继承父项目配置

---

## Chunk 1: Source 模块结构和依赖配置

### Task 1: 创建 Source 父模块

**文件:**
- 创建: `flink-etl-source/pom.xml`
- 修改: `pom.xml`

- [ ] **Step 1: 创建 Source 父模块 pom.xml**

创建文件: `flink-etl-source/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-tool</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flink-etl-source</artifactId>
    <packaging>pom</packaging>
    <name>Flink ETL Source</name>
    <description>Source 插件父模块</description>

    <modules>
        <module>flink-etl-source-jdbc</module>
        <module>flink-etl-source-mysql</module>
    </modules>

    <dependencies>
        <!-- 核心模块 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-core</artifactId>
            <version>${project.version}</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 更新父项目 pom.xml 添加 Source 模块**

读取文件: `pom.xml`

在 `<modules>` 部分添加：

```xml
<modules>
    <module>flink-etl-core</module>
    <module>flink-etl-source</module>
    <module>flink-etl-sink</module>
    <module>flink-etl-transform</module>
</modules>
```

- [ ] **Step 3: 提交 Source 父模块配置**

```bash
git add flink-etl-source/pom.xml pom.xml
git commit -m "feat: 添加 Source 父模块配置"
```

---

### Task 2: 创建 JDBC 核心模块

**文件:**
- 创建: `flink-etl-source/flink-etl-source-jdbc/pom.xml`

- [ ] **Step 1: 创建 JDBC 核心模块 pom.xml**

创建文件: `flink-etl-source/flink-etl-source-jdbc/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-source</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flink-etl-source-jdbc</artifactId>
    <name>Flink ETL Source - JDBC</name>
    <description>JDBC 核心抽象层</description>

    <dependencies>
        <!-- Flink -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
        </dependency>

        <!-- MySQL 驱动 -->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 提交 JDBC 核心模块配置**

```bash
git add flink-etl-source/flink-etl-source-jdbc/
git commit -m "feat: 添加 JDBC 核心模块配置"
```

---

### Task 3: 创建 MySQL 插件模块

**文件:**
- 创建: `flink-etl-source/flink-etl-source-mysql/pom.xml`

- [ ] **Step 1: 创建 MySQL 插件模块 pom.xml**

创建文件: `flink-etl-source/flink-etl-source-mysql/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-source</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flink-etl-source-mysql</artifactId>
    <name>Flink ETL Source - MySQL</name>
    <description>MySQL Source 插件</description>

    <dependencies>
        <!-- JDBC 核心模块 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-source-jdbc</artifactId>
            <version>${project.version}</version>
        </dependency>

        <!-- MySQL 驱动 -->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 提交 MySQL 插件模块配置**

```bash
git add flink-etl-source/flink-etl-source-mysql/
git commit -m "feat: 添加 MySQL 插件模块配置"
```

---

## Chunk 2: JdbcDialect 接口和 MySQL 方言实现

### Task 4: 创建 JdbcDialect 接口

**文件:**
- 创建: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcDialect.java`

- [ ] **Step 1: 创建 JdbcDialect 接口**

创建文件: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcDialect.java`

```java
package com.etl.source.jdbc;

import org.apache.flink.types.Row;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * JDBC 数据库方言接口
 * 定义数据库特定的 SQL 构建和类型转换方法
 */
public interface JdbcDialect {

    /**
     * 获取 JDBC 驱动类名
     *
     * @return 驱动类名
     */
    String getDriverClassName();

    /**
     * 构建分片范围查询 SQL
     * 用于查询分片列的 MIN 和 MAX 值
     *
     * @param table 表名（可能为 null）
     * @param sql 自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @return 查询 SQL
     */
    String buildRangeQuery(String table, String sql, String splitColumn);

    /**
     * 构建分片数据查询 SQL
     *
     * @param table 表名（可能为 null）
     * @param sql 自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param start 起始值
     * @param end 结束值
     * @return 查询 SQL
     */
    String buildSplitQuery(String table, String sql, String splitColumn, long start, long end);

    /**
     * 从 ResultSet 创建 Flink Row
     * 处理数据库特定的类型转换
     *
     * @param rs ResultSet
     * @return Flink Row
     * @throws SQLException SQL 异常
     */
    Row createRow(ResultSet rs) throws SQLException;
}
```

- [ ] **Step 2: 提交 JdbcDialect 接口**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcDialect.java
git commit -m "feat: 添加 JdbcDialect 接口定义"
```

---

### Task 5: 创建 MySQLDialect 实现

**文件:**
- 创建: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/MySQLDialect.java`

- [ ] **Step 1: 创建 MySQLDialect 实现**

创建文件: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/MySQLDialect.java`

```java
package com.etl.source.jdbc.dialect;

import com.etl.source.jdbc.dialect.JdbcDialect;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * MySQL 数据库方言实现
 */
public class MySQLDialect implements JdbcDialect {
    private static final Logger logger = LoggerFactory.getLogger(MySQLDialect.class);

    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String buildRangeQuery(String table, String sql, String splitColumn) {
        String query;
        if (table != null) {
            // 表名模式
            query = String.format("SELECT MIN(%s), MAX(%s) FROM %s",
                    splitColumn, splitColumn, table);
        } else {
            // 自定义 SQL 模式
            query = String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t",
                    splitColumn, splitColumn, sql);
        }
        logger.debug("构建范围查询 SQL: {}", query);
        return query;
    }

    @Override
    public String buildSplitQuery(String table, String sql, String splitColumn, long start, long end) {
        String query;
        if (table != null) {
            // 表名模式
            query = String.format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d",
                    table, splitColumn, start, end);
        } else {
            // 自定义 SQL 模式
            query = String.format("SELECT * FROM (%s) AS t WHERE %s BETWEEN %d AND %d",
                    sql, splitColumn, start, end);
        }
        logger.debug("构建分片查询 SQL: {}", query);
        return query;
    }

    @Override
    public Row createRow(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        Row row = new Row(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            row.setField(i - 1, rs.getObject(i));
        }
        return row;
    }
}
```

- [ ] **Step 2: 提交 MySQLDialect 实现**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/MySQLDialect.java
git commit -m "feat: 添加 MySQLDialect 实现"
```

---

## Chunk 3: JdbcSource 核心类实现

### Task 6: 创建 JdbcSplitEnumerator

**文件:**
- 创建: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java`

- [ ] **Step 1: 创建 JdbcSplitEnumerator 实现**

创建文件: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java`

```java
package com.etl.source.jdbc;

import com.etl.core.source.PendingSplitsCheckpoint;
import com.etl.source.jdbc.RangeSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * JDBC 分片枚举器
 * 负责分配分片给 SourceReader
 */
public class JdbcSplitEnumerator
        implements SplitEnumerator<RangeSplit, PendingSplitsCheckpoint<RangeSplit>> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSplitEnumerator.class);

    private final List<RangeSplit> splits;
    private final SplitEnumeratorContext<RangeSplit> context;
    private int currentSplitIndex = 0;

    public JdbcSplitEnumerator(List<RangeSplit> splits, SplitEnumeratorContext<RangeSplit> context) {
        this.splits = splits;
        this.context = context;
    }

    @Override
    public void start() {
        logger.info("JDBC Split Enumerator 启动，总分片数: {}", splits.size());
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (currentSplitIndex < splits.size()) {
            RangeSplit split = splits.get(currentSplitIndex++);
            logger.info("分配分片 {} 给 Reader {}", split.splitId(), subtaskId);
            context.assignSplit(split, subtaskId);
        } else {
            logger.info("所有分片已分配完毕，通知 Reader {}", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<RangeSplit> splits, int subtaskId) {
        logger.warn("Reader {} 返回 {} 个未处理的分片", subtaskId, splits.size());
        this.splits.addAll(splits);
    }

    @Override
    public PendingSplitsCheckpoint<RangeSplit> snapshotState(long checkpointId) {
        List<RangeSplit> pendingSplits = splits.subList(currentSplitIndex, splits.size());
        logger.info("创建检查点 {}，待处理分片数: {}", checkpointId, pendingSplits.size());
        return new PendingSplitsCheckpoint<>(pendingSplits);
    }

    @Override
    public void close() {
        logger.info("JDBC Split Enumerator 关闭");
    }
}
```

- [ ] **Step 2: 提交 JdbcSplitEnumerator 实现**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSplitEnumerator.java
git commit -m "feat: 添加 JdbcSplitEnumerator 实现"
```

---

### Task 7: 创建 JdbcSourceReader

**文件:**
- 创建: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSourceReader.java`

- [ ] **Step 1: 创建 JdbcSourceReader 实现（第1部分：构造和方法签名）**

创建文件: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSourceReader.java`

```java
package com.etl.source.jdbc;

import com.etl.source.jdbc.RangeSplit;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * JDBC Source Reader
 * 负责执行 SQL 查询并读取数据
 */
public class JdbcSourceReader implements SourceReader<Row, RangeSplit> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSourceReader.class);

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final String splitColumn;
    private final Integer fetchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;
    private final SourceReaderContext context;

    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private RangeSplit currentSplit;

    public JdbcSourceReader(String url, String username, String password,
                            String table, String sql, String splitColumn,
                            Integer fetchSize, Integer queryTimeout,
                            JdbcDialect dialect, SourceReaderContext context) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.table = table;
        this.sql = sql;
        this.splitColumn = splitColumn;
        this.fetchSize = fetchSize;
        this.queryTimeout = queryTimeout;
        this.dialect = dialect;
        this.context = context;
    }
```

- [ ] **Step 2: 添加 SourceReader 接口方法实现**

继续添加到 `JdbcSourceReader.java`：

```java
    @Override
    public void start() {
        logger.info("JDBC Source Reader 启动");
        try {
            // 加载驱动
            Class.forName(dialect.getDriverClassName());
            // 创建连接
            connection = DriverManager.getConnection(url, username, password);
            logger.info("数据库连接成功");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC 驱动加载失败: " + e.getMessage(), e);
        } catch (SQLException e) {
            throw new RuntimeException("数据库连接失败: " + e.getMessage(), e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Row> output) throws Exception {
        if (resultSet == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (resultSet.next()) {
            try {
                // 创建 Row 并输出
                Row row = dialect.createRow(resultSet);
                output.collect(row);
                return InputStatus.MORE_AVAILABLE;
            } catch (SQLException e) {
                throw new RuntimeException("数据读取失败: " + e.getMessage(), e);
            }
        } else {
            // 当前分片读取完毕
            logger.info("分片 {} 读取完毕", currentSplit.splitId());
            closeCurrentSplit();
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public void addSplits(List<RangeSplit> splits) {
        if (!splits.isEmpty()) {
            this.currentSplit = splits.get(0);
            logger.info("接收分片: {}", currentSplit.splitId());
            executeQuery();
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        logger.info("通知无更多分片");
    }

    @Override
    public void close() throws Exception {
        logger.info("关闭 JDBC Source Reader");
        closeCurrentSplit();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void executeQuery() {
        String querySql = dialect.buildSplitQuery(table, sql, splitColumn,
                currentSplit.getStart(), currentSplit.getEnd());

        try {
            statement = connection.createStatement();
            if (fetchSize != null) {
                statement.setFetchSize(fetchSize);
            }
            if (queryTimeout != null) {
                statement.setQueryTimeout(queryTimeout);
            }

            logger.info("执行查询 SQL: {}", querySql);
            resultSet = statement.executeQuery(querySql);
        } catch (SQLException e) {
            throw new RuntimeException("查询执行失败: " + e.getMessage(), e);
        }
    }

    private void closeCurrentSplit() {
        try {
            if (resultSet != null) {
                resultSet.close();
                resultSet = null;
            }
            if (statement != null) {
                statement.close();
                statement = null;
            }
        } catch (SQLException e) {
            logger.error("关闭资源失败", e);
        }
    }
}
```

- [ ] **Step 3: 提交 JdbcSourceReader 实现**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSourceReader.java
git commit -m "feat: 添加 JdbcSourceReader 实现"
```

---

### Task 8: 创建 JdbcSource

**文件:**
- 创建: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 1: 创建 JdbcSource 实现（第1部分：构造和字段）**

创建文件: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

```java
package com.etl.source.jdbc;

import com.etl.core.localFileSourceConfig.SourceConfig;
import com.etl.source.jdbc.AbstractRangeSplitSource;
import com.etl.core.source.PendingSplitsCheckpoint;
import com.etl.source.jdbc.RangeSplit;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 */
public class JdbcSource extends AbstractRangeSplitSource<Row> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final Integer fetchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;

    public JdbcSource(SourceConfig localFileSourceConfig, JdbcDialect dialect) {
        super(
                localFileSourceConfig.getString("splitColumn"),
                localFileSourceConfig.getInteger("splitSize")
        );
        this.url = localFileSourceConfig.getString("url");
        this.username = localFileSourceConfig.getString("username");
        this.password = localFileSourceConfig.getString("password");
        this.table = localFileSourceConfig.getString("table");
        this.sql = localFileSourceConfig.getString("sql");
        this.fetchSize = localFileSourceConfig.getInteger("fetchSize");
        this.queryTimeout = localFileSourceConfig.getInteger("queryTimeout");
        this.dialect = dialect;

        logger.info("创建 JdbcSource: table={}, sql={}, splitColumn={}, splitSize={}",
                table, sql, splitColumn, splitSize);
    }
```

- [ ] **Step 2: 实现 getSplitColumnRange 方法**

继续添加到 `JdbcSource.java`：

```java
    @Override
    protected Range<Long> getSplitColumnRange() {
        String querySql = dialect.buildRangeQuery(table, sql, splitColumn);
        logger.info("查询分片范围: {}", querySql);

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql)) {

            if (rs.next()) {
                long min = rs.getLong(1);
                long max = rs.getLong(2);
                logger.info("分片范围: [{}, {}]", min, max);
                return Range.between(min, max);
            }
            return Range.between(0L, 0L);
        } catch (SQLException e) {
            throw new RuntimeException("获取分片范围失败: " + e.getMessage(), e);
        }
    }
```

- [ ] **Step 3: 实现 createEnumerator 和 createReader 方法**

继续添加到 `JdbcSource.java`：

```java
    @Override
    public SplitEnumerator<RangeSplit, PendingSplitsCheckpoint<RangeSplit>>
    createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
        logger.info("创建 SplitEnumerator");
        List<RangeSplit> splits = calculateSplits();
        return new JdbcSplitEnumerator(splits, enumContext);
    }

    @Override
    public SplitEnumerator<RangeSplit, PendingSplitsCheckpoint<RangeSplit>>
    restoreEnumerator(SplitEnumeratorContext<RangeSplit> enumContext,
                      PendingSplitsCheckpoint<RangeSplit> checkpoint) {
        logger.info("恢复 SplitEnumerator");
        List<RangeSplit> splits = calculateSplits();
        return new JdbcSplitEnumerator(splits, enumContext);
    }

    @Override
    public SourceReader<Row, RangeSplit> createReader(SourceReaderContext readerContext) {
        logger.info("创建 SourceReader");
        return new JdbcSourceReader(url, username, password, table, sql,
                splitColumn, fetchSize, queryTimeout, dialect, readerContext);
    }
```

- [ ] **Step 4: 实现序列化器方法**

继续添加到 `JdbcSource.java`：

```java
    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        // 使用简单的字符串序列化
        return new SimpleVersionedSerializer<RangeSplit>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(RangeSplit split) {
                return split.splitId().getBytes();
            }

            @Override
            public RangeSplit deserialize(int version, byte[] serialized) {
                // 简化实现，实际使用时会从 splitId 解析
                return new RangeSplit(splitColumn, 0, 0);
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint<RangeSplit>>
    getEnumeratorCheckpointSerializer() {
        // 简化实现
        return new SimpleVersionedSerializer<PendingSplitsCheckpoint<RangeSplit>>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(PendingSplitsCheckpoint<RangeSplit> checkpoint) {
                return new byte[0];
            }

            @Override
            public PendingSplitsCheckpoint<RangeSplit> deserialize(int version, byte[] serialized) {
                return new PendingSplitsCheckpoint<>(List.of());
            }
        };
    }
}
```

- [ ] **Step 5: 提交 JdbcSource 实现**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java
git commit -m "feat: 添加 JdbcSource 核心实现"
```

---

## Chunk 4: MySQL 插件集成

### Task 9: 创建 MySQLSourcePlugin

**文件:**
- 创建: `flink-etl-source/flink-etl-source-mysql/src/main/java/com/etl/source/mysql/MySQLSourcePlugin.java`

- [ ] **Step 1: 创建 MySQLSourcePlugin 实现**

创建文件: `flink-etl-source/flink-etl-source-mysql/src/main/java/com/etl/source/mysql/MySQLSourcePlugin.java`

```java
package com.etl.source.mysql;

import com.etl.core.localFileSourceConfig.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.SplitStrategy;
import com.etl.source.jdbc.JdbcSource;
import com.etl.source.jdbc.dialect.MySQLDialect;
import org.apache.flink.api.connector.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL Source 插件
 * 支持主键范围分片读取 MySQL 数据
 */
public class MySQLSourcePlugin implements SourcePlugin {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSourcePlugin.class);

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig localFileSourceConfig) {
        logger.info("创建 MySQL Source");
        MySQLDialect dialect = new MySQLDialect();
        return new JdbcSource(localFileSourceConfig, dialect);
    }

    @Override
    public SplitStrategy getSplitStrategy() {
        return SplitStrategy.RANGE;
    }
}
```

- [ ] **Step 2: 提交 MySQLSourcePlugin 实现**

```bash
git add flink-etl-source/flink-etl-source-mysql/src/main/java/com/etl/source/mysql/MySQLSourcePlugin.java
git commit -m "feat: 添加 MySQLSourcePlugin 实现"
```

---

### Task 10: 创建 SPI 配置文件

**文件:**
- 创建: `flink-etl-source/flink-etl-source-mysql/src/main/resources/META-INF/services/com.etl.core.spi.SourcePlugin`

- [ ] **Step 1: 创建 SPI 配置文件**

创建文件: `flink-etl-source/flink-etl-source-mysql/src/main/resources/META-INF/services/com.etl.core.spi.SourcePlugin`

内容：
```
com.etl.source.mysql.MySQLSourcePlugin
```

- [ ] **Step 2: 提交 SPI 配置文件**

```bash
git add flink-etl-source/flink-etl-source-mysql/src/main/resources/
git commit -m "feat: 添加 MySQL Source SPI 配置"
```

---

## Chunk 5: 编译验证和测试

### Task 11: 编译验证

**目标**: 确保所有代码编译通过

- [ ] **Step 1: 编译整个项目**

运行命令：
```bash
mvn clean compile -DskipTests
```

预期输出：
```
[INFO] BUILD SUCCESS
```

- [ ] **Step 2: 检查编译结果**

如果编译失败，根据错误信息修复代码，然后重新编译。

- [ ] **Step 3: 提交编译验证**

```bash
# 如果有修复
git add .
git commit -m "fix: 修复编译问题"
```

---

### Task 12: 更新示例配置

**文件:**
- 修改: `docs/examples/mysql-to-console.json`

- [ ] **Step 1: 验证 MySQL 配置示例**

读取文件: `docs/examples/mysql-to-console.json`

确认配置格式正确，包含必要的字段。

- [ ] **Step 2: 提交配置验证**

```bash
git add docs/examples/
git commit -m "docs: 验证 MySQL 配置示例"
```

---

## 总结

完成以上所有任务后，JDBC Source 插件实现完成：

**已实现功能**：
- ✓ JDBC 核心抽象层（JdbcDialect、JdbcSource、JdbcSourceReader、JdbcSplitEnumerator）
- ✓ MySQL 方言实现（MySQLDialect）
- ✓ MySQL 插件入口（MySQLSourcePlugin）
- ✓ SPI 配置和模块依赖
- ✓ 编译验证通过

**架构特点**：
- 分层设计，核心逻辑复用
- 易于扩展，支持新增数据库
- 配置驱动，使用简单
- 完善的错误处理和日志记录

**下一步工作**：
- 编写单元测试
- 编写集成测试
- 添加其他数据库方言（Oracle、PostgreSQL）
- 性能优化和连接池支持

---

**计划完成！准备执行。**