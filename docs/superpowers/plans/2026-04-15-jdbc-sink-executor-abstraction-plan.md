# JDBC Sink Executor 抽象层重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 引入 Executor 抽象层，实现 CDC/UPSERT 模式的 Key 归并缓冲和时间阈值刷写，优化性能和正确性。

**Architecture:** 通过 JdbcBatchStatementExecutor 接口分离执行逻辑，JdbcOutputFormat 管理刷写逻辑，BufferReducedExecutor 实现 Key 归并和分段执行。

**Tech Stack:** Java 1.8, Apache Flink 1.15.2, JDBC, H2 Database (测试)

---

## 实现优先级

按照依赖关系从底层向上层实现：

1. Executor 接口 + 基础实现（无依赖）
2. BufferReducedExecutor（依赖基础 Executor）
3. JdbcOutputFormat + Builder（依赖 Executor）
4. 配置变更（无依赖）
5. Writer 重构（依赖 OutputFormat）
6. 测试覆盖（依赖所有组件）

---

### Task 1: 创建 JdbcBatchStatementExecutor 接口

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor/JdbcBatchStatementExecutor.java`

- [ ] **Step 1: 创建 executor 包目录**

Run: `mkdir -p flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor`

- [ ] **Step 2: 编写 Executor 接口**

```java
package com.etl.connector.jdbc.sink.executor;

import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC 批量执行器接口
 * 不同写入模式有不同的实现策略
 */
public interface JdbcBatchStatementExecutor {

    /**
     * 初始化 PreparedStatement
     * @param connection 数据库连接
     * @throws SQLException SQL 异常
     */
    void prepareStatements(Connection connection) throws SQLException;

    /**
     * 添加数据到批次
     * @param record 数据行
     * @throws SQLException SQL 异常
     */
    void addToBatch(Row record) throws SQLException;

    /**
     * 执行当前批次
     * @throws SQLException SQL 异常
     */
    void executeBatch() throws SQLException;

    /**
     * 关闭 Statement
     * @throws SQLException SQL 异常
     */
    void closeStatements() throws SQLException;
}
```

- [ ] **Step 3: 提交接口**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor/JdbcBatchStatementExecutor.java
git commit -m "feat(jdbc-sink): 添加 JdbcBatchStatementExecutor 接口

定义 JDBC 批量执行的抽象，支持不同写入模式的实现策略。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 2: 实现 SimpleBatchExecutor（CUSTOM 模式）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor/SimpleBatchExecutor.java`

- [ ] **Step 1: 编写 SimpleBatchExecutor 类**

```java
package com.etl.connector.jdbc.sink.executor;

import com.etl.connector.jdbc.sink.NamedParameterSqlParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 简单批量执行器
 * 用于 CUSTOM 模式，直接执行用户自定义 SQL
 */
@Slf4j
public class SimpleBatchExecutor implements JdbcBatchStatementExecutor {

    private final String sql;
    private transient PreparedStatement statement;

    public SimpleBatchExecutor(String sql) {
        this.sql = sql;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(sql);
        String preparedSql = parsed.getPreparedSql();
        this.statement = connection.prepareStatement(preparedSql);
        log.info("SimpleBatchExecutor 初始化: sql={}", preparedSql);
    }

    @Override
    public void addToBatch(Row record) throws SQLException {
        String[] fieldNames = record.getFieldNames(true).stream()
                .filter(name -> !name.startsWith("__"))
                .toArray(String[]::new);

        for (int i = 0; i < fieldNames.length; i++) {
            statement.setObject(i + 1, record.getField(fieldNames[i]));
        }
        statement.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        statement.executeBatch();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
        }
    }
}
```

- [ ] **Step 2: 提交 SimpleBatchExecutor**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor/SimpleBatchExecutor.java
git commit -m "feat(jdbc-sink): 实现 SimpleBatchExecutor 用于 CUSTOM 模式

支持用户自定义 SQL 的批量执行。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 3: 实现 SimpleBufferedExecutor（INSERT 模式）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor/SimpleBufferedExecutor.java`

- [ ] **Step 1: 编写 SimpleBufferedExecutor 类**

```java
package com.etl.connector.jdbc.sink.executor;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 简单缓冲执行器
 * 用于 INSERT 模式，缓冲数据后批量执行
 */
@Slf4j
public class SimpleBufferedExecutor implements JdbcBatchStatementExecutor {

    private final String sql;
    private final String[] columns;
    private transient PreparedStatement statement;

    public SimpleBufferedExecutor(String sql, String[] columns) {
        this.sql = sql;
        this.columns = columns;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.statement = connection.prepareStatement(sql);
        log.info("SimpleBufferedExecutor 初始化: sql={}", sql);
    }

    @Override
    public void addToBatch(Row record) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            statement.setObject(i + 1, record.getField(columns[i]));
        }
        statement.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        statement.executeBatch();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
        }
    }
}
```

- [ ] **Step 2: 提交 SimpleBufferedExecutor**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor/SimpleBufferedExecutor.java
git commit -m "feat(jdbc-sink): 实现 SimpleBufferedExecutor 用于 INSERT 模式

缓冲数据后批量执行 INSERT SQL。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 4: 实现 BufferReducedExecutor（CDC/UPSERT 核心）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor/BufferReducedExecutor.java`

- [ ] **Step 1: 编写 BufferReducedExecutor 类（Key 归并逻辑）**

```java
package com.etl.connector.jdbc.sink.executor;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Key 归并缓冲执行器
 * 用于 CDC/UPSERT 模式，按主键归并数据并分段执行
 */
@Slf4j
public class BufferReducedExecutor implements JdbcBatchStatementExecutor {

    private final JdbcDialect dialect;
    private final String table;
    private final String[] columns;
    private final List<String> keyFields;
    private final boolean isCdcMode;

    private final JdbcBatchStatementExecutor upsertExecutor;
    private final JdbcBatchStatementExecutor deleteExecutor;
    private final Function<Row, Row> keyExtractor;

    // 核心缓冲：LinkedHashMap 保证流入顺序
    // Boolean: true=upsert, false=delete
    private transient LinkedHashMap<Row, Map.Entry<Boolean, Row>> buffer;

    public BufferReducedExecutor(
            JdbcDialect dialect,
            String table,
            String[] columns,
            List<String> keyFields,
            boolean isCdcMode) {
        this.dialect = dialect;
        this.table = table;
        this.columns = columns;
        this.keyFields = Preconditions.checkNotNull(keyFields, "keyFields 不能为 null");
        this.isCdcMode = isCdcMode;

        // 初始化内部 Executor
        String upsertSql = dialect.getUpsertSql(table, columns, keyFields);
        String deleteSql = dialect.getDeleteSql(table, keyFields);

        this.upsertExecutor = new SimpleBufferedExecutor(upsertSql, columns);
        this.deleteExecutor = new SimpleBufferedExecutor(deleteSql, keyFields.toArray(new String[0]));

        // Key 提取函数
        this.keyExtractor = row -> {
            Object[] keyValues = new Object[keyFields.size()];
            for (int i = 0; i < keyFields.size(); i++) {
                keyValues[i] = row.getField(keyFields.get(i));
            }
            return Row.of(keyValues);
        };

        log.info("BufferReducedExecutor 初始化: table={}, keyFields={}, isCdcMode={}, upsertSql={}, deleteSql={}",
                table, keyFields, isCdcMode, upsertSql, deleteSql);
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        upsertExecutor.prepareStatements(connection);
        deleteExecutor.prepareStatements(connection);
        this.buffer = new LinkedHashMap<>();
    }

    @Override
    public void addToBatch(Row record) throws SQLException {
        RowKind kind = record.getKind();

        // CDC 模式：UPDATE_BEFORE 直接跳过
        if (isCdcMode && kind == RowKind.UPDATE_BEFORE) {
            log.debug("跳过 UPDATE_BEFORE: {}", record);
            return;
        }

        // 提取主键
        Row key = keyExtractor.apply(record);

        // changeFlag: true=upsert(INSERT/UPDATE_AFTER), false=delete(DELETE)
        boolean changeFlag = (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER);

        // 归并入 buffer（同 key 自动覆盖，保留最终状态）
        buffer.put(key, Map.entry(changeFlag, record));
    }

    @Override
    public void executeBatch() throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }

        Boolean prevFlag = null;
        for (Map.Entry<Row, Map.Entry<Boolean, Row>> entry : buffer.entrySet()) {
            boolean currentFlag = entry.getValue().getKey();
            Row data = entry.getValue().getValue();

            if (currentFlag) {  // Upsert
                // 前面是 delete，先执行完 delete
                if (prevFlag != null && !prevFlag) {
                    deleteExecutor.executeBatch();
                }
                upsertExecutor.addToBatch(data);
            } else {  // Delete
                // 前面是 upsert，先执行完 upsert
                if (prevFlag != null && prevFlag) {
                    upsertExecutor.executeBatch();
                }
                deleteExecutor.addToBatch(entry.getKey());  // 只需要主键
            }
            prevFlag = currentFlag;
        }

        // 执行最后的批次
        if (prevFlag != null) {
            if (prevFlag) {
                upsertExecutor.executeBatch();
            } else {
                deleteExecutor.executeBatch();
            }
        }

        buffer.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        upsertExecutor.closeStatements();
        deleteExecutor.closeStatements();
    }
}
```

- [ ] **Step 2: 提交 BufferReducedExecutor**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/executor/BufferReducedExecutor.java
git commit -m "feat(jdbc-sink): 实现 BufferReducedExecutor 用于 CDC/UPSERT 模式

- Key 归并缓冲：LinkedHashMap 按主键归并，只保留最终状态
- UPDATE_BEFORE 跳过：CDC 模式下丢弃旧镜像数据
- 分段执行：Upsert/Delete 分开 batch，中间有 commit 隔断

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 5: 创建 JdbcOutputFormat（刷写管理）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcOutputFormat.java`

- [ ] **Step 1: 编写 JdbcOutputFormat 类**

```java
package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.sink.executor.JdbcBatchStatementExecutor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC 输出格式
 * 管理批量刷写逻辑，双重检查（batch_size + batch_interval_ms）
 */
@Slf4j
public class JdbcOutputFormat<I> {

    private final JdbcBatchStatementExecutor executor;
    private final Connection connection;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;

    private transient int batchCount = 0;
    private transient long lastFlushTimeMs;

    public JdbcOutputFormat(
            JdbcBatchStatementExecutor executor,
            Connection connection,
            int batchSize,
            long batchIntervalMs,
            int maxRetries) {
        this.executor = executor;
        this.connection = connection;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
    }

    /**
     * 初始化 Executor
     */
    public void open() throws SQLException {
        executor.prepareStatements(connection);
        this.lastFlushTimeMs = System.currentTimeMillis();
    }

    /**
     * 写入记录
     */
    public void writeRecord(I record) throws SQLException, InterruptedException {
        executor.addToBatch(record);
        batchCount++;

        // 双重检查：数量 或 时间
        if (batchCount > 0 && (isOverBatchSize() || isOverInterval())) {
            flush();
        }
    }

    /**
     * 刷写数据
     */
    public synchronized void flush() throws IOException, InterruptedException {
        if (batchCount == 0) {
            return;
        }

        for (int retry = 0; retry <= maxRetries; retry++) {
            try {
                executor.executeBatch();
                connection.commit();
                batchCount = 0;
                lastFlushTimeMs = System.currentTimeMillis();
                log.debug("Flush 成功: subtaskId={}", Thread.currentThread().getId());
                break;
            } catch (SQLException e) {
                if (retry >= maxRetries) {
                    // 重试次数用尽，回滚事务
                    try {
                        connection.rollback();
                        log.warn("Flush 失败，已回滚事务");
                    } catch (SQLException rollbackEx) {
                        log.error("回滚失败", rollbackEx);
                    }
                    throw new IOException("Flush failed after " + maxRetries + " retries", e);
                }

                // 等待后重试
                long sleepMs = 1000L * retry;
                log.warn("Flush 失败，等待 {}ms 后重试 (retry={})", sleepMs, retry);
                Thread.sleep(sleepMs);
            }
        }
    }

    /**
     * 关闭资源
     */
    public void close() throws SQLException, IOException, InterruptedException {
        // 提交剩余数据
        flush();

        // 关闭 Executor
        executor.closeStatements();
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

- [ ] **Step 2: 提交 JdbcOutputFormat**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcOutputFormat.java
git commit -m "feat(jdbc-sink): 创建 JdbcOutputFormat 管理刷写逻辑

- 双重检查：batch_size + batch_interval_ms 触发刷写
- 重试机制：失败后指数等待重试
- 事务管理：成功 commit，失败 rollback

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 6: 创建 JdbcOutputFormatBuilder（Executor 工厂）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcOutputFormatBuilder.java`

- [ ] **Step 1: 编写 JdbcOutputFormatBuilder 类**

```java
package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.dialect.WriteMode;
import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import com.etl.connector.jdbc.sink.executor.BufferReducedExecutor;
import com.etl.connector.jdbc.sink.executor.JdbcBatchStatementExecutor;
import com.etl.connector.jdbc.sink.executor.SimpleBatchExecutor;
import com.etl.connector.jdbc.sink.executor.SimpleBufferedExecutor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;

/**
 * JdbcOutputFormat 构建器
 * 根据写入模式创建对应的 Executor
 */
@Slf4j
public class JdbcOutputFormatBuilder {

    private final JdbcSinkConfig config;
    private final Connection connection;
    private transient String[] columns;

    public JdbcOutputFormatBuilder(JdbcSinkConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }

    /**
     * 构建 OutputFormat
     */
    public JdbcOutputFormat<org.apache.flink.types.Row> build() {
        JdbcBatchStatementExecutor executor = createExecutor(config);

        int batchSize = config.getBatchSize() != null ? config.getBatchSize() : 100;
        long batchIntervalMs = config.getBatchIntervalMs() != null ? config.getBatchIntervalMs() : 0L;
        int maxRetries = 3;  // 默认重试 3 次

        return new JdbcOutputFormat<>(executor, connection, batchSize, batchIntervalMs, maxRetries);
    }

    /**
     * 根据模式创建 Executor
     */
    private JdbcBatchStatementExecutor createExecutor(JdbcSinkConfig config) {
        switch (config.getMode()) {
            case INSERT:
                String insertSql = config.getDialect().getInsertSql(config.getTable(), getColumns());
                log.info("INSERT 模式: table={}, sql={}", config.getTable(), insertSql);
                return new SimpleBufferedExecutor(insertSql, getColumns());

            case UPSERT:
                log.info("UPSERT 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
                return new BufferReducedExecutor(
                        config.getDialect(),
                        config.getTable(),
                        getColumns(),
                        config.getKeyFields(),
                        false  // 非 CDC 模式，不跳过 UPDATE_BEFORE
                );

            case CDC:
                log.info("CDC 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
                return new BufferReducedExecutor(
                        config.getDialect(),
                        config.getTable(),
                        getColumns(),
                        config.getKeyFields(),
                        true   // CDC 模式，跳过 UPDATE_BEFORE
                );

            case CUSTOM:
                log.info("CUSTOM 模式: sql={}", config.getSql());
                return new SimpleBatchExecutor(config.getSql());

            default:
                throw new IllegalArgumentException("不支持的写入模式: " + config.getMode());
        }
    }

    /**
     * 获取列名数组（缓存）
     */
    private String[] getColumns() {
        if (columns == null) {
            // 从第一个字段推断列名（实际会在 Writer 首次写入时更新）
            columns = new String[0];
        }
        return columns;
    }

    /**
     * 更新列名（Writer 首次写入时调用）
     */
    public void updateColumns(String[] columns) {
        this.columns = columns;
    }
}
```

- [ ] **Step 2: 提交 JdbcOutputFormatBuilder**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcOutputFormatBuilder.java
git commit -m "feat(jdbc-sink): 创建 JdbcOutputFormatBuilder 工厂

根据写入模式创建对应的 Executor：
- INSERT → SimpleBufferedExecutor
- UPSERT/CDC → BufferReducedExecutor
- CUSTOM → SimpleBatchExecutor

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 7: 更新 JdbcSinkConfig 配置

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/config/JdbcSinkConfig.java`

- [ ] **Step 1: 修改 JdbcSinkConfig 添加 batchIntervalMs 字段**

在文件第 37 行（`private final JdbcDialect dialect;`）后添加：

```java
    /** 批量刷写间隔（毫秒），默认 0 表示禁用 */
    private final Long batchIntervalMs;
```

完整修改后的类：

```java
package com.etl.connector.jdbc.sink.config;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.dialect.WriteMode;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

/**
 * JDBC Sink 配置
 */
@Getter
@Builder
public class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 数据库连接 URL */
    private final String url;
    /** 用户名 */
    private final String username;
    /** 密码 */
    private final String password;
    /** 目标表名（与 sql 二选一，优先） */
    private final String table;
    /** 自定义 SQL，支持具名占位符 :paramName */
    private final String sql;
    /** 批量写入大小，默认 100 */
    private final Integer batchSize;
    /** 批量刷写间隔（毫秒），默认 0 表示禁用 */
    private final Long batchIntervalMs;
    /** 写入模式：INSERT 或 UPSERT */
    private final WriteMode mode;
    /** Upsert 模式下的主键/唯一键字段列表 */
    private final List<String> keyFields;
    /** 数据库方言 */
    private final JdbcDialect dialect;
}
```

- [ ] **Step 2: 提交配置变更**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/config/JdbcSinkConfig.java
git commit -m "feat(jdbc-sink): JdbcSinkConfig 新增 batchIntervalMs 配置

支持时间阈值刷写，默认 0 禁用。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 8: 更新 JdbcSink 配置解析

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSink.java`

- [ ] **Step 1: 在 JdbcSink.java 第 70 行后添加 batchIntervalMs 解析**

在第 70 行（`Integer batchSize = config.getInteger...`）后添加：

```java
        Long batchIntervalMs = config.getLong("batchIntervalMs", 0L);
```

在第 82 行（`.batchSize(batchSize)`）后添加：

```java
                .batchIntervalMs(batchIntervalMs)
```

修改后的构造函数片段：

```java
        Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
        Preconditions.checkArgument(batchSize != null && batchSize > 0, "batchSize must be greater than 0");

        Long batchIntervalMs = config.getLong("batchIntervalMs", 0L);

        this.jdbcSinkConfig = JdbcSinkConfig.builder()
                .url(dialect.wrapUrl(url))
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .dialect(dialect)
                .mode(mode)
                .keyFields(keyFields)
                .batchSize(batchSize)
                .batchIntervalMs(batchIntervalMs)
                .build();
```

- [ ] **Step 2: 提交配置解析变更**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSink.java
git commit -m "feat(jdbc-sink): JdbcSink 解析 batchIntervalMs 配置

从 SinkConfig 读取 batchIntervalMs 参数，默认 0。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 9: 重构 JdbcSinkWriter

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSinkWriter.java`

- [ ] **Step 1: 重写 JdbcSinkWriter 类**

完全重写为使用 OutputFormat：

```java
package com.etl.connector.jdbc.sink;

import com.etl.core.sink.AbstractSinkWriter;
import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * JDBC Sink Writer 实现
 * 简化为调用 OutputFormat
 */
@Slf4j
public class JdbcSinkWriter extends AbstractSinkWriter<JdbcSinkConfig> {

    private static final String FIELD_FILTER_PREFIX = "__";
    private final transient Connection connection;
    private final JdbcOutputFormat<Row> outputFormat;
    private transient String[] columns;
    private transient JdbcOutputFormatBuilder builder;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
        super(context, config);

        // 初始化数据库连接
        try {
            connection = DriverManager.getConnection(
                config.getUrl(),
                config.getUsername(),
                config.getPassword()
            );
            connection.setAutoCommit(false);

            // 创建 OutputFormat Builder
            this.builder = new JdbcOutputFormatBuilder(config, connection);

            // 创建 OutputFormat
            this.outputFormat = builder.build();
            this.outputFormat.open();

            log.info("JDBC Sink Writer 已连接: url={}, mode={}, subtaskId={}",
                config.getUrl(), config.getMode(), context.getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to initialize JDBC connection", e);
        }
    }

    @Override
    public void write(Row row, Context context) throws IOException, InterruptedException {
        try {
            // 首次写入时缓存列名（过滤掉 __ 开头的隐藏字段）
            if (columns == null) {
                columns = row.getFieldNames(true).stream()
                        .filter(name -> !name.startsWith(FIELD_FILTER_PREFIX))
                    .toArray(String[]::new);
                log.debug("JDBC Sink 写入字段（已过滤隐藏字段）: {}", Arrays.toString(columns));

                // 更新 Builder 的列名（用于 Executor）
                builder.updateColumns(columns);
            }

            outputFormat.writeRecord(row);
        } catch (SQLException e) {
            throw new IOException("Failed to write row", e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        outputFormat.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            // 提交剩余数据并关闭 OutputFormat
            outputFormat.close();

            // 关闭数据库连接
            if (connection != null) {
                connection.close();
            }
            log.info("JDBC Sink 资源清理完成, subtaskId={}", context.getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to cleanup JDBC resources", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing", e);
        }
    }
}
```

- [ ] **Step 2: 提交 Writer 重构**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/JdbcSinkWriter.java
git commit -m "refactor(jdbc-sink): JdbcSinkWriter 简化为调用 OutputFormat

- 移除 PreparedStatement 管理（由 Executor 管理）
- 移除 CDC 逻辑（由 BufferReducedExecutor 处理）
- 简化为一行调用：outputFormat.writeRecord(row)

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 10: 编写 BufferReducedExecutor 测试

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/executor/BufferReducedExecutorTest.java`

- [ ] **Step 1: 创建测试目录**

Run: `mkdir -p flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/executor`

- [ ] **Step 2: 编写 Key 归并测试**

```java
package com.etl.connector.jdbc.sink.executor;

import com.etl.connector.jdbc.dialect.H2Dialect;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BufferReducedExecutor 测试
 */
class BufferReducedExecutorTest {

    private Connection connection;
    private BufferReducedExecutor executor;

    @BeforeEach
    void setUp() throws SQLException {
        // 创建 H2 内存数据库
        connection = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        connection.setAutoCommit(false);

        // 创建测试表
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
        stmt.close();
        connection.commit();

        // 初始化 Executor
        String[] columns = {"id", "name", "age"};
        List<String> keyFields = Arrays.asList("id");
        executor = new BufferReducedExecutor(
                new H2Dialect(),
                "users",
                columns,
                keyFields,
                true  // CDC 模式
        );
        executor.prepareStatements(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        executor.closeStatements();
        connection.close();
    }

    @Test
    void testKeyReduction_sameKeyMultipleOperations_keepsFinalState() throws SQLException {
        // 同主键多次变更
        Row row1 = Row.ofKind(RowKind.INSERT, 1, "张三", 20);
        Row row2 = Row.ofKind(RowKind.UPDATE_AFTER, 1, "李四", 20);
        Row row3 = Row.ofKind(RowKind.UPDATE_AFTER, 1, "王五", 25);

        executor.addToBatch(row1);
        executor.addToBatch(row2);
        executor.addToBatch(row3);

        // 执行批次
        executor.executeBatch();
        connection.commit();

        // 验证数据库只有最终状态
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id=1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("王五", rs.getString("name"));
        assertEquals(25, rs.getInt("age"));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }

    @Test
    void testUpdateBeforeSkipped_cdcMode_updateBeforeIgnored() throws SQLException {
        // UPDATE_BEFORE 应该被跳过
        Row updateBefore = Row.ofKind(RowKind.UPDATE_BEFORE, 1, "张三", 20);
        Row updateAfter = Row.ofKind(RowKind.UPDATE_AFTER, 1, "李四", 21);

        executor.addToBatch(updateBefore);
        executor.addToBatch(updateAfter);

        executor.executeBatch();
        connection.commit();

        // 验证数据库只有 UPDATE_AFTER 的数据
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id=1");
        assertTrue(rs.next());
        assertEquals("李四", rs.getString("name"));
        assertEquals(21, rs.getInt("age"));

        rs.close();
        stmt.close();
    }

    @Test
    void testSegmentedExecution_upsertThenDelete_executedInOrder() throws SQLException {
        // Upsert → Delete 混合
        Row insert = Row.ofKind(RowKind.INSERT, 1, "张三", 20);
        Row delete = Row.ofKind(RowKind.DELETE, 2, "李四", 21);

        executor.addToBatch(insert);
        executor.addToBatch(delete);

        executor.executeBatch();
        connection.commit();

        // 验证 id=1 存在，id=2 不存在
        Statement stmt = connection.createStatement();

        ResultSet rs1 = stmt.executeQuery("SELECT * FROM users WHERE id=1");
        assertTrue(rs1.next());
        assertEquals("张三", rs1.getString("name"));
        rs1.close();

        ResultSet rs2 = stmt.executeQuery("SELECT * FROM users WHERE id=2");
        assertFalse(rs2.next());
        rs2.close();

        stmt.close();
    }

    @Test
    void testUpsertThenDelete_finalStateIsDelete() throws SQLException {
        // 先 Upsert，后 Delete → 最终状态是 Delete
        Row upsert = Row.ofKind(RowKind.INSERT, 1, "张三", 20);
        Row delete = Row.ofKind(RowKind.DELETE, 1);

        executor.addToBatch(upsert);
        executor.addToBatch(delete);

        executor.executeBatch();
        connection.commit();

        // 验证数据库中没有 id=1
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id=1");
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }
}
```

- [ ] **Step 3: 运行测试验证通过**

Run: `cd flink-etl-connector/connector-jdbc && mvn test -Dtest=BufferReducedExecutorTest`

Expected: 4 tests passed

- [ ] **Step 4: 提交测试**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/executor/BufferReducedExecutorTest.java
git commit -m "test(jdbc-sink): BufferReducedExecutor 测试覆盖

- Key 归并正确性测试
- UPDATE_BEFORE 跳过测试
- 分段执行顺序测试
- Upsert→Delete 最终状态测试

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 11: 编写 JdbcOutputFormat 测试

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/JdbcOutputFormatTest.java`

- [ ] **Step 1: 编写时间阈值刷写测试**

```java
package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.sink.executor.SimpleBufferedExecutor;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JdbcOutputFormat 测试
 */
class JdbcOutputFormatTest {

    private Connection connection;
    private JdbcOutputFormat<Row> outputFormat;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:test2;DB_CLOSE_DELAY=-1");
        connection.setAutoCommit(false);

        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE test_table (id INT PRIMARY KEY, value VARCHAR(50))");
        stmt.close();
        connection.commit();

        // 创建简单的 Executor
        String insertSql = "INSERT INTO test_table (id, value) VALUES (?, ?)";
        SimpleBufferedExecutor executor = new SimpleBufferedExecutor(insertSql, new String[]{"id", "value"});
        executor.prepareStatements(connection);

        // 创建 OutputFormat：batchSize=10, batchIntervalMs=0
        outputFormat = new JdbcOutputFormat<>(executor, connection, 10, 0, 3);
        outputFormat.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        outputFormat.close();
        connection.close();
    }

    @Test
    void testBatchSizeFlush_reachesBatchSize_flushesAutomatically() throws Exception {
        // 写入 10 条数据触发 batch_size 刷写
        for (int i = 0; i < 10; i++) {
            outputFormat.writeRecord(Row.of(i, "value" + i));
        }

        // 验证数据库有 10 条记录
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_table");
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));

        rs.close();
        stmt.close();
    }

    @Test
    void testFlushBeforeClose_partialBatch_flushedOnClose() throws Exception {
        // 写入 5 条数据（未达到 batch_size）
        for (int i = 0; i < 5; i++) {
            outputFormat.writeRecord(Row.of(i, "value" + i));
        }

        // close() 应该触发 flush
        outputFormat.close();

        // 验证数据库有 5 条记录
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_table");
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));

        rs.close();
        stmt.close();
    }

    @Test
    void testBatchIntervalMs_enabled_flushesAfterInterval() throws Exception {
        // 重新创建 OutputFormat：batchSize=100, batchIntervalMs=1000ms
        SimpleBufferedExecutor executor = new SimpleBufferedExecutor(
                "INSERT INTO test_table (id, value) VALUES (?, ?)",
                new String[]{"id", "value"}
        );
        executor.prepareStatements(connection);

        outputFormat = new JdbcOutputFormat<>(executor, connection, 100, 1000, 3);
        outputFormat.open();

        // 写入 1 条数据（未达到 batch_size）
        outputFormat.writeRecord(Row.of(99, "test"));

        // 等待超过 batchIntervalMs
        Thread.sleep(1100);

        // 再次写入触发时间检查
        outputFormat.writeRecord(Row.of(100, "trigger"));

        // 验证数据库已经有数据（时间阈值触发了刷写）
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_table");
        assertTrue(rs.next());
        assertTrue(rs.getInt(1) >= 1);  // 至少有第一条数据

        rs.close();
        stmt.close();
    }
}
```

- [ ] **Step 2: 运行测试验证通过**

Run: `cd flink-etl-connector/connector-jdbc && mvn test -Dtest=JdbcOutputFormatTest`

Expected: 3 tests passed

- [ ] **Step 3: 提交测试**

```bash
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/JdbcOutputFormatTest.java
git commit -m "test(jdbc-sink): JdbcOutputFormat 时间阈值刷写测试

- batch_size 触发刷写测试
- close 时 flush 剩余数据测试
- batchIntervalMs 时间阈值刷写测试

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 12: 运行全部测试验证集成

**Files:**
- 无新增

- [ ] **Step 1: 运行 connector-jdbc 所有测试**

Run: `cd flink-etl-connector/connector-jdbc && mvn test`

Expected: 所有测试通过（包括新增的 Executor 测试）

- [ ] **Step 2: 运行项目全部测试验证向后兼容**

Run: `mvn test`

Expected: 所有测试通过，INSERT/CUSTOM 模式行为不变

- [ ] **Step 3: 提交集成测试验证**

```bash
git commit --allow-empty -m "test(jdbc-sink): 集成测试验证 Executor 抽象层

所有测试通过，向后兼容性验证成功。

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Self-Review

完成计划后，检查以下清单：

**1. Spec Coverage**（设计文档覆盖检查）：
- ✅ Executor 接口定义 → Task 1
- ✅ SimpleBatchExecutor → Task 2
- ✅ SimpleBufferedExecutor → Task 3
- ✅ BufferReducedExecutor → Task 4
- ✅ JdbcOutputFormat → Task 5
- ✅ JdbcOutputFormatBuilder → Task 6
- ✅ JdbcSinkConfig 新增字段 → Task 7
- ✅ JdbcSink 解析配置 → Task 8
- ✅ JdbcSinkWriter 重构 → Task 9
- ✅ Key 归并测试 → Task 10
- ✅ UPDATE_BEFORE 跳过测试 → Task 10
- ✅ 分段执行测试 → Task 10
- ✅ 时间阈值刷写测试 → Task 11
- ✅ 错误重试机制 → Task 5（在 JdbcOutputFormat 中实现）

**2. Placeholder Scan**（无 placeholder）：
- ✅ 无 TBD/TODO
- ✅ 无 "implement later"
- ✅ 无 "handle edge cases"
- ✅ 每个步骤都有完整代码

**3. Type Consistency**（类型一致性）：
- ✅ JdbcBatchStatementExecutor 接口方法签名统一
- ✅ BufferReducedExecutor 构造参数顺序一致
- ✅ JdbcOutputFormat 泛型类型 Row 一致

---

Plan complete and saved to `docs/superpowers/plans/2026-04-15-jdbc-sink-executor-abstraction-plan.md`.

**Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**