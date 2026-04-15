# JDBC Sink Mode 配置与 Core Dialect 实现

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 core 模块新增 JDBC Dialect 抽象层，支持 URL 包装和 Upsert SQL 生成；在 JDBC Sink 新增 mode 配置支持 insert/upsert 模式。

**Architecture:**
1. 在 `flink-etl-core` 创建 `dialect` 包，定义 `JdbcDialect` 接口，提供 URL 包装和 SQL 生成能力
2. 使用简单工厂模式 `JdbcDialects` 根据 URL 自动识别数据库类型并返回对应 Dialect
3. JDBC Source 和 Sink 通过 Dialect 统一处理数据库差异

**Tech Stack:** Java 1.8, Lombok, Flink 1.15.2

---

## 文件结构

```
flink-etl-core/src/main/java/com/etl/core/
├── dialect/
│   ├── JdbcDialect.java           # Dialect 接口
│   ├── JdbcDialects.java          # 简单工厂
│   ├── MySQLDialect.java          # MySQL 实现
│   └── PostgreSQLDialect.java     # PostgreSQL 实现

flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/
├── JdbcSource.java                # 修改：使用 Dialect 包装 URL

flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/
├── JdbcSinkPlugin.java            # 修改：使用 Dialect，支持 mode 配置
├── JdbcSinkFunction.java          # 修改：支持 upsert 模式
├── config/
│   └── JdbcSinkConfig.java        # 修改：新增 mode 和 keyFields 字段
```

---

## Task 1: 定义 JdbcDialect 接口

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialect.java`

- [ ] **Step 1: 创建 JdbcDialect 接口**

```java
package com.etl.core.dialect;

import java.io.Serializable;
import java.util.List;

/**
 * JDBC 数据库方言接口
 * 提供数据库特定的 SQL 生成和 URL 处理能力
 */
public interface JdbcDialect extends Serializable {

    /**
     * 获取数据库类型标识
     * @return 类型标识，如 "mysql", "postgresql"
     */
    String getName();

    /**
     * 判断 URL 是否匹配此数据库类型
     * @param url JDBC 连接 URL
     * @return 是否匹配
     */
    boolean acceptsUrl(String url);

    /**
     * 包装 JDBC URL，添加必要的参数
     * @param url 原始 URL
     * @return 包装后的 URL
     */
    String wrapUrl(String url);

    /**
     * 转义 SQL 标识符
     * @param identifier 标识符名称
     * @return 转义后的标识符
     */
    String quoteIdentifier(String identifier);

    /**
     * 生成 INSERT SQL
     * @param table 表名
     * @param columns 列名数组
     * @return INSERT SQL
     */
    String getInsertSql(String table, String[] columns);

    /**
     * 生成 UPSERT SQL（存在则更新，不存在则插入）
     * @param table 表名
     * @param columns 所有列名数组
     * @param keyFields 主键/唯一键字段列表
     * @return UPSERT SQL
     */
    String getUpsertSql(String table, String[] columns, List<String> keyFields);

    /**
     * 是否支持 UPSERT
     * @return 是否支持
     */
    default boolean supportsUpsert() {
        return true;
    }
}
```

- [ ] **Step 2: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialect.java
git commit -m "feat(core): 新增 JdbcDialect 接口定义"
```

---

## Task 2: 实现 MySQLDialect

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/dialect/MySQLDialect.java`

- [ ] **Step 1: 创建 MySQLDialect 实现类**

```java
package com.etl.core.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MySQL 数据库方言
 */
@Slf4j
@AutoService(JdbcDialect.class)
public class MySQLDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "mysql";
    }

    @Override
    public boolean acceptsUrl(String url) {
        return url != null && url.contains(":mysql:");
    }

    @Override
    public String wrapUrl(String url) {
        // MySQL 需要添加 useCursorFetch 参数，使 batchSize 生效
        if (url != null && !url.contains("useCursorFetch=true")) {
            url = url.contains("?") ? url + "&useCursorFetch=true" : url + "?useCursorFetch=true";
            log.info("MySQL URL 添加 useCursorFetch 参数");
        }
        // 添加 rewriteBatchedStatements 参数，优化批量写入
        if (url != null && !url.contains("rewriteBatchedStatements=true")) {
            url = url.contains("?") ? url + "&rewriteBatchedStatements=true" : url + "?rewriteBatchedStatements=true";
            log.info("MySQL URL 添加 rewriteBatchedStatements 参数");
        }
        return url;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String getInsertSql(String table, String[] columns) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                quoteIdentifier(table), colList, placeholders);
    }

    @Override
    public String getUpsertSql(String table, String[] columns, List<String> keyFields) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        // MySQL 使用 ON DUPLICATE KEY UPDATE
        // 更新所有非主键字段
        String updateClause = Arrays.stream(columns)
                .filter(col -> !keyFields.contains(col))
                .map(col -> quoteIdentifier(col) + "=VALUES(" + quoteIdentifier(col) + ")")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                quoteIdentifier(table), colList, placeholders, updateClause);
    }
}
```

- [ ] **Step 2: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/MySQLDialect.java
git commit -m "feat(core): 实现 MySQLDialect，支持 upsert"
```

---

## Task 3: 实现 PostgreSQLDialect

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/dialect/PostgreSQLDialect.java`

- [ ] **Step 1: 创建 PostgreSQLDialect 实现类**

```java
package com.etl.core.dialect;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * PostgreSQL 数据库方言
 */
@Slf4j
@AutoService(JdbcDialect.class)
public class PostgreSQLDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "postgresql";
    }

    @Override
    public boolean acceptsUrl(String url) {
        return url != null && url.contains(":postgresql:");
    }

    @Override
    public String wrapUrl(String url) {
        // PostgreSQL 默认参数，可根据需要添加
        return url;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String getInsertSql(String table, String[] columns) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                quoteIdentifier(table), colList, placeholders);
    }

    @Override
    public String getUpsertSql(String table, String[] columns, List<String> keyFields) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        // PostgreSQL 使用 ON CONFLICT ... DO UPDATE
        String keyFieldsStr = keyFields.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        // 更新所有非主键字段
        String updateClause = Arrays.stream(columns)
                .filter(col -> !keyFields.contains(col))
                .map(col -> quoteIdentifier(col) + "=EXCLUDED." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                quoteIdentifier(table), colList, placeholders, keyFieldsStr, updateClause);
    }
}
```

- [ ] **Step 2: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/PostgreSQLDialect.java
git commit -m "feat(core): 实现 PostgreSQLDialect，支持 upsert"
```

---

## Task 4: 创建 JdbcDialects 工厂类

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialects.java`

- [ ] **Step 1: 创建简单工厂类（使用 SPI 加载）**

```java
package com.etl.core.dialect;

import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

/**
 * JDBC Dialect 简单工厂
 * 使用 SPI 加载所有 Dialect 实现，根据 URL 自动识别数据库类型
 */
@Slf4j
public final class JdbcDialects {

    private JdbcDialects() {}

    /**
     * 根据 JDBC URL 获取对应的 Dialect
     * @param url JDBC 连接 URL
     * @return 对应的 Dialect
     * @throws IllegalArgumentException 如果不支持的数据库类型
     */
    public static JdbcDialect get(String url) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("JDBC URL 不能为空");
        }

        // 使用 SPI 加载所有 Dialect 实现
        ServiceLoader<JdbcDialect> loader = ServiceLoader.load(JdbcDialect.class);
        for (JdbcDialect dialect : loader) {
            if (dialect.acceptsUrl(url)) {
                log.debug("URL {} 匹配 Dialect: {}", url, dialect.getName());
                return dialect;
            }
        }

        throw new IllegalArgumentException("不支持的数据库类型，URL: " + url);
    }

    /**
     * 检查 URL 是否被支持
     * @param url JDBC 连接 URL
     * @return 是否支持
     */
    public static boolean isSupported(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }
        ServiceLoader<JdbcDialect> loader = ServiceLoader.load(JdbcDialect.class);
        for (JdbcDialect dialect : loader) {
            if (dialect.acceptsUrl(url)) {
                return true;
            }
        }
        return false;
    }
}
```

- [ ] **Step 2: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialects.java
git commit -m "feat(core): 新增 JdbcDialects 简单工厂"
```

---

## Task 5: 创建 WriteMode 枚举

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/dialect/WriteMode.java`

- [ ] **Step 1: 创建写入模式枚举**

```java
package com.etl.core.dialect;

/**
 * JDBC Sink 写入模式
 */
public enum WriteMode {
    /**
     * 插入模式，直接插入数据
     */
    INSERT,

    /**
     * Upsert 模式，存在则更新，不存在则插入
     */
    UPSERT
}
```

- [ ] **Step 2: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/dialect/WriteMode.java
git commit -m "feat(core): 新增 WriteMode 枚举"
```

---

## Task 6: 修改 JdbcSource 使用 Dialect

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java:35-39`

- [ ] **Step 1: 添加 dialect 导入并修改构造函数**

在 `JdbcSource.java` 中：
1. 添加导入：`import com.etl.core.dialect.JdbcDialect;` 和 `import com.etl.core.dialect.JdbcDialectLoader;`
2. 添加字段：`private final JdbcDialect dialect;`
3. 修改构造函数中的 URL 包装逻辑：

```java
// 原代码 (35-39行):
// MySQL 需要添加 useCursorFetch 参数，使 batchSize 生效
if (url.contains(":mysql:") && !url.contains("useCursorFetch=true")) {
    url = url.contains("?") ? url + "&useCursorFetch=true" : url + "?useCursorFetch=true";
    log.info("MySQL URL 添加 useCursorFetch 参数");
}

// 修改为:
// 使用 Dialect 包装 URL
this.dialect = JdbcDialects.get(url);
url = dialect.wrapUrl(url);
```

4. 在 `JdbcSourceConfig` 构建时添加 `dialect` 字段（需要在 config 中添加该字段）

- [ ] **Step 2: 修改 JdbcSourceConfig 添加 dialect 字段**

在 `JdbcSourceConfig.java` 中添加：
```java
/** 数据库方言 */
private final JdbcDialect dialect;
```

- [ ] **Step 3: 提交**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java
git commit -m "refactor(jdbc-source): 使用 JdbcDialect 替代硬编码 URL 包装"
```

---

## Task 7: 修改 JdbcSinkConfig 新增配置字段

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/config/JdbcSinkConfig.java`

- [ ] **Step 1: 添加 mode 和 keyFields 字段**

```java
package com.etl.jdbc.sink.config;

import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.WriteMode;
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
    /** 写入模式：INSERT 或 UPSERT */
    private final WriteMode mode;
    /** Upsert 模式下的主键/唯一键字段列表 */
    private final List<String> keyFields;
    /** 数据库方言 */
    private final JdbcDialect dialect;
}
```

- [ ] **Step 2: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/config/JdbcSinkConfig.java
git commit -m "feat(jdbc-sink): 配置类新增 mode 和 keyFields 字段"
```

---

## Task 8: 修改 JdbcSinkPlugin 支持 mode 配置

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkPlugin.java`

- [ ] **Step 1: 重构 JdbcSinkPlugin**

```java
package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialectLoader;
import com.etl.core.dialect.WriteMode;
import com.etl.core.spi.SinkPlugin;
import com.etl.jdbc.sink.configJdbcSinkConfig;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * JDBC Sink 插件
 * 支持所有 JDBC 数据库，提供 table 和 sql 两种配置模式
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class JdbcSinkPlugin implements SinkPlugin {

    @Override
    public String getType() {
        return "jdbc";
    }

    @Override
    public SinkFunction<Row> createSink(SinkConfig config) {
        String url = config.getString("url");
        // 必要参数校验
        if (url == null) {
            throw new IllegalArgumentException("JDBC Sink 缺少必要配置: url");
        }

        String username = config.getString("username");
        if (username == null) {
            throw new IllegalArgumentException("JDBC Sink 缺少必要配置: username");
        }

        String password = config.getString("password");
        if (password == null) {
            throw new IllegalArgumentException("JDBC Sink 缺少必要配置: password");
        }

        String table = config.getString("table");
        String sql = config.getString("sql");
        if (table == null && sql == null) {
            throw new IllegalArgumentException("JDBC Sink 需要配置 table 或 sql");
        }

        // 获取 Dialect 并包装 URL
        JdbcDialect dialect = JdbcDialects.get(url);
        url = dialect.wrapUrl(url);

        int batchSize = config.getInteger("batchSize", getDefaultBatchSize());

        // 解析写入模式
        String modeStr = config.getString("mode", "insert").toUpperCase();
        WriteMode mode;
        try {
            mode = WriteMode.valueOf(modeStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("不支持的写入模式: " + modeStr + "。支持的模式: INSERT, UPSERT");
        }

        // 解析主键字段
        List<String> keyFields = Collections.emptyList();
        if (mode == WriteMode.UPSERT) {
            String keyFieldsStr = config.getString("keyFields");
            if (keyFieldsStr == null || keyFieldsStr.isEmpty()) {
                throw new IllegalArgumentException("UPSERT 模式需要配置 keyFields（主键/唯一键字段）");
            }
            keyFields = Arrays.asList(keyFieldsStr.split(","));
        }

        // table 模式检查 upsert 支持
        if (table != null && mode == WriteMode.UPSERT && !dialect.supportsUpsert()) {
            throw new IllegalArgumentException("数据库 " + dialect.getName() + " 不支持 UPSERT 模式");
        }

        JdbcSinkConfig jdbcConfig = JdbcSinkConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .batchSize(batchSize)
                .mode(mode)
                .keyFields(keyFields)
                .dialect(dialect)
                .build();

        String configMode = table != null ? "table" : "sql";
        log.info("创建 JDBC Sink, mode={}, writeMode={}, batchSize={}", configMode, mode, batchSize);

        return new JdbcSinkFunction(jdbcConfig);
    }
}
```

- [ ] **Step 2: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkPlugin.java
git commit -m "feat(jdbc-sink): 支持 mode 和 keyFields 配置"
```

---

## Task 9: 修改 JdbcSinkFunction 支持 Upsert

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkFunction.java`

- [ ] **Step 1: 修改 initStatement 方法支持 upsert**

```java
private void initStatement(Row row) throws SQLException {
    // table 优先
    if (config.getTable() != null) {
        // table 模式：从 Row 字段名生成 SQL
        Set<String> fieldNames = row.getFieldNames(true);
        if (fieldNames == null || fieldNames.isEmpty()) {
            throw new IllegalStateException("Row 没有字段名信息，请使用 Row.withNames()");
        }
        columns = fieldNames.toArray(new String[0]);

        String sql;
        JdbcDialect dialect = config.getDialect();
        if (config.getMode() == WriteMode.UPSERT) {
            // Upsert 模式
            sql = dialect.getUpsertSql(config.getTable(), columns, config.getKeyFields());
            log.info("JDBC Sink upsert 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
        } else {
            // Insert 模式
            sql = dialect.getInsertSql(config.getTable(), columns);
            log.info("JDBC Sink insert 模式: table={}, columns={}", config.getTable(), Arrays.toString(columns));
        }
        statement = connection.prepareStatement(sql);
    } else {
        // sql 模式：解析具名占位符
        NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(config.getSql());
        paramNames = parsed.getParamNames();
        statement = connection.prepareStatement(parsed.getPreparedSql());
        log.info("JDBC Sink sql 模式: params={}", paramNames);
    }
}
```

- [ ] **Step 2: 添加必要的导入**

```java
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.WriteMode;
```

- [ ] **Step 3: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSinkFunction.java
git commit -m "feat(jdbc-sink): 支持 upsert 模式"
```

---

## Task 10: 更新 SqlUtils 移除重复逻辑

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java`

- [ ] **Step 1: 移除 getInsertSql 方法（已移至 Dialect）**

删除 `getInsertSql` 方法，保留 `quoteIdentifier` 和 `inferRowType`、`getColumnType` 方法。

将 `quoteIdentifier` 方法改为调用 Dialect：

```java
/**
 * 转义 SQL 标识符（兼容旧代码）
 * @deprecated 请使用 JdbcDialect.quoteIdentifier
 */
@Deprecated
public static String quoteIdentifier(String name, String jdbcUrl) {
    JdbcDialect dialect = JdbcDialects.get(jdbcUrl);
    return dialect.quoteIdentifier(name);
}
```

- [ ] **Step 2: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java
git commit -m "refactor(core): SqlUtils.quoteIdentifier 标记为废弃"
```

---

## Task 11: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 更新 JDBC Sink 配置说明**

在 JDBC Sink 配置参数表格中新增：

```markdown
| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | JDBC 连接 URL |
| `username` | 是 | - | 数据库用户名 |
| `password` | 是 | - | 数据库密码 |
| `table` | 条件必填 | - | 目标表名。与 `sql` 二选一，优先 |
| `sql` | 条件必填 | - | 自定义 SQL，支持具名占位符 `:paramName` |
| `mode` | 否 | `insert` | 写入模式：`insert`（插入）或 `upsert`（存在则更新） |
| `keyFields` | upsert 必填 | - | Upsert 模式的主键/唯一键字段，多个字段用逗号分隔 |
| `batchSize` | 否 | `100` | 批量写入大小 |
```

添加配置示例：

```markdown
**table 模式 upsert（MySQL）：**

```json
{
  "sink": {
    "type": "jdbc",
    "inputTable": "output_data",
    "config": {
      "url": "jdbc:mysql://localhost:3306/mydb",
      "username": "root",
      "password": "password",
      "table": "user_table",
      "mode": "upsert",
      "keyFields": "id",
      "batchSize": 100
    }
  }
}
```

生成的 SQL：
```sql
INSERT INTO `user_table` (`id`, `name`, `email`) VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE `name`=VALUES(`name`), `email`=VALUES(`email`)
```
```

- [ ] **Step 2: 提交**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 JDBC Sink 文档，说明 mode 和 keyFields 配置"
```

---

## Task 12: 编写 Dialect 单元测试

**Files:**
- Create: `flink-etl-core/src/test/java/com/etl/core/dialect/MySQLDialectTest.java`
- Create: `flink-etl-core/src/test/java/com/etl/core/dialect/PostgreSQLDialectTest.java`

> **注意：** JdbcDialects 使用 SPI 加载，单元测试中直接实例化各 Dialect 类测试其方法。集成测试时需先 `mvn compile` 生成 SPI 配置文件。

- [ ] **Step 1: 创建 MySQLDialectTest**

```java
package com.etl.core.dialect;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class MySQLDialectTest {

    private final MySQLDialect dialect = new MySQLDialect();

    @Test
    void testAcceptsUrl() {
        assertTrue(dialect.acceptsUrl("jdbc:mysql://localhost:3306/test"));
        assertTrue(dialect.acceptsUrl("jdbc:mysql:replication://master,slave/test"));
        assertFalse(dialect.acceptsUrl("jdbc:postgresql://localhost:5432/test"));
        assertFalse(dialect.acceptsUrl(null));
    }

    @Test
    void testWrapUrl() {
        String url = "jdbc:mysql://localhost:3306/test";
        String wrapped = dialect.wrapUrl(url);
        assertTrue(wrapped.contains("useCursorFetch=true"));
        assertTrue(wrapped.contains("rewriteBatchedStatements=true"));
    }

    @Test
    void testWrapUrlWithExistingParams() {
        String url = "jdbc:mysql://localhost:3306/test?useSSL=false";
        String wrapped = dialect.wrapUrl(url);
        assertTrue(wrapped.contains("useSSL=false"));
        assertTrue(wrapped.contains("useCursorFetch=true"));
        assertTrue(wrapped.contains("rewriteBatchedStatements=true"));
    }

    @Test
    void testQuoteIdentifier() {
        assertEquals("`name`", dialect.quoteIdentifier("name"));
        assertEquals("`table_name`", dialect.quoteIdentifier("table_name"));
    }

    @Test
    void testGetInsertSql() {
        String sql = dialect.getInsertSql("user", new String[]{"id", "name", "email"});
        assertEquals("INSERT INTO `user` (`id`, `name`, `email`) VALUES (?, ?, ?)", sql);
    }

    @Test
    void testGetUpsertSql() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Collections.singletonList("id"));
        assertEquals("INSERT INTO `user` (`id`, `name`, `email`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`), `email`=VALUES(`email`)", sql);
    }

    @Test
    void testGetUpsertSqlWithCompositeKey() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Arrays.asList("id", "name"));
        assertEquals("INSERT INTO `user` (`id`, `name`, `email`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `email`=VALUES(`email`)", sql);
    }
}
```

- [ ] **Step 2: 创建 PostgreSQLDialectTest**

```java
package com.etl.core.dialect;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class PostgreSQLDialectTest {

    private final PostgreSQLDialect dialect = new PostgreSQLDialect();

    @Test
    void testAcceptsUrl() {
        assertTrue(dialect.acceptsUrl("jdbc:postgresql://localhost:5432/test"));
        assertFalse(dialect.acceptsUrl("jdbc:mysql://localhost:3306/test"));
        assertFalse(dialect.acceptsUrl(null));
    }

    @Test
    void testWrapUrl() {
        String url = "jdbc:postgresql://localhost:5432/test";
        String wrapped = dialect.wrapUrl(url);
        assertEquals(url, wrapped); // PostgreSQL 不添加额外参数
    }

    @Test
    void testQuoteIdentifier() {
        assertEquals("\"name\"", dialect.quoteIdentifier("name"));
        assertEquals("\"table_name\"", dialect.quoteIdentifier("table_name"));
    }

    @Test
    void testGetInsertSql() {
        String sql = dialect.getInsertSql("user", new String[]{"id", "name", "email"});
        assertEquals("INSERT INTO \"user\" (\"id\", \"name\", \"email\") VALUES (?, ?, ?)", sql);
    }

    @Test
    void testGetUpsertSql() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Collections.singletonList("id"));
        assertEquals("INSERT INTO \"user\" (\"id\", \"name\", \"email\") VALUES (?, ?, ?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\", \"email\"=EXCLUDED.\"email\"", sql);
    }

    @Test
    void testGetUpsertSqlWithCompositeKey() {
        String sql = dialect.getUpsertSql("user", new String[]{"id", "name", "email"}, Arrays.asList("id", "name"));
        assertEquals("INSERT INTO \"user\" (\"id\", \"name\", \"email\") VALUES (?, ?, ?) ON CONFLICT (\"id\", \"name\") DO UPDATE SET \"email\"=EXCLUDED.\"email\"", sql);
    }
}
```

- [ ] **Step 3: 运行测试验证**

```bash
mvn test -pl flink-etl-core -Dtest=MySQLDialectTest,PostgreSQLDialectTest
```

Expected: Tests run: 12, Failures: 0, Errors: 0

- [ ] **Step 4: 提交**

```bash
git add flink-etl-core/src/test/java/com/etl/core/dialect/
git commit -m "test(core): 新增 Dialect 单元测试"
```

---

## Task 13: 编译验证

- [ ] **Step 1: 编译项目**

```bash
mvn clean compile
```

Expected: BUILD SUCCESS

- [ ] **Step 2: 提交（如有修改）**

```bash
git add -A
git commit -m "fix: 修复编译问题"
```

---

## 验证清单

实现完成后验证以下功能：

- [ ] 编译后 `META-INF/services/com.etl.core.dialect.JdbcDialect` 文件包含 MySQL 和 PostgreSQL 实现
- [ ] `JdbcDialects.get("jdbc:mysql://...")` 通过 SPI 返回 MySQLDialect
- [ ] `JdbcDialects.get("jdbc:postgresql://...")` 通过 SPI 返回 PostgreSQLDialect
- [ ] MySQL URL 自动添加 `useCursorFetch` 和 `rewriteBatchedStatements` 参数
- [ ] JDBC Sink `mode=insert` 生成 INSERT SQL
- [ ] JDBC Sink `mode=upsert` 生成正确的 UPSERT SQL
- [ ] 不支持的数据库类型抛出明确错误信息

---

## 注意事项

1. **向后兼容**：`mode` 默认值为 `insert`，不配置时行为与之前一致
2. **sql 模式优先**：配置了 `sql` 时，忽略 `mode` 和 `keyFields`，使用自定义 SQL
3. **主键字段必填**：`mode=upsert` 时必须配置 `keyFields`
4. **数据库支持检查**：Dialect 可以声明是否支持 upsert
5. **SPI 扩展**：新增数据库支持只需创建新模块，实现 `JdbcDialect` 接口并添加 `@AutoService` 注解，无需修改核心代码