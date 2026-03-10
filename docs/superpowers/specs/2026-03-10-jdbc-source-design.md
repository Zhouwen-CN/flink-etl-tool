# JDBC Source 插件设计文档

**日期**: 2026-03-10
**作者**: Claude & User
**状态**: 已批准

---

## 1. 概述

### 1.1 目标

实现基于 JDBC 的数据源插件，支持 MySQL、Oracle、PostgreSQL 等关系型数据库的主键分片读取功能。

### 1.2 设计原则

- **分层抽象**：JDBC Core 核心层 + 数据库方言适配层
- **易于扩展**：新增数据库只需实现方言接口
- **配置驱动**：通过 JSON 配置实现灵活的数据读取
- **性能优化**：支持 fetch size、query timeout 等参数配置

---

## 2. 架构设计

### 2.1 模块结构

```
flink-etl-source/
├── pom.xml                                    # Source 父模块
├── flink-etl-source-jdbc/                     # JDBC 核心抽象层
│   ├── pom.xml
│   └── src/main/java/com/etl/source/jdbc/
│       ├── JdbcSource.java                    # 核心 Source 实现
│       ├── JdbcSourceReader.java              # 数据读取器
│       ├── JdbcSplitEnumerator.java           # 分片枚举器
│       ├── JdbcDialect.java                   # 数据库方言接口
│       └── dialect/
│           └── MySQLDialect.java              # MySQL 方言实现
└── flink-etl-source-mysql/                    # MySQL 插件模块
    ├── pom.xml
    └── src/main/
        ├── java/com/etl/source/mysql/
        │   └── MySQLSourcePlugin.java         # MySQL 插件入口
        └── resources/META-INF/services/
            └── com.etl.core.spi.SourcePlugin  # SPI 配置
```

### 2.2 核心组件职责

| 组件 | 职责 |
|------|------|
| **JdbcDialect** | 数据库方言接口，定义 SQL 构建、类型转换等适配方法 |
| **JdbcSource** | Flink Source 实现，继承 AbstractRangeSplitSource，管理分片计算 |
| **JdbcSourceReader** | 数据读取器，执行 SQL 查询，将 ResultSet 转换为 Row |
| **JdbcSplitEnumerator** | 分片协调器，分配分片给 Reader |
| **MySQLSourcePlugin** | SPI 插件入口，创建 JdbcSource 实例 |
| **MySQLDialect** | MySQL 数据库方言实现 |

---

## 3. 核心接口设计

### 3.1 JdbcDialect 接口

```java
public interface JdbcDialect {
    /**
     * 获取 JDBC 驱动类名
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
     */
    Row createRow(ResultSet rs) throws SQLException;
}
```

### 3.2 MySQLDialect 实现

```java
public class MySQLDialect implements JdbcDialect {

    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String buildRangeQuery(String table, String sql, String splitColumn) {
        if (table != null) {
            // 表名模式
            return String.format("SELECT MIN(%s), MAX(%s) FROM %s",
                splitColumn, splitColumn, table);
        } else {
            // 自定义 SQL 模式
            return String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t",
                splitColumn, splitColumn, sql);
        }
    }

    @Override
    public String buildSplitQuery(String table, String sql, String splitColumn, long start, long end) {
        if (table != null) {
            // 表名模式
            return String.format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d",
                table, splitColumn, start, end);
        } else {
            // 自定义 SQL 模式
            return String.format("SELECT * FROM (%s) AS t WHERE %s BETWEEN %d AND %d",
                sql, splitColumn, start, end);
        }
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

---

## 4. 配置设计

### 4.1 表名模式配置

```json
{
  "type": "mysql",
  "config": {
    "url": "jdbc:mysql://localhost:3306/test_db",
    "table": "user_table",
    "username": "root",
    "password": "123456",
    "splitColumn": "id",
    "splitSize": 10000,
    "fetchSize": 1000,
    "queryTimeout": 300
  }
}
```

### 4.2 自定义 SQL 模式配置

```json
{
  "type": "mysql",
  "config": {
    "url": "jdbc:mysql://localhost:3306/test_db",
    "sql": "SELECT id, name FROM user_table WHERE status='active'",
    "username": "root",
    "password": "123456",
    "splitColumn": "id",
    "splitSize": 10000,
    "fetchSize": 1000,
    "queryTimeout": 300
  }
}
```

### 4.3 配置字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| url | String | 是 | JDBC 连接 URL |
| table | String | 否* | 表名（与 sql 二选一） |
| sql | String | 否* | 自定义 SQL（与 table 二选一） |
| username | String | 是 | 用户名 |
| password | String | 是 | 密码 |
| splitColumn | String | 是 | 分片列名（主键） |
| splitSize | Integer | 是 | 每个分片的行数 |
| fetchSize | Integer | 否 | JDBC fetch size |
| queryTimeout | Integer | 否 | 查询超时（秒） |

\* table 和 sql 必须提供一个，不能同时为空

---

## 5. 核心类设计

### 5.1 JdbcSource

继承 `AbstractRangeSplitSource<Row>`，实现：

- `getSplitColumnRange()` - 查询分片列的 MIN/MAX 值
- `createEnumerator()` - 创建分片枚举器
- `createReader()` - 创建数据读取器

**关键实现**：

```java
@Override
protected Range<Long> getSplitColumnRange() {
    String querySql = dialect.buildRangeQuery(table, sql, splitColumn);

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
```

### 5.2 JdbcSourceReader

实现 `SourceReader<Row, RangeSplit>` 接口：

**核心方法**：
- `start()` - 初始化数据库连接
- `pollNext()` - 读取下一行数据并转换为 Row
- `addSplits()` - 接收新分片并执行查询
- `close()` - 关闭连接和资源

**执行流程**：
1. 接收分片（addSplits）
2. 构建 SQL 并执行查询（executeQuery）
3. 轮询 ResultSet，逐行转换为 Row（pollNext）
4. 分片读取完毕后关闭资源

### 5.3 JdbcSplitEnumerator

实现 `SplitEnumerator` 接口：

**职责**：
- 管理所有分片
- 响应 Reader 的分片请求
- 分配分片给对应的 Reader

**实现逻辑**：
```java
@Override
public void handleSplitRequest(int subtaskId, String requesterHostname) {
    if (currentSplitIndex < splits.size()) {
        RangeSplit split = splits.get(currentSplitIndex++);
        context.assignSplit(split, subtaskId);
    } else {
        context.signalNoMoreSplits(subtaskId);
    }
}
```

---

## 6. 数据流执行流程

```
1. CLI 启动，读取配置文件
   ↓
2. PluginLoader 加载 MySQLSourcePlugin
   ↓
3. MySQLSourcePlugin.createSource()
   → 创建 JdbcSource（传入 MySQLDialect）
   ↓
4. JdbcSource.getSplitColumnRange()
   → 执行 SQL: SELECT MIN(id), MAX(id) FROM table
   ↓
5. JdbcSource.calculateSplits()
   → 计算所有分片 [id_1_10000, id_10001_20000, ...]
   ↓
6. JdbcSplitEnumerator 分配分片给 Reader
   ↓
7. JdbcSourceReader 接收分片
   → 执行查询: SELECT * FROM table WHERE id BETWEEN 1 AND 10000
   ↓
8. ResultSet → Row 转换（通过 MySQLDialect.createRow()）
   ↓
9. Row 发送到下游 Transform
   ↓
10. Transform 处理后发送到 Sink
```

---

## 7. 错误处理策略

### 7.1 连接失败

- 抛出 `RuntimeException`，快速失败
- 记录详细错误信息（URL、用户名、错误原因）

### 7.2 SQL 执行失败

- 抛出 `RuntimeException`，包含 SQL 语句和错误信息
- 关闭当前连接，释放资源

### 7.3 数据类型转换失败

- 在 `createRow()` 方法中捕获异常
- 记录字段名、类型、值等详细信息

### 7.4 配置错误

- 表名和 SQL 同时为空 → 启动时验证失败
- 分片列不存在 → 查询执行时失败

---

## 8. 性能优化

### 8.1 支持的性能参数

| 参数 | 说明 | 建议值 |
|------|------|--------|
| fetchSize | JDBC fetch size，控制每次从数据库获取的行数 | 1000 或 Integer.MIN_VALUE（MySQL 流式读取） |
| queryTimeout | 查询超时时间（秒） | 根据数据量调整，建议 300 |

### 8.2 优化建议

**MySQL 特定优化**：
- 设置 `fetchSize=Integer.MIN_VALUE` 启用流式读取
- 避免大量数据加载到内存

**通用优化**：
- 分片列必须有索引
- 分片大小根据数据量和性能调整
- 建议 10000-50000 行/分片

**查询优化**：
- 自定义 SQL 避免使用 `SELECT *`，只查询需要的列
- 添加适当的 WHERE 条件减少数据量

---

## 9. 扩展性设计

### 9.1 新增数据库支持

只需实现 `JdbcDialect` 接口并创建插件模块：

```java
public class PostgreSQLDialect implements JdbcDialect {
    // 实现 PostgreSQL 特定的 SQL 构建逻辑
}

public class PostgreSQLSourcePlugin implements SourcePlugin {
    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        return new JdbcSource(config, new PostgreSQLDialect());
    }
}
```

### 9.2 未来扩展方向

**短期**：
- Oracle、PostgreSQL 方言实现
- 连接池支持（HikariCP）

**中期**：
- 哈希分片策略
- 增量读取支持

**长期**：
- CDC（变更数据捕获）支持
- 实时流式读取

---

## 10. 测试策略

### 10.1 单元测试

- JdbcDialect SQL 构建测试
- 分片计算逻辑测试
- Row 创建和类型转换测试

### 10.2 集成测试

- MySQL 数据库连接测试
- 分片读取完整性测试
- 性能参数配置测试

### 10.3 端到端测试

- 完整 ETL 流程测试（MySQL → Console）
- 自定义 SQL 模式测试
- 错误处理测试

---

## 11. 总结

本设计实现了一个灵活、高性能的 JDBC Source 插件框架：

**核心优势**：
- ✓ 分层架构，核心逻辑复用
- ✓ 易于扩展，新增数据库成本低
- ✓ 配置驱动，使用简单
- ✓ 性能优化，支持大数据量读取
- ✓ 错误处理完善，快速失败

**实现路径**：
Phase 1 - 实现 JDBC Core 和 MySQL 方言（本次）
Phase 2 - 添加其他数据库方言和优化（后续）

该设计为 Flink ETL 工具提供了可靠的关系型数据源支持。