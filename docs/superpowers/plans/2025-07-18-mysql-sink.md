# 实现计划：MySQL Sink 插件

## 概述

新增 `flink-etl-sink-mysql` 模块，实现将 Flink `Row` 数据写入 MySQL 数据库的 Sink 插件。

参照现有架构：
- Source 侧：`flink-etl-source-jdbc`（JDBC 核心抽象）+ `flink-etl-source-mysql`（MySQL 插件）
- Sink 侧：新建 `flink-etl-sink-mysql`，复用 JDBC 连接逻辑，不需要单独的 jdbc-sink 抽象层

---

## 文件结构

### 新建文件

```
flink-etl-sink/
  flink-etl-sink-mysql/
    pom.xml
    src/main/java/com/etl/sink/mysql/
      MySQLSinkPlugin.java          # SinkPlugin 实现，类型标识 "mysql"
      MySQLSinkFunction.java        # RichSinkFunction，负责连接和写入
    src/main/resources/META-INF/services/
      com.etl.core.spi.SinkPlugin   # SPI 注册文件
    src/test/java/com/etl/sink/mysql/
      MySQLSinkFunctionTest.java    # 单元测试（用 H2 内存数据库）
docs/examples/
  mysql-to-mysql.json              # 示例配置
```

### 修改文件

```
flink-etl-sink/pom.xml            # 添加 flink-etl-sink-mysql 子模块
pom.xml                           # 确认 mysql-connector-java 已在 dependencyManagement（已存在，无需改动）
```

---

## 配置参数

Sink 配置中 `type: "mysql"`，`config` 字段支持：

| 参数 | 必填 | 说明 |
|---|---|---|
| `url` | 是 | JDBC URL，如 `jdbc:mysql://host:3306/db` |
| `username` | 是 | 数据库用户名 |
| `password` | 是 | 数据库密码 |
| `table` | 是 | 目标表名 |
| `columns` | 是 | 写入列名列表（逗号分隔字符串或 List） |
| `batchSize` | 否 | 批量写入大小，默认 100 |
| `writeMode` | 否 | `insert`（默认）或 `upsert` |

---

## 实现任务

### Task 1 — 创建 Maven 模块骨架

**文件：** `flink-etl-sink/flink-etl-sink-mysql/pom.xml`

参照 `flink-etl-sink/flink-etl-sink-console/pom.xml`，parent 指向 `flink-etl-sink`，添加以下依赖：

```xml
<dependencies>
  <!-- 核心模块（继承自 parent） -->

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

  <!-- 测试 -->
  <dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <scope>test</scope>
  </dependency>
  <dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <version>2.2.224</version>
    <scope>test</scope>
  </dependency>
</dependencies>
```

**文件：** `flink-etl-sink/pom.xml`

在 `<modules>` 中添加：

```xml
<module>flink-etl-sink-mysql</module>
```

**验证：** `mvn compile -pl flink-etl-sink/flink-etl-sink-mysql` 能编译通过（空模块）。

**提交：** `feat: 添加 flink-etl-sink-mysql 模块骨架`

---

### Task 2 — 实现 MySQLSinkFunction（TDD）

**先写测试，再写实现。**

#### 2a. 写测试

**文件：** `src/test/java/com/etl/sink/mysql/MySQLSinkFunctionTest.java`

使用 H2 内存数据库（H2 支持 MySQL 兼容模式 `MODE=MySQL`）。

测试覆盖：

```java
// 需要覆盖的测试场景：

// 1. invoke_insertsRowToDatabase
//    构造 SinkConfig（url=H2, table=test, columns=id,name）
//    调用 open() 初始化连接
//    调用 invoke() 写入一条 Row
//    调用 close() 刷新并关闭
//    断言：SELECT * FROM test 返回该行

// 2. invoke_batchFlush_whenBatchSizeReached
//    设置 batchSize=3
//    写入 3 条数据
//    close() 之前查询，断言已写入（批满自动刷新）

// 3. invoke_flushOnClose_whenBatchNotFull
//    设置 batchSize=10
//    写入 2 条数据
//    close() 后查询，断言 2 条均写入（关闭时刷新未满批次）

// 4. open_throwsException_whenUrlInvalid
//    配置错误的 url
//    断言 open() 抛出 RuntimeException
```

H2 测试连接配置参考（MySQL 兼容模式）：
```
url: jdbc:h2:mem:testdb;MODE=MySQL;DB_CLOSE_DELAY=-1
username: sa
password:
```

#### 2b. 实现 MySQLSinkFunction

**文件：** `src/main/java/com/etl/sink/mysql/MySQLSinkFunction.java`

关键设计：
- 继承 `RichSinkFunction<Row>`（而非 `SinkFunction`），以使用 `open/close` 生命周期管理连接
- `open()` 中建立 JDBC 连接，构建 `INSERT INTO table (col1, col2, ...) VALUES (?, ?, ...)` 的 `PreparedStatement`
- `invoke()` 将 `Row` 的各字段按 columns 顺序绑定到 PreparedStatement，添加到批次；批次满时调用 `executeBatch()`
- `close()` 刷新剩余批次，关闭 Statement 和 Connection
- 列名列表通过 `SinkConfig.getString("columns")` 读取，按逗号分隔解析

```java
public class MySQLSinkFunction extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final List<String> columns;   // 写入列顺序
    private final int batchSize;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient int pendingCount;

    // open(): 建立连接，prepare INSERT SQL
    // invoke(): 绑定行数据到 statement，addBatch()，到达 batchSize 时 executeBatch()
    // close(): executeBatch() 处理剩余数据，关闭资源
    // buildInsertSql(): 构建 INSERT SQL 字符串（私有方法）
}
```

注意：`Row.getField(i)` 返回 `Object`，直接用 `statement.setObject(index, value)` 绑定。

**验证：** `mvn test -pl flink-etl-sink/flink-etl-sink-mysql` 全部通过。

**提交：** `feat: 实现 MySQLSinkFunction，支持批量写入`

---

### Task 3 — 实现 MySQLSinkPlugin

**文件：** `src/main/java/com/etl/sink/mysql/MySQLSinkPlugin.java`

实现 `SinkPlugin` 接口，极简：

```java
public class MySQLSinkPlugin implements SinkPlugin {
    private static final long serialVersionUID = 1L;

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public SinkFunction<?> createSink(SinkConfig config) {
        return new MySQLSinkFunction(config);
    }
}
```

`MySQLSinkFunction` 构造器接收 `SinkConfig`，在内部提取所有参数。

**文件：** `src/main/resources/META-INF/services/com.etl.core.spi.SinkPlugin`

```
com.etl.sink.mysql.MySQLSinkPlugin
```

**验证：** `mvn compile -pl flink-etl-sink/flink-etl-sink-mysql`

**提交：** `feat: 添加 MySQLSinkPlugin，注册 SPI`

---

### Task 4 — 添加示例配置

**文件：** `docs/examples/mysql-to-mysql.json`

```json
{
  "job": {
    "name": "mysql-to-mysql",
    "mode": "batch",
    "parallelism": 1
  },
  "source": {
    "type": "mysql",
    "config": {
      "url": "jdbc:mysql://source-host:3306/source_db",
      "table": "source_table",
      "username": "root",
      "password": "password",
      "splitColumn": "id"
    }
  },
  "sink": {
    "type": "mysql",
    "config": {
      "url": "jdbc:mysql://target-host:3306/target_db",
      "table": "target_table",
      "username": "root",
      "password": "password",
      "columns": "id,name,email",
      "batchSize": 100
    }
  }
}
```

**提交：** `docs: 添加 mysql-to-mysql 示例配置`

---

## 测试策略

- 单元测试使用 H2 内存数据库，不依赖真实 MySQL，CI 可直接运行
- H2 的 `MODE=MySQL` 兼容 MySQL 语法（`INSERT INTO ... VALUES (...)`）
- 无需集成测试：现有 `mysql-to-console.json` 示例可用于手动端到端验证
- 测试在 `flink-etl-sink-mysql` 模块内，不影响其他模块

运行所有测试：
```bash
mvn test -pl flink-etl-sink/flink-etl-sink-mysql
```

运行全量构建：
```bash
mvn clean install
```

---

## 注意事项

1. **Column 顺序**：`Row.getField(i)` 按索引访问字段，`columns` 参数的顺序必须与 Source 输出的 Row 字段顺序一致。如果中间有 Transform（field-mapping），需要确认映射后的字段顺序。

2. **upsert 模式**（可选扩展）：`writeMode=upsert` 时改用 `INSERT INTO ... ON DUPLICATE KEY UPDATE ...` 语法。Task 2 可先只实现 `insert` 模式，upsert 作为后续扩展。

3. **序列化**：`MySQLSinkFunction` 中 `connection` 和 `statement` 标记为 `transient`，避免序列化问题（`RichSinkFunction` 会在 TaskManager 上反序列化后调用 `open()`）。

4. **现有 SinkPlugin 接口**：`SinkPlugin.createSink()` 返回 `SinkFunction<?>`，`RichSinkFunction` 是 `SinkFunction` 的子类，类型兼容，无需修改接口。
