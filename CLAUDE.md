# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个基于 Apache Flink 的配置驱动 ETL 工具，类似于 DataX 和 SeaTunnel。通过 JSON 配置文件驱动数据同步任务，支持批处理和流处理模式。

## 常用命令

```bash
# 编译项目
mvn clean compile

# 打包项目（生成可执行 JAR）
mvn clean package

# 运行 ETL 任务
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/batch-mysql2console.json

# 运行带变量替换的 ETL 任务
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file docs/examples/batch-mysql2console.json \
  --db_url jdbc:mysql://localhost:3306/test \
  --db_user root \
  --db_password secret

# 运行所有测试
mvn test

# 运行单个测试类
mvn test -Dtest=ConfigParserTest

# 安装到本地仓库（开发新插件时）
mvn clean install -DskipTests
```

## 架构概览

### 模块结构

```
flink-etl-tool/
├── flink-etl-core/               # 核心框架
├── flink-etl-client/             # 客户端启动器
├── flink-etl-connector/          # 连接器插件
│   ├── connector-jdbc/           # JDBC 连接器（Source + Sink + Dialect）
│   ├── connector-kafka/          # Kafka 连接器（Source + Sink）
│   ├── connector-cdc/            # CDC 连接器（MySQL CDC Source）
│   ├── connector-localfile/      # 本地文件连接器（Source）
│   ├── connector-console/        # Console 连接器（Sink）
│   ├── connector-http/           # HTTP 连接器（Source）
│   ├── connector-mock/           # Mock 连接器（Source）
│   ├── connector-modbus/         # Modbus 连接器（Source）
│   └── connector-mqtt/           # MQTT 连接器（Source）
└── flink-etl-transform/          # Transform 插件（SQL Transform）
```

### 核心执行流程

```
EtlClient.main()
  └─ CliArgumentParser.parse()        # 解析命令行 → JobConfig（支持变量替换）
       └─ ConfigParser.parse()        # 校验配置完整性（outputTable/inputTable 链路）
            └─ JobExecutor.execute()
                 ├─ 创建 StreamExecutionEnvironment（batch/streaming 模式）
                 ├─ 加载所有 UDF（通过 ServiceLoader 自动扫描）
                 └─ JobBuilder.build()
                      ├─ 1. Source → DataStream<Row> → Table（注册为 outputTable）
                      ├─ 2. Transform 链（SQL 引用的表名就是上游的 outputTable）
                      └─ 3. Table → DataStream<Row> → Sink（从 inputTable 读取）
```

### 数据流转机制

- `sources[].outputTable` → Source 输出注册为 Flink Table
- `transforms[].inputTable`（SQL 中引用的表名）→ 从上游 Table 读取
- `transforms[].outputTable` → Transform 结果注册为中间表
- `sinks[].inputTable` → 从该 Table 读取数据写入 Sink
- **校验规则**：`inputTable` 必须在上游 `outputTable` 中定义；多个 Source/Transform 的 `outputTable` 不可重复

### SPI 插件机制

使用 Java SPI (`ServiceLoader`) + `@AutoService` 注解实现插件化：

| 接口 | 作用 | 加载方式 |
|------|------|---------|
| `SourcePlugin` | 创建 Flink Source | `PluginLoader.loadSourcePlugin(type)` |
| `SinkPlugin` | 创建 Flink Sink | `PluginLoader.loadSinkPlugin(type)` |
| `TransformPlugin` | 基于 Table API 做 SQL 转换 | `PluginLoader.loadTransformPlugin(type)` |
| `UdfPlugin` | 创建 Flink UDF | `PluginLoader.loadAllUdfPlugins()` 批量加载 |

### Source 抽象层

简化 Flink FLIP-27 Source API，核心类在 `flink-etl-core/source/`：

- `AbstractSplitSource<SplitT, CheckpointT>` — Source 基类
- `AbstractSplitEnumerator` — 分片枚举器（自动处理分片分配和回收）
- `AbstractSourceReader` — 源阅读器（封装线程模型和状态管理）
- `AbstractSplitReader<SplitT>` — 分片读取器（阻塞式数据读取）

所有 Source 输出 `Row` 类型，通过 `ResultTypeQueryable<Row>` 提供类型信息。

### Sink 抽象层

使用 `AbstractSink` + `AbstractSinkWriter<ConfigT>` 基类，采用 at-least-once 语义：

- `AbstractSinkWriter` 只提供 `context` 和 `config` 字段访问，子类自行实现 `write()`、`flush()`、`close()`
- `context` 可获取 subtaskId、并行度、metricGroup
- 异常时抛出 `IOException`，Flink 从 checkpoint 重试

### 数据库方言抽象

`connector-jdbc` 模块中的 `JdbcDialect` 接口封装各数据库差异：

- `MySQLDialect` — backtick 转义、`ON DUPLICATE KEY UPDATE` upsert
- `PostgreSQLDialect` — 双引号转义、`ON CONFLICT` upsert
- `OracleDialect` — 双引号转义、`MERGE INTO` upsert
- `H2Dialect` — 测试用

`JdbcDialectLoader` 根据 JDBC URL 自动识别方言，也支持通过 `dialect` 参数显式指定。

## 扩展新插件

### 扩展新 Source

1. 在 `flink-etl-connector/` 下创建新模块，依赖 `flink-etl-core`
2. 包名规范：com.etl.connector.`连接器名称`.source，比如 `com.etl.connector.jdbc.source`
   - 配置类放在 `config/` 子包，比如 `com.etl.connector.jdbc.source.config.JdbcSourceConfig`
3. 实现 `SourcePlugin`，添加 `@AutoService(SourcePlugin.class)` 注解
4. 继承 `AbstractSplitSource` 实现分片读取
   - 关系型数据库：参考 `connector-jdbc` 模块的 `JdbcSource`，分片逻辑在 Enumerator 的 `start()` 中计算
   - 文件类：参考 `connector-localfile` 模块的 `LocalFileSource`
5. **架构规则（重要）**：
   - **配置参数校验分离**：配置类（如 `JdbcSourceConfig`）提供静态方法 `fromSourceConfig(SourceConfig config)`
     ，在此方法中完成所有参数校验、类型转换和推断逻辑，Source 构造函数只调用该方法
   - **Split 包含完整数据**：Split 类必须包含 Reader 执行所需的所有信息（连接参数、配置等），Reader 不通过构造函数接收配置，而是从
     Split 中获取
6. 在 `flink-etl-client/pom.xml` 添加模块依赖

### 扩展新 Sink

1. 在 `flink-etl-connector/` 下创建新模块，依赖 `flink-etl-core`
2. 包名规范：com.etl.connector.`连接器名称`.sink，比如 `com.etl.connector.jdbc.sink`
    - 配置类放在 `config/` 子包，比如 `com.etl.connector.jdbc.sink.config.JdbcSinkConfig`
3. 实现 `SinkPlugin`，添加 `@AutoService(SinkPlugin.class)` 注解
4. 继承 `AbstractSink`，实现 `createWriter(InitContext context)`
5. 继承 `AbstractSinkWriter<ConfigT>` 实现 `write()`、`flush()`、`close()`
6. 配置参数校验分离，配置类（如 `JdbcSinkConfig`）提供静态方法 `fromSinkConfig(SinkConfig config)`
   ，在此方法中完成所有参数校验、类型转换和推断逻辑，Sink 构造函数只调用该方法
7. 在 `flink-etl-client/pom.xml` 添加模块依赖

### 扩展新 UDF

1. 在 `flink-etl-core/src/main/java/com/etl/core/udf/` 对应目录创建类：
   - 标量函数：`scalar/`（ScalarFunction）
   - 表值函数：`table/`（TableFunction）
   - 聚合函数：`agg/`（AggregateFunction）
   - 表值聚合函数：`tagg/`（TableAggregateFunction）
2. 实现 `UdfPlugin`，添加 `@AutoService(UdfPlugin.class)` 注解
3. `identifier()` 返回 SQL 中使用的函数名；`createFunction()` 返回 Flink UDF 实例
4. 编译安装：`mvn clean install -DskipTests`（生成 SPI 配置文件）
5. Job 启动时自动扫描并注册所有 UDF

## 类型转换

项目中有多个独立的类型转换模块，各司其职：

| 模块 | 作用 |
|------|------|
| `TypeConverter` | JDBC ResultSet → Java 对象 |
| `JsonToRowConverter` | JSON 数据 → `Row` 对象 |
| `RowToJsonConverter` | `Row` 对象 → JSON（用于 Kafka Sink） |
| `SqlTypeConverter` | SQL 类型字符串 → Flink `LogicalType` |

支持的 Schema 类型：简单类型（STRING、BOOLEAN、INT、LONG、DOUBLE、DECIMAL、TIMESTAMP）、ARRAY（`["TYPE"]`）、OBJECT（嵌套结构）、ARRAY\<OBJECT\>。

## 测试规范

- 测试文件位置：`src/test/java/`，镜像 `src/main/java/` 结构
- 测试类命名：`<ClassName>Test.java`，使用 JUnit 5
- 测试范围：专注于功能测试，例如单个函数、工具类的单元测试
- 集成测试：不建议针对整个任务流程进行测试，例如根据配置文件启动完整 job 的测试场景

## 文档维护

**重要：** 每次修改或新增 Source、Sink、Transform、UDF 插件时，必须同步更新 [PLUGINS.md](PLUGINS.md) 文档。

**设计文档：** 重要架构决策位于 `docs/superpowers/specs/`，开发计划位于 `docs/superpowers/plans/`。

**示例配置：** `docs/examples/` 目录包含 batch 和 streaming 模式的多个示例场景，覆盖 JDBC、Kafka、CDC、HTTP、Mock、Modbus、MQTT 等连接器。

## 技术栈

- Java 1.8
- Apache Flink 1.15.2（Flink Table API、FLIP-27 Source API）
- SLF4J + Log4j2
- Maven