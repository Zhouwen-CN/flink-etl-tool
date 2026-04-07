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
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-console.json

# 运行带变量替换的 ETL 任务
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file docs/examples/mysql-to-console.json \
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
├── flink-etl-core/               # 核心框架（SPI 接口、配置解析、Job 编排、Source 抽象层）
├── flink-etl-client/             # 客户端启动器（打包入口）
├── flink-etl-source/             # Source 插件父模块
│   ├── flink-etl-source-jdbc/    # JDBC 通用实现
│   ├── flink-etl-source-localfile/  # 本地文件 Source
│   ├── flink-etl-source-http/    # HTTP Source
│   └── flink-etl-source-kafka/   # Kafka Source
├── flink-etl-sink/               # Sink 插件父模块
│   ├── flink-etl-sink-console/   # Console Sink
│   ├── flink-etl-sink-jdbc/      # JDBC Sink
│   └── flink-etl-sink-kafka/     # Kafka Sink
└── flink-etl-transform/          # Transform 插件（SQL Transform）
```

### 核心执行流程

1. `EtlClient.main()` 解析命令行参数
2. `CliArgumentParser.parse()` 解析参数 → 配置来源
3. `ConfigParser` 解析 JSON 配置 → `JobConfig`
4. `JobExecutor` 创建 Flink 执行环境和 Table 环境
5. `JobBuilder.build()` 构建处理链：Source → DataStream<Row> → Table → Transform → Sink
6. 提交到 Flink 执行引擎运行

### SPI 插件机制

项目使用 Java SPI (`ServiceLoader`) + `@AutoService` 注解实现插件化。核心接口：

- **SourcePlugin**: 数据源插件，创建 Flink Source
- **SinkPlugin**: 数据写入插件，创建 Flink SinkFunction<Row>
- **TransformPlugin**: 数据转换插件，基于 Table API 进行 SQL 转换

插件加载通过 [PluginLoader.java](flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java) 实现。

### Source 抽象层架构

简化 Flink FLIP-27 Source API 的实现：

**核心抽象类（core 模块）：**
- `AbstractSplitSource<SplitT, CheckpointT>` - Source 基类
- `BaseSplitEnumerator` - 分片枚举器基类（自动处理分片分配和回收）
- `BaseSourceReader` - 源阅读器基类（封装线程模型和状态管理）
- `BaseSplitReader` - 分片读取器接口（阻塞式数据读取）

**数据类型：** 所有 Source 直接使用 Flink `Row` 类型输出，通过 `ResultTypeQueryable<Row>` 提供 RowTypeInfo。

### Sink 抽象层架构

简化 Flink Sink API 的实现，采用 at-least-once 语义：

**核心抽象类（core 模块）：**
- `AbstractSink` - Sink 基类
- `AbstractSinkWriter<ConfigT>` - SinkWriter 基类

### 扩展新数据源

1. 创建新模块，依赖 `flink-etl-core`
2. 实现 `SourcePlugin` 接口，添加 `@AutoService(SourcePlugin.class)` 注解
3. 继承 `AbstractSplitSource` 实现分片读取
   - 关系型数据库：参考 `JdbcSource`，分片逻辑在 Enumerator 的 `start()` 方法中计算
   - 文件类：参考 `LocalFileSource`
4. 创建配置封装类（实现 `Serializable`），在 Source 构造函数中集中校验参数
5. 在 `flink-etl-client/pom.xml` 添加新模块依赖

**设计要点：** 参数校验集中在 Source 构造函数；配置对象使用 `final` 字段 + `@Builder`；Enumerator/Reader 只关注业务逻辑。

### 扩展新 Sink

1. 创建新模块，依赖 `flink-etl-core`
2. 实现 `SinkPlugin` 接口，添加 `@AutoService(SinkPlugin.class)` 注解
3. 继承 `AbstractSink`，在构造函数中进行参数校验和配置对象构建
4. 继承 `AbstractSinkWriter<ConfigT>` 实现 `write()`、`flush()`、`close()` 方法
5. 在 `flink-etl-client/pom.xml` 添加新模块依赖

**设计要点：** 参数校验集中在 Sink 构造函数；批量 Sink 自行管理 batchSize；异常时抛出 IOException，Flink 会从 checkpoint 重试。

## 开发实践

### Schema 配置

- JDBC Source 可自动推断 Schema，其他 Source 必须显式配置
- Schema 类型支持（简单类型、ARRAY、OBJECT）详见 [PLUGINS.md#schema-配置](PLUGINS.md#schema-配置)

### 类型转换

- JDBC ResultSet → `TypeConverter.convertFromValue()`
- JSON 数据 → `JsonToRowConverter.convertJsonToRows()`
- SQL 类型字符串 → `SqlTypeConverter.toFlinkType()`
- Row 转 JSON → `RowToJsonConverter.convertRowToJsonNode()`

### 异常处理

| 场景 | Source | Sink |
|------|--------|------|
| 读取/写入失败 | 抛出异常，Reader 重试 | 抛出 `IOException`，checkpoint 重试 |
| 配置校验失败 | Source 构造函数抛 `IllegalArgumentException` | Sink 构造函数抛 `IllegalArgumentException` |

Sink 异常处理详见 [PLUGINS.md#sink-插件开发指南](PLUGINS.md#sink-插件开发指南)。

### 测试规范

- 测试文件位置：`src/test/java/`，镜像 `src/main/java/` 结构
- 测试类命名：`<ClassName>Test.java`
- 使用 JUnit 5：`@Test`、`@BeforeEach`、`assertThrows()`
- 测试覆盖：配置解析、类型转换、Dialect SQL 生成、Schema 校验
- JDBC 测试：使用 Mock 或 H2 测试数据库
- Flink 测试：参考 `AbstractSinkWriterTest` 使用 MiniCluster
- Kafka 测试：使用 EmbeddedKafka 或 Mock Consumer/Producer

### 插件打包

- 新插件添加依赖：在 `flink-etl-client/pom.xml` 添加模块依赖
- 依赖管理：MySQL/OceanBase 驱动已包含，Oracle 需手动添加；Kafka connector 使用 Flink 版本；CSV 使用 Commons CSV；JSONPath 用于 HTTP Source

## 文档维护

**重要：** 每次修改或新增 Source、Sink、Transform 插件时，必须同步更新 [PLUGINS.md](PLUGINS.md) 文档。

## 配置文件格式

配置采用 DataX 风格的 JSON 结构。**详细配置参数和示例请参考 [PLUGINS.md](PLUGINS.md)。**

```json
{
  "job": {
    "name": "job-name",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [{ "type": "...", "outputTable": "...", "config": {...} }],
  "transforms": [{ "type": "sql", "outputTable": "...", "config": { "sql": "..." } }],
  "sinks": [{ "type": "...", "inputTable": "...", "config": {...} }]
}
```

**数据流转机制：**
- `sources` → 每个 Source 的 `outputTable` 注册为 Table
- `transforms` → 链式处理，SQL 中引用上游的 `outputTable`
- `sinks` → 从 `inputTable` 读取数据写入目标

**job 配置项：**
- `name`: Job 名称
- `mode`: `batch` 或 `streaming`
- `parallelism`: 并行度（可选），分片数量等于并行度

**变量替换：**
配置支持变量替换，通过命令行参数动态传递值：
- 格式：`${variable}` 或 `${variable:-default}`（带默认值）
- 变量值通过命令行参数传递（例如：`--db_url xxx`）
- 未定义变量（无默认值）会抛异常，明确提示缺失参数

示例配置：
```json
{
  "sources": [{
    "config": {
      "url": "${db_url}",
      "user": "${db_user:-root}",
      "password": "${db_password}"
    }
  }]
}
```

运行命令：
```bash
--db_url jdbc:mysql://localhost:3306/test --db_password secret
```

## 技术栈

- Java 1.8
- Apache Flink 1.15.2
- Flink Table API
- SLF4J + Log4j2
- Maven