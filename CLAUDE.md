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

# 运行 ETL 任务（从文件加载配置）
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-console.json

# 安装到本地仓库（开发新插件时需要）
mvn clean install -DskipTests
```

## 架构概览

### 模块结构

```
flink-etl-tool/
├── flink-etl-core/               # 核心框架（SPI 接口、配置解析、Job 编排、Source 抽象层）
├── flink-etl-client/             # 客户端启动器（打包入口）
├── flink-etl-source/             # Source 插件父模块
│   ├── flink-etl-source-jdbc/    # JDBC 通用实现（支持 MySQL、PostgreSQL 等关系型数据库）
│   └── flink-etl-source-localfile/  # 本地文件 Source 插件
├── flink-etl-sink/               # Sink 插件父模块
│   ├── flink-etl-sink-console/   # Console Sink 插件
│   └── flink-etl-sink-jdbc/      # JDBC Sink 插件（支持 MySQL、PostgreSQL 等）
└── flink-etl-transform/          # Transform 插件（SQL Transform）
```

### 核心执行流程

1. `EtlClient.main()` 解析命令行参数
2. `CliArgumentParser.parse()` 静态方法解析参数 → 配置来源
3. `ConfigParser` 解析 JSON 配置 → `JobConfig`
4. `JobExecutor` 创建 Flink 执行环境和 Table 环境
5. `JobBuilder.build()` 静态方法构建处理链：
   - Source → DataStream<Row> → 注册为 Table
   - Transform 链式处理（SQL）
   - Table → DataStream<Row> → Sink
6. 提交到 Flink 执行引擎运行

### SPI 插件机制

项目使用 Java SPI (`ServiceLoader`) 实现插件化。核心接口：

- **SourcePlugin**: 数据源插件，创建 Flink Source
- **SinkPlugin**: 数据写入插件，创建 Flink SinkFunction<Row>
- **TransformPlugin**: 数据转换插件，基于 Table API 进行 SQL 转换

插件加载通过 [PluginLoader.java](flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java) 静态方法实现：
- `PluginLoader.loadSourcePlugin(type)`
- `PluginLoader.loadTransformPlugin(type)`
- `PluginLoader.loadSinkPlugin(type)`

### Source 抽象层架构

项目实现了完整的 Source 抽象层，简化 Flink FLIP-27 Source API 的实现：

**核心抽象类：**
- `AbstractSplitSource<SplitT, CheckpointT>` - Source 基类（core 模块）

**base 包组件（core 模块）：**
- `BaseSplitEnumerator` - 分片枚举器基类（自动处理分片分配和回收）
- `BaseSourceReader` - 源阅读器基类（封装线程模型和状态管理）
- `BaseSplitReader` - 分片读取器接口（阻塞式数据读取）
- `BaseSplitState` - 分片状态
- `BaseEnumCheckpoint` - 枚举器检查点
- `BaseSourceSplit` - 分片接口

**数据类型：**
- 所有 Source 直接使用 Flink `Row` 类型输出，无额外包装层
- 通过 `ResultTypeQueryable<Row>` 提供 RowTypeInfo，支持 Flink Table API 列名识别

### 扩展新数据源

添加新数据源需要：

1. 创建新模块，依赖 `flink-etl-core`
2. 实现 `SourcePlugin` 接口
3. 添加 `@AutoService(SourcePlugin.class)` 注解（自动生成 SPI 配置）
4. 继承 `AbstractSplitSource` 实现分片读取
   - 关系型数据库：参考 `JdbcSource` 实现，分片逻辑在 Enumerator 的 `start()` 方法中计算
   - 文件类：参考 `LocalFileSource` 实现
5. 创建配置封装类（如 `XxxSourceConfig`），实现 `Serializable`
6. 在 `flink-etl-client/pom.xml` 添加新模块依赖

**注意：** 使用 `@AutoService` 注解后，无需手动创建 `META-INF/services/` 目录下的服务配置文件，编译时会自动生成。

```java
import com.google.auto.service.AutoService;

@AutoService(SourcePlugin.class)
public class MySourcePlugin implements SourcePlugin {
    // ...
}
```

**示例代码：**

```java
// 1. 配置类定义
@Getter
@Builder
public class JdbcSourceConfig implements Serializable {
    private final String url;
    private final String username;
    private final String password;
    private final String splitColumn;
    private final Integer batchSize;
    private final JdbcDialect dialect;
}

// 2. Source 构造函数中完成校验和封装
public class JdbcSource extends AbstractSplitSource<...> {
    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSource(SourceConfig config, JdbcDialect dialect) {
        super(config);

        // 参数校验
        String url = config.getString("url");
        Preconditions.checkNotNull(url, "url is null");

        String splitColumn = config.getString("splitColumn");
        Preconditions.checkNotNull(splitColumn, "splitColumn is null");

        Integer batchSize = config.getInteger("batchSize", getDefaultBatchSize());
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        // 封装成配置对象
        jdbcSourceConfig = JdbcSourceConfig.builder()
                .url(url)
                .splitColumn(splitColumn)
                .batchSize(batchSize)
                .dialect(dialect)
                .build();
    }

    // 3. 传递给下游组件
    @Override
    public SplitEnumerator<...> createEnumerator(...) {
        return new JdbcSplitEnumerator(enumContext, jdbcSourceConfig);
    }

    @Override
    public SourceReader<...> createReader(...) {
        return new JdbcSourceReader(() -> new JdbcSplitReader(jdbcSourceConfig), ...);
    }
}
```

**设计要点：**
- **单一校验点**：所有参数校验集中在 Source 构造函数，避免在 Enumerator/Reader 中重复校验
- **不可变配置**：配置对象使用 `final` 字段 + `@Builder`，保证线程安全
- **可序列化**：配置对象实现 `Serializable`，支持 Flink 检查点序列化
- **职责分离**：Source 负责配置解析校验，Enumerator/Reader 只关注业务逻辑

## 文档维护

**重要：** 每次修改或新增 Source、Sink、Transform 插件时，必须同步更新 [PLUGINS.md](PLUGINS.md) 文档，包括：
- 配置参数说明
- 配置示例
- 使用说明

## 配置文件格式

配置采用 DataX 风格的 JSON 结构，使用 Table API 进行数据流转：

```json
{
  "job": {
    "name": "job-name",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "jdbc",
      "outputTable": "source_table",
      "config": { ... }
    }
  ],
  "transforms": [
    {
      "type": "sql",
      "outputTable": "transformed_table",
      "config": {
        "sql": "SELECT * FROM source_table WHERE id > 0"
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "transformed_table",
      "config": {}
    }
  ]
}
```

**数据流转机制：**
- `sources` → 支持多个数据源，每个 Source 的 `outputTable` 注册为 Table
- `transforms` → 支持多个 Transform 组成处理链
- `sinks` → 支持多个数据目的地，每个 Sink 从 `inputTable` 读取数据写入目标
- SQL 中引用的表名为上游的 `outputTable`

**job 配置项说明：**
- `name`: Job 名称
- `mode`: 执行模式，支持 `batch`（批处理）或 `streaming`（流处理）
- `parallelism`: 并行度配置（可选），不设置则使用 Flink 默认值
  - 对于支持分片的 Source（如 JDBC），分片数量等于并行度
  - 如果数据量小于并行度，实际分片数会自动调整为数据量

**JDBC source 配置项：**
- `url`: 数据库连接 URL
- `table`: 表名（与 `sql` 二选一）
- `sql`: 自定义查询 SQL（与 `table` 二选一）
- `username`: 用户名
- `password`: 密码
- `splitColumn`: 分片列名（通常为主键）
- `batchSize`: 批量读取大小（可选，默认 100）
- `queryTimeout`: 查询超时时间（可选，单位秒）
- `schema`: 字段定义数组（可选，用于定义输出字段名和类型）

**LocalFile source 配置项：**
- `path`: 文件路径（支持通配符 `*`）
- `format`: 文件格式（如 `csv`）
- `encoding`: 文件编码（可选，默认 UTF-8）
- `delimiter`: CSV 分隔符（可选，默认 `,`）
- `skipHeader`: 是否跳过首行（可选，默认 true）
- `recursive`: 是否递归扫描目录（可选，默认 false）
- `schema`: 字段定义数组（必填）

**schema 格式：**
```json
"schema": [
  { "name": "id", "type": "INT" },
  { "name": "name", "type": "STRING" }
]
```

支持的类型：`INT`, `LONG`, `STRING`, `DOUBLE`, `BOOLEAN`, `DATE`, `TIMESTAMP`

**transforms 配置说明：**
- 支持多个 Transform 组成处理链
- 当前支持 `sql` 类型，通过 SQL 语句进行数据转换
- SQL 中引用的表名为上游的 `outputTable`

**JDBC Sink 配置项：**
- `url`: 数据库连接 URL
- `username`: 用户名
- `password`: 密码
- `table`: 目标表名（与 `sql` 二选一，优先）
- `sql`: 自定义 SQL，支持具名占位符 `:paramName`（与 `table` 二选一）
- `batchSize`: 批量写入大小（可选，默认 100）

JDBC Sink 支持两种模式：
- **table 模式**：自动生成 `INSERT INTO table(col1, col2, ...) VALUES(?, ?...)`，字段名从 Row 获取
- **sql 模式**：自定义 SQL，使用具名占位符，如 `INSERT INTO t(name, email) VALUES(:name, :email)`

示例配置位于 `docs/examples/` 目录。

## 关键抽象类

**Source 抽象层：**
- `AbstractSplitSource<SplitT, CheckpointT>`: Source 基类，封装 Flink FLIP-27 Source API

**Base 组件（core 模块）：**
- `BaseSplitEnumerator`: 分片枚举器基类，自动处理分片分配和回收
- `BaseSourceReader`: 源阅读器基类，封装线程模型和状态管理
- `BaseSplitReader`: 分片读取器接口，实现阻塞式数据读取

**序列化组件（core 模块）：**
- `SerializerUtils`: JDK 序列化工具类，封装对象序列化/反序列化逻辑
- `DefaultSplitSerializer`: 分片序列化器，使用 JDK 原生序列化
- `DefaultCheckpointSerializer`: 检查点序列化器，使用 JDK 原生序列化

**JDBC 实现（jdbc 模块）：**
- `JdbcSource`: JDBC Source 实现，直接继承 `AbstractSplitSource`
- `JdbcSourceConfig`: JDBC 配置封装类
- `JdbcSplitEnumerator`: 分片枚举器，在 `start()` 中延迟计算分片
- `JdbcSplitReader`: 分片读取器
- `RangeSplit`: 范围分片，表示数据范围 [start, end]
- `JdbcDialect`: 数据库方言接口
- `MySQLDialect`: MySQL 方言实现

**LocalFile 实现（localfile 模块）：**
- `LocalFileSource`: 本地文件 Source 实现，支持通配符路径匹配
- `LocalFileSourceConfig`: 本地文件配置封装类
- `LocalFileSplitEnumerator`: 文件扫描和分片枚举
- `LocalFileSplitReader`: 文件读取器
- `LocalFileSplit`: 文件分片，每个文件对应一个分片
- `FileFormatPlugin`: 文件格式解析插件接口（SPI）
- `CsvFormatPlugin`: CSV 格式解析实现

**Transform 实现：**
- `SqlTransformPlugin`: SQL Transform 插件，通过 Flink Table API 执行 SQL

## 技术栈

- Java 11
- Apache Flink 1.19.0
- Flink Table API（DataStream 与 Table 互转）
- Jackson 2.15.2 (JSON 解析)
- SLF4J + Log4j2 (日志)
- Maven (构建)