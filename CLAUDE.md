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
# 方式一：从文件加载配置（推荐）
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-console.json

# 方式二：从 JSON 字符串加载配置
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --config '{"job":{...},"source":{...},"sink":{...}}'

# 安装到本地仓库（开发新插件时需要）
mvn clean install -DskipTests
```

## 架构概览

### 模块结构

```
flink-etl-tool/
├── flink-etl-core/           # 核心框架（SPI 接口、配置解析、Job 编排、Source 抽象层）
├── flink-etl-client/         # 客户端启动器（打包入口）
├── flink-etl-source/         # Source 插件父模块
│   ├── flink-etl-source-jdbc/    # JDBC 通用实现
│   └── flink-etl-source-mysql/   # MySQL Source 插件
├── flink-etl-sink/           # Sink 插件父模块
│   └── flink-etl-sink-console/   # Console Sink 插件
└── flink-etl-transform/      # Transform 插件
```

### 核心执行流程

1. `EtlClient.main()` 解析命令行参数
2. `ConfigParser` 解析 JSON 配置 → `JobConfig`
3. `JobExecutor` 创建 Flink 执行环境
4. `JobBuilder` 通过 SPI 加载插件，构建 Source → Transform → Sink 处理链
5. 提交到 Flink 执行引擎运行

### SPI 插件机制

项目使用 Java SPI (`ServiceLoader`) 实现插件化。核心接口：

- **SourcePlugin**: 数据源插件，创建 Flink Source
- **SinkPlugin**: 数据写入插件，创建 Flink SinkFunction
- **TransformPlugin**: 数据转换插件，创建 MapFunction

插件加载通过 [PluginLoader.java](flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java) 实现。

### Source 抽象层架构

项目参考 Clink 项目设计，实现了完整的 Source 抽象层，简化 Flink FLIP-27 Source API 的实现：

**核心抽象类层次：**
- `AbstractSplitSource<T, SplitT, CheckpointT>` - Source 基类
  - `AbstractRangeSplitSource<T>` - 范围分片 Source（适用于关系型数据库）

**base 包组件：**
- `BaseSourceSplit` - 分片接口
- `BaseSplitState` - 分片状态
- `BaseEnumCheckpoint` - 枚举器检查点
- `BaseSplitEnumerator` - 分片枚举器基类（自动处理分片分配和回收）
- `BaseSourceReader` - 源阅读器基类（封装线程模型和状态管理）
- `BaseSplitReader` - 分片读取器接口（阻塞式数据读取）

**数据类型：**
- 所有 Source 直接使用 Flink `Row` 类型输出，无额外包装层
- 降低对象创建开销，与 Flink 生态更好集成

**扩展新数据源：**

添加新数据源需要：

1. 创建新模块，依赖 `flink-etl-core`
2. 实现 `SourcePlugin` 接口
3. 继承 `AbstractRangeSplitSource` 实现分片读取（关系型数据库）
   - 实现 `getSplitColumnRange()` 方法获取分片范围
   - 分片数量根据 Job 配置的 `parallelism` 自动计算
4. 添加 `META-INF/services/com.etl.core.spi.SourcePlugin` 文件
5. 在 `flink-etl-client/pom.xml` 添加新模块依赖

## 配置文件格式

配置采用 DataX 风格的 JSON 结构：

```json
{
  "job": {
    "name": "job-name",
    "mode": "batch",
    "parallelism": 4
  },
  "source": { "type": "mysql", "config": { ... } },
  "transform": { "type": "field-mapping", "config": { ... } },
  "sink": { "type": "console", "config": { ... } }
}
```

**job 配置项说明：**
- `name`: Job 名称
- `mode`: 执行模式，支持 `batch`（批处理）或 `streaming`（流处理）
- `parallelism`: 并行度配置（可选），不设置则使用 Flink 默认值
  - 对于支持分片的 Source（如 MySQL），分片数量等于并行度
  - 如果数据量小于并行度，实际分片数会自动调整为数据量

**source 配置项说明（以 MySQL 为例）：**
- `url`: 数据库连接 URL
- `table`: 表名（与 `sql` 二选一）
- `sql`: 自定义查询 SQL（与 `table` 二选一）
- `username`: 用户名
- `password`: 密码
- `splitColumn`: 分片列名（通常为主键）
- `fetchSize`: JDBC fetch size（可选，流式读取优化）
- `queryTimeout`: 查询超时时间（可选，单位秒）

**注意：**
- 旧版本的 `splitSize` 配置项已移除，分片数量现在根据 `parallelism` 自动计算
- 分片算法会根据数据范围和并行度自动优化，确保每个分片大小均衡

示例配置位于 `docs/examples/` 目录。

## 关键抽象类

**Source 抽象层：**
- `AbstractSplitSource<T, SplitT, CheckpointT>`: Source 基类，封装 Flink FLIP-27 Source API
- `AbstractRangeSplitSource<T>`: 范围分片 Source，子类只需实现 `getSplitColumnRange()` 方法
  - 自动根据并行度计算分片数量
  - 自动处理分片大小均衡

**Base 组件：**
- `BaseSplitEnumerator`: 分片枚举器基类，自动处理分片分配和回收
- `BaseSourceReader`: 源阅读器基类，封装线程模型和状态管理
- `BaseSplitReader`: 分片读取器接口，实现阻塞式数据读取

**分片和状态：**
- `RangeSplit`: 范围分片，表示数据范围 [start, end]
- `RangeSplitState`: 分片状态，追踪读取进度
- `RangeEnumCheckpoint`: 枚举器检查点

**JDBC 实现：**
- `JdbcSource`: JDBC Source 实现，继承 `AbstractRangeSplitSource<Row>`
- `JdbcDialect`: 数据库方言接口
- `MySQLDialect`: MySQL 方言实现

## 技术栈

- Java 11
- Apache Flink 1.19.0
- Jackson 2.15.2 (JSON 解析)
- SLF4J + Log4j2 (日志)
- Maven (构建)