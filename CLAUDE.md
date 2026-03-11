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
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar docs/examples/mysql-to-console.json

# 安装到本地仓库（开发新插件时需要）
mvn clean install -DskipTests
```

## 架构概览

### 模块结构

```
flink-etl-tool/
├── flink-etl-core/           # 核心框架（SPI 接口、配置解析、Job 编排）
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

### 扩展新数据源

添加新数据源需要：

1. 创建新模块，依赖 `flink-etl-core`
2. 实现 `SourcePlugin` 接口
3. 继承 `AbstractRangeSplitSource` 实现分片读取（关系型数据库）
4. 添加 `META-INF/services/com.etl.core.spi.SourcePlugin` 文件
5. 在 `flink-etl-client/pom.xml` 添加新模块依赖

## 配置文件格式

配置采用 DataX 风格的 JSON 结构：

```json
{
  "job": { "name": "job-name", "mode": "batch" },
  "source": { "type": "mysql", "config": { ... } },
  "transform": { "type": "field-mapping", "config": { ... } },
  "sink": { "type": "console", "config": { ... } }
}
```

示例配置位于 `docs/examples/` 目录。

## 关键抽象类

- **AbstractSplitSource**: 支持分片的 Source 抽象基类，封装 Flink FLIP-27 Source API
- **AbstractRangeSplitSource**: 范围分片 Source，子类只需实现 `getSplitColumnRange()` 方法

## 技术栈

- Java 11
- Apache Flink 1.19.0
- Jackson 2.15.2 (JSON 解析)
- SLF4J + Logback (日志)
- Maven (构建)