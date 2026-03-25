# Flink ETL Tool

基于 Apache Flink 的配置驱动 ETL 工具，通过 JSON 配置文件即可定义数据同步任务。

## 特性

- **配置驱动**：无需编码，JSON 配置即可完成数据同步
- **批流一体**：支持批处理和流处理两种模式
- **插件化架构**：基于 SPI 机制，易于扩展
- **并行读取**：支持分片并行，提升吞吐量

## 快速开始

```bash
# 编译
mvn clean package

# 运行任务
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file your-config.json
```

## 支持的插件

**Source**：JDBC、LocalFile、HTTP、Kafka

**Sink**：Console、JDBC

**Transform**：SQL

## 文档

- [CLAUDE.md](CLAUDE.md) - 架构设计、开发指南
- [PLUGINS.md](PLUGINS.md) - 插件配置文档

## 示例

```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "jdbc",
      "outputTable": "users",
      "config": {
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": "root",
        "password": "password",
        "table": "users",
        "splitColumn": "id"
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "users",
      "config": {}
    }
  ]
}
```

更多示例见 `docs/examples/` 目录。