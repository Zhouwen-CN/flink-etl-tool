# 参数传递使用说明

## 概述

ETL 工具支持两种配置传递方式：

1. **文件路径方式**：使用 `--file` 参数传递配置文件路径
2. **JSON 字符串方式**：使用 `--localFileSourceConfig` 参数直接传递 JSON 配置

## 使用方法

### 方式一：从文件加载配置

```bash
java -jar flink-etl-client-1.0.0-SNAPSHOT.jar --file localFileSourceConfig/mysql-to-console.json
```

**优点：**
- 配置文件易于维护和版本控制
- 支持复杂的配置结构
- 推荐用于生产环境

### 方式二：从 JSON 字符串加载配置

```bash
java -jar flink-etl-client-1.0.0-SNAPSHOT.jar --localFileSourceConfig '{"job":{"name":"test","mode":"batch"},"source":{...},"sink":{...}}'
```

**优点：**
- 无需创建配置文件
- 适用于临时测试和动态配置生成
- 便于脚本化和自动化

## 错误处理

### 文件不存在

```bash
配置错误: 配置文件不存在: /non/existent/file.json
```

### JSON 解析失败

```bash
配置错误: 配置解析失败: Unexpected character...
```

### 参数值为空

```bash
错误: --file 参数值不能为空
错误: --localFileSourceConfig 参数值不能为空
```

## 向后兼容性

旧版本参数格式仍然支持（已弃用）：

```bash
java -jar flink-etl-tool.jar localFileSourceConfig/mysql-to-console.json
```

建议尽快迁移到新参数格式。

## 使用 Flink ParameterTool

本项目使用 Apache Flink 提供的 `ParameterTool` 工具类处理命令行参数，支持：

- 标准的 `--key value` 格式
- 类型安全的参数访问
- 与 Flink 生态无缝集成

更多参数处理功能参考 [Flink ParameterTool 文档](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/api/java/utils/ParameterTool.html)。