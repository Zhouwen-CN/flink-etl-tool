# Flink ETL 工具设计文档

**日期**: 2026-03-10
**作者**: Claude & User
**状态**: 已批准

---

## 1. 项目概述

### 1.1 项目定位

开发一个基于 Apache Flink 的 ETL 工具，类似于 DataX 和 SeaTunnel，面向数据工程师提供独立的数据集成产品。

### 1.2 核心目标

- 通过 JSON 配置文件驱动数据同步任务
- 支持批处理和流处理两种模式（初始版本聚焦批处理）
- 使用 Flink FLIP-27 新 Source 架构，支持数据源分片
- 通过 SPI 机制实现插件化扩展
- 提供简洁的抽象层，方便开发者扩展新数据源

### 1.3 核心特性

| 特性 | 策略 |
|------|------|
| 执行模式 | 批处理优先，流处理后期扩展 |
| 数据源支持 | MySQL（分片）、文件系统、控制台输出（验证架构） |
| 数据转换 | 基础转换（字段映射、过滤），配置驱动 |
| 分片策略 | 固定策略，每种数据源一种（MySQL: 主键范围分片） |
| 错误处理 | 快速失败，不重试 |
| 监控日志 | 基础日志输出 |

---

## 2. 架构设计

### 2.1 整体架构

采用经典分层架构，职责清晰，易于扩展：

```
┌─────────────────────────────────────────────────────┐
│                   CLI 入口层                          │
│              (解析命令行参数，启动 Job)                │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                 配置解析层                           │
│      (读取 JSON，校验配置，转换为 JobConfig)         │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│                Job 编排层                            │
│  (构建 Flink Job，配置 Source/Transform/Sink)       │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          │            │            │
┌─────────▼───┐  ┌────▼────┐  ┌───▼──────┐
│ Source 插件 │  │Transform│  │Sink 插件  │
│   (SPI)     │  │ 插件(SPI)│  │  (SPI)   │
└──────┬──────┘  └────┬────┘  └─────┬─────┘
       │              │             │
       └──────────────┼─────────────┘
                      │
         ┌────────────▼────────────┐
         │   Flink 执行引擎         │
         │  (Local/Remote 模式)    │
         └─────────────────────────┘
```

### 2.2 核心分层职责

**CLI 入口层**
- 解析命令行参数（配置文件路径、运行模式等）
- 初始化日志系统
- 调用配置解析层，启动 Job 执行

**配置解析层**
- 读取并解析 JSON 配置文件
- 校验配置完整性（必填字段、类型检查）
- 将 JSON 转换为内部的 JobConfig 对象
- 错误信息友好提示

**Job 编排层**
- 根据 JobConfig 构建 Flink DataStream Job
- 通过 SPI 加载对应的插件实例
- 组装 Source → Transform → Sink 的处理链
- 提交 Job 到 Flink 执行环境

**插件层（Source/Transform/Sink）**
- Source 插件：实现 Flink Source 接口，负责数据读取和分片
- Transform 插件：实现数据转换逻辑（字段映射、过滤等）
- Sink 插件：实现 Flink Sink 接口，负责数据写入
- 所有插件通过 SPI 动态加载

**Flink 执行层**
- 提供 Local 和 Remote 两种执行模式
- 管理资源分配和任务调度
- 提供 Checkpoint 和容错机制（为后续流处理准备）

---

## 3. 核心接口设计

### 3.1 SPI 插件接口

#### SourcePlugin 接口

```java
public interface SourcePlugin {
    /**
     * 获取插件类型标识
     * 例如：mysql、file、kafka
     */
    String getType();

    /**
     * 创建 Flink Source
     */
    Source<?, ?, ?> createSource(SourceConfig localFileSourceConfig);

    /**
     * 获取分片策略描述
     */
    SplitStrategy getSplitStrategy();
}
```

#### TransformPlugin 接口

```java
public interface TransformPlugin {
    /**
     * 获取插件类型标识
     */
    String getType();

    /**
     * 创建转换函数
     */
    MapFunction<?, ?> createTransform(TransformConfig localFileSourceConfig);
}
```

#### SinkPlugin 接口

```java
public interface SinkPlugin {
    /**
     * 获取插件类型标识
     */
    String getType();

    /**
     * 创建 Sink 函数
     */
    SinkFunction<?> createSink(SinkConfig localFileSourceConfig);
}
```

### 3.2 Flink Source API 封装层

为了简化 Flink FLIP-27 Source 的实现，提供抽象基类：

#### AbstractSplitSource

支持分片的 Source 抽象基类，简化 Flink FLIP-27 Source API 的实现。

#### AbstractRangeSplitSource

范围分片 Source 抽象基类，适用于 MySQL、PostgreSQL 等关系型数据库。

**关键设计**：
- 子类只需实现 `getSplitColumnRange()` 获取分片列的最小值和最大值
- 框架自动计算所有分片（通过 `calculateSplits()` 方法）
- 大幅降低实现难度，符合"简单扩展"目标

#### RangeSplit

范围分片对象，表示一个数据范围，如 [1, 10000]。

---

## 4. 配置文件设计

### 4.1 配置格式

采用 DataX 风格的配置结构，简洁明了：

```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch"
  },
  "source": {
    "type": "mysql",
    "localFileSourceConfig": {
      "url": "jdbc:mysql://localhost:3306/test_db",
      "table": "user_table",
      "username": "root",
      "password": "123456",
      "splitColumn": "id",
      "splitSize": 10000
    }
  },
  "transform": {
    "type": "field-mapping",
    "localFileSourceConfig": {
      "mappings": [
        { "from": "id", "to": "user_id" },
        { "from": "name", "to": "user_name" }
      ],
      "filters": ["age", "email"]
    }
  },
  "sink": {
    "type": "console",
    "localFileSourceConfig": {
      "format": "json"
    }
  }
}
```

### 4.2 配置字段说明

**MySQL Source 配置字段**：
- `url`: JDBC 连接 URL（包含 host、port、database）
- `table`: 表名
- `username`: 用户名
- `password`: 密码
- `splitColumn`: 分片列（主键）
- `splitSize`: 每个分片的行数

**MySQL Sink 配置字段**：
- `url`: JDBC 连接 URL
- `table`: 表名
- `username`: 用户名
- `password`: 密码
- `batchSize`: 批量写入大小

---

## 5. 项目模块结构

```
flink-etl-tool/
├── pom.xml                                 # Maven 父项目配置
│
├── flink-etl-core/                         # 核心模块
│   ├── pom.xml
│   └── src/main/java/com/etl/core/
│       ├── EtlApplication.java             # CLI 入口
│       ├── localFileSourceConfig/                         # 配置解析层
│       │   ├── JobConfig.java
│       │   ├── SourceConfig.java
│       │   ├── TransformConfig.java
│       │   ├── SinkConfig.java
│       │   └── ConfigParser.java
│       ├── job/                            # Job 编排层
│       │   ├── JobBuilder.java
│       │   └── JobExecutor.java
│       └── spi/                            # SPI 核心接口
│           ├── SourcePlugin.java
│           ├── TransformPlugin.java
│           ├── SinkPlugin.java
│           └── PluginLoader.java
│
├── flink-etl-source/                       # Source 插件模块（父模块）
│   ├── pom.xml
│   ├── flink-etl-source-mysql/             # MySQL Source 实现
│   └── flink-etl-source-file/              # File Source 实现
│
├── flink-etl-transform/                    # Transform 插件模块
│   ├── pom.xml
│   └── src/main/java/com/etl/transform/
│       └── FieldMappingTransform.java
│
├── flink-etl-sink/                         # Sink 插件模块（父模块）
│   ├── pom.xml
│   ├── flink-etl-sink-console/             # Console Sink 实现
│   └── flink-etl-sink-mysql/               # MySQL Sink 实现
│
└── docs/                                   # 文档目录
    └── examples/                           # 配置文件示例
        ├── mysql-to-console.json
        └── csv-to-mysql.json
```

### 5.1 模块职责

**flink-etl-core**: 核心框架
- 定义 SPI 接口
- 实现配置解析、Job 编排、插件加载
- 提供抽象基类简化插件开发

**flink-etl-source-***: Source 插件实现
- MySQL Source: 支持主键范围分片
- File Source: 支持读取 CSV、JSON 文件

**flink-etl-transform**: Transform 插件实现
- FieldMappingTransform: 字段映射、过滤

**flink-etl-sink-***: Sink 插件实现
- Console Sink: 控制台输出
- MySQL Sink: 批量写入 MySQL

---

## 6. 数据流执行流程

```
1. CLI 启动
   ↓
2. ConfigParser 解析 JSON → JobConfig
   ↓
3. JobExecutor 创建 Flink Environment
   ↓
4. JobBuilder 构建处理链：
   a) PluginLoader 加载 SourcePlugin
   b) SourcePlugin.createSource() → Flink Source
   c) env.fromSource() → DataStream
   d) PluginLoader 加载 TransformPlugin（如果有）
   e) DataStream.map() 应用转换
   f) PluginLoader 加载 SinkPlugin
   g) DataStream.addSink() 写出数据
   ↓
5. env.execute() 提交 Flink Job
   ↓
6. Flink 执行引擎运行 Job
   - SplitEnumerator 分配分片给 Reader
   - SourceReader 读取数据
   - Transform 处理数据
   - Sink 写出数据
```

---

## 7. 扩展机制

### 7.1 如何扩展新数据源

开发者只需 3 步即可扩展新数据源：

**步骤 1**: 创建新模块，添加 SPI 配置文件
```
META-INF/services/com.etl.core.spi.SourcePlugin
```

**步骤 2**: 实现 SourcePlugin 接口
```java
public class PostgreSQLSourcePlugin implements SourcePlugin {
    @Override
    public String getType() {
        return "postgresql";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig localFileSourceConfig) {
        return new PostgreSQLSource(localFileSourceConfig);
    }

    @Override
    public SplitStrategy getSplitStrategy() {
        return SplitStrategy.RANGE;
    }
}
```

**步骤 3**: 继承 AbstractRangeSplitSource 实现数据读取
```java
public class PostgreSQLSource extends AbstractRangeSplitSource<Row> {
    // 只需实现 3-4 个方法即可
}
```

### 7.2 SPI 加载机制

使用 Java SPI 机制，通过 `ServiceLoader` 动态加载插件：

```java
public class PluginLoader {
    public SourcePlugin loadSourcePlugin(String type) {
        ServiceLoader<SourcePlugin> loader = ServiceLoader.load(SourcePlugin.class);
        for (SourcePlugin plugin : loader) {
            if (plugin.getType().equals(type)) {
                return plugin;
            }
        }
        throw new IllegalArgumentException("未找到 Source 插件: " + type);
    }
}
```

---

## 8. 错误处理策略

### 8.1 配置错误

- 配置文件不存在：提示文件路径
- JSON 格式错误：提示具体错误位置
- 必填字段缺失：提示缺失的字段名
- 字段类型错误：提示期望类型和实际类型

### 8.2 运行时错误

- 插件加载失败：提示插件类型未找到
- 数据源连接失败：提示连接信息和错误原因
- 数据读取/写入错误：打印完整堆栈信息，快速失败
- Job 执行失败：提示错误信息并退出

---

## 9. 技术选型

| 组件 | 技术选择 | 版本 | 理由 |
|------|---------|------|------|
| 核心框架 | Apache Flink | 1.19+ | FLIP-27 Source 架构 |
| 构建工具 | Maven | 3.6+ | 成熟的 Java 构建工具 |
| JSON 解析 | Jackson | 2.15+ | 性能优秀，生态完善 |
| 日志框架 | SLF4J + Logback | 1.2+ | Java 标准日志方案 |
| MySQL 驱动 | MySQL Connector/J | 8.0+ | 官方驱动 |

---

## 10. 后续演进方向

### 10.1 短期优化

- 添加更多数据源支持（PostgreSQL、Oracle、Kafka）
- 支持更多 Transform 类型（SQL 表达式、UDF）
- 优化错误提示信息，提供解决方案建议

### 10.2 中期扩展

- 支持流处理模式
- 提供 Web UI 监控界面
- 集成 Prometheus 监控指标

### 10.3 长期规划

- 支持分布式部署模式
- 提供任务调度和编排能力
- 构建完整的数据集成平台

---

## 11. 总结

本设计文档定义了一个基于 Flink 的 ETL 工具的完整架构，核心特点：

1. **简洁易用**：配置驱动，无需编程即可使用
2. **易于扩展**：SPI 插件机制，新增数据源只需继承抽象类
3. **架构清晰**：分层设计，职责明确
4. **性能可靠**：基于 Flink，支持分片并行处理

该设计为后续实现提供了清晰的蓝图，确保项目开发方向正确。