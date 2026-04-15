# Connector 模块重构设计文档

**日期**: 2026-04-15
**状态**: 设计阶段
**影响范围**: 全项目模块结构重构

---

## 背景

当前项目将 Source 和 Sink 插件分别放在 `flink-etl-source` 和 `flink-etl-sink` 两个独立的顶级模块中。这种划分方式导致以下问题：

1. **共用代码跨模块问题**: JDBC dialect 相关代码（JdbcDialect、JdbcDialectLoader、MySQLDialect 等）被放在 `flink-etl-core/dialect` 模块中，但实际上这些类只在 JDBC Source 和 JDBC Sink 中使用。Core 模块承担了不属于核心框架的职责。

2. **模块职责不清晰**: Source 和 Sink 是同一个数据系统的两端，例如 JDBC、Kafka 等连接器的 source 和 sink 实现应该统一管理，而不是分散在不同的顶级模块中。

3. **依赖管理复杂**: 同一个连接器的 source 和 sink 可能依赖相同的第三方库（如 MySQL driver），分散在不同模块导致依赖声明重复。

## 目标

通过模块重构解决上述问题：

1. **职责清晰化**: 将 dialect 等连接器专用代码从 core 模块迁移到对应的 connector 模块，core 模块只保留真正的框架核心代码。

2. **统一管理**: 将同一个连接器的 source 和 sink 实现统一放在同一个 connector 子模块中，便于共用代码管理和依赖统一。

3. **无逻辑变更**: 本次重构纯粹是模块划分调整，不涉及任何代码逻辑变更，不修改原有类的实现。

## 架构设计

### 顶级模块调整

**调整前**:
```
flink-etl-tool/
├── flink-etl-core/
├── flink-etl-client/
├── flink-etl-source/          (顶级模块，将被删除)
├── flink-etl-sink/            (顶级模块，将被删除)
└── flink-etl-transform/
```

**调整后**:
```
flink-etl-tool/
├── flink-etl-core/
├── flink-etl-client/
├── flink-etl-connector/       (新增顶级模块)
└── flink-etl-transform/
```

### flink-etl-connector 详细结构

```
flink-etl-connector/
├── pom.xml                           (父模块 POM)
│
├── connector-jdbc/                   (JDBC 连接器)
│   ├── pom.xml
│   └── src/main/java/com/etl/connector/jdbc/
│       ├── source/                   (原 flink-etl-source-jdbc)
│       │   ├── JdbcSource.java
│       │   ├── JdbcSourcePlugin.java
│       │   ├── JdbcSourceReader.java
│       │   ├── JdbcSourceSplitEnumerator.java
│       │   ├── JdbcSourceSplitReader.java
│       │   ├── RangeSplit.java
│       │   ├── RangeSplitState.java
│       │   ├── RangeEnumCheckpoint.java
│       │   ├── RowRecordEmitter.java
│       │   ├── enums/                (原有的子包保持不变)
│       │   └── config/
│       │       └── JdbcSourceConfig.java
│       ├── sink/                     (原 flink-etl-sink-jdbc)
│       │   ├── JdbcSink.java
│       │   ├── JdbcSinkPlugin.java
│       │   ├── JdbcSinkWriter.java
│       │   ├── NamedParameterSqlParser.java
│       │   └── config/
│       │       └── JdbcSinkConfig.java
│       ├── dialect/                  (从 flink-etl-core/dialect 迁移)
│       │   ├── JdbcDialect.java
│       │   ├── JdbcDialectLoader.java
│       │   ├── MySQLDialect.java
│       │   ├── PostgreSQLDialect.java
│       │   ├── OracleDialect.java
│       │   ├── H2Dialect.java
│       │   └── WriteMode.java
│       ├── converter/                (从 flink-etl-core/jdbc 迁移)
│       │   └── TypeConverter.java
│       └── utils/                    (原 flink-etl-source-jdbc/utils)
│           └── JdbcSplitHelper.java
│   └── src/test/java/com/etl/connector/jdbc/
│       ├── source/
│       ├── sink/
│       ├── dialect/
│       └── utils/
│
├── connector-kafka/                  (Kafka 连接器)
│   ├── pom.xml
│   └── src/main/java/com/etl/connector/kafka/
│       ├── source/                   (原 flink-etl-source-kafka)
│       │   ├── KafkaSourcePlugin.java
│       │   ├── StartupMode.java
│       │   ├── config/
│       │   │   └── KafkaSourceConfig.java
│       │   └── format/
│       │       ├── JsonToRowConverter.java (如果存在)
│       │       └── RowToJsonConverter.java (如果存在)
│       └── sink/                     (原 flink-etl-sink-kafka)
│       │   ├── KafkaSinkPlugin.java
│       │   ├── RowToJsonSerializationSchema.java
│       │   └── config/
│       │       └── KafkaSinkConfig.java
│   └── src/test/java/com/etl/connector/kafka/
│       ├── source/
│       └── sink/
│
├── connector-localfile/              (本地文件连接器，只有 source)
│   ├── pom.xml
│   └── src/main/java/com/etl/connector/localfile/
│       └── source/                   (原 flink-etl-source-localfile)
│           ├── LocalFileSource.java
│           ├── LocalFileSourcePlugin.java
│           ├── config/
│           └── ... (其他原有类和子包)
│   └── src/test/java/com/etl/connector/localfile/
│       └── source/
│
├── connector-console/                (Console 连接器，只有 sink)
│   ├── pom.xml
│   └── src/main/java/com/etl/connector/console/
│       └── sink/                     (原 flink-etl-sink-console)
│           ├── ConsoleSink.java
│           ├── ConsoleSinkPlugin.java
│           └── config/
│   └── src/test/java/com/etl/connector/console/
│       └── sink/
│
├── connector-http/                   (HTTP 连接器，只有 source)
│   ├── pom.xml
│   └── src/main/java/com/etl/connector/http/
│       └── source/                   (原 flink-etl-source-http)
│           ├── HttpSource.java
│           ├── HttpSourcePlugin.java
│           ├── config/
│           └── ... (其他原有类和子包)
│   └── src/test/java/com/etl/connector/http/
│       └── source/
│
└── connector-mock/                   (Mock 连接器，只有 source)
    ├── pom.xml
    └── src/main/java/com/etl/connector/mock/
        └── source/                   (原 flink-etl-source-mock)
            ├── MockSource.java
            ├── MockSourcePlugin.java
            ├── config/
            └── ... (其他原有类和子包)
    └── src/test/java/com/etl/connector/mock/
        └── source/
```

### 包名调整策略

**不调整包名**: 保持原有的包名结构，只调整物理路径。

- 原 `com.etl.source.jdbc.JdbcSource` → 物理路径从 `flink-etl-source-jdbc` 迁移到 `flink-etl-connector/connector-jdbc`，但包名保持不变
- 原 `com.etl.sink.jdbc.JdbcSink` → 物理路径迁移，包名保持不变
- 原 `com.etl.core.dialect.JdbcDialect` → **需要调整包名**为 `com.etl.connector.jdbc.dialect.JdbcDialect`（因为这是 core 到 connector 的跨模块迁移）

**例外**: dialect 和 converter 从 core 模块迁移到 connector 模块时，需要调整包名：

| 原包名 | 新包名 |
|--------|--------|
| `com.etl.core.dialect.*` | `com.etl.connector.jdbc.dialect.*` |
| `com.etl.core.jdbc.TypeConverter` | `com.etl.connector.jdbc.converter.TypeConverter` |

## Maven POM 文件调整

### 顶级 pom.xml (flink-etl-tool/pom.xml)

```xml
<modules>
    <!-- 删除 -->
    <!-- <module>flink-etl-source</module> -->
    <!-- <module>flink-etl-sink</module> -->

    <!-- 新增 -->
    <module>flink-etl-connector</module>

    <!-- 保持不变 -->
    <module>flink-etl-core</module>
    <module>flink-etl-client</module>
    <module>flink-etl-transform</module>
</modules>
```

### flink-etl-connector/pom.xml (新增父模块)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-tool</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flink-etl-connector</artifactId>
    <packaging>pom</packaging>

    <name>Flink ETL Connector</name>
    <description>连接器插件集合（Source + Sink）</description>

    <modules>
        <module>connector-jdbc</module>
        <module>connector-kafka</module>
        <module>connector-localfile</module>
        <module>connector-console</module>
        <module>connector-http</module>
        <module>connector-mock</module>
    </modules>
</project>
```

### connector-jdbc/pom.xml (合并原 source-jdbc 和 sink-jdbc)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-connector</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>connector-jdbc</artifactId>

    <name>Flink ETL Connector - JDBC</name>
    <description>JDBC 连接器（Source + Sink + Dialect）</description>

    <dependencies>
        <!-- MySQL 驶动 -->
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
        </dependency>
        <!-- OceanBase 驿动 -->
        <dependency>
            <groupId>com.oceanbase</groupId>
            <artifactId>oceanbase-client</artifactId>
        </dependency>
        <!-- H2 数据库（测试用） -->
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

### flink-etl-client/pom.xml (调整依赖引用)

```xml
<dependencies>
    <!-- 核心框架 -->
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-core</artifactId>
        <version>${project.version}</version>
    </dependency>

    <!-- 连接器插件 - 替换原有的独立 source/sink 依赖 -->
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>connector-jdbc</artifactId>
        <version>${project.version}</version>
    </dependency>
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>connector-kafka</artifactId>
        <version>${project.version}</version>
    </dependency>
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>connector-localfile</artifactId>
        <version>${project.version}</version>
    </dependency>
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>connector-console</artifactId>
        <version>${project.version}</version>
    </dependency>
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>connector-http</artifactId>
        <version>${project.version}</version>
    </dependency>
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>connector-mock</artifactId>
        <version>${project.version}</version>
    </dependency>

    <!-- Transform 插件保持不变 -->
    <dependency>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-transform</artifactId>
        <version>${project.version}</version>
    </dependency>
</dependencies>
```

## 实施步骤

### 第 1 步：创建新的模块结构

1. 创建 `flink-etl-connector/` 目录
2. 创建 `flink-etl-connector/pom.xml` 父模块文件
3. 创建 6 个 connector 子模块目录和各自的 pom.xml

### 第 2 步：迁移 connector-jdbc

**从 flink-etl-source-jdbc 迁移到 connector-jdbc/source/**:
- 迁移 `src/main/java/com/etl/source/jdbc/` → `connector-jdbc/src/main/java/com/etl/connector/jdbc/source/`
- 迁移 `src/test/java/com/etl/source/jdbc/` → `connector-jdbc/src/test/java/com/etl/connector/jdbc/source/`
- **包名保持不变**：继续使用 `com.etl.source.jdbc.*`（只有物理路径变更）

**从 flink-etl-sink-jdbc 迁移到 connector-jdbc/sink/**:
- 迁移 `src/main/java/com/etl/sink/jdbc/` → `connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/`
- 迁移 `src/test/java/com/etl/sink/jdbc/` → `connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/`
- **包名保持不变**：继续使用 `com.etl.sink.jdbc.*`

**从 flink-etl-core/dialect 迁移到 connector-jdbc/dialect/**:
- 迁移 `flink-etl-core/src/main/java/com/etl/core/dialect/` 下的所有文件
- **需要调整包名**：`com.etl.core.dialect.*` → `com.etl.connector.jdbc.dialect.*`
- 更新所有引用这些类的 import 语句（主要在 JdbcSource 和 JdbcSink 中）

**从 flink-etl-core/jdbc 迁移到 connector-jdbc/converter/**:
- 迁移 `flink-etl-core/src/main/java/com/etl/core/jdbc/TypeConverter.java`
- **需要调整包名**：`com.etl.core.jdbc.TypeConverter` → `com.etl.connector.jdbc.converter.TypeConverter`
- 更新所有引用该类的 import 语句

**原 utils 目录迁移**:
- 迁移 `flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/utils/` → `connector-jdbc/src/main/java/com/etl/connector/jdbc/utils/`
- 包名保持不变

### 第 3 步：迁移其他 connector

按照相同的模式迁移 connector-kafka、connector-localfile、connector-console、connector-http、connector-mock：

- **只有 source 的 connector**：迁移 source 目录和测试，保持包名不变
- **只有 sink 的 connector**：迁移 sink 目录和测试，保持包名不变
- **既有 source 又有 sink 的 connector (Kafka)**：分别迁移到 source/ 和 sink/ 子目录

### 第 4 步：更新 Maven POM 文件

1. 修改顶级 `pom.xml` 的 `<modules>` 部分
2. 创建 `flink-etl-connector/pom.xml` 及各子模块的 pom.xml
3. 更新 `flink-etl-client/pom.xml` 的依赖引用

### 第 5 步：删除旧模块

删除以下目录：
- `flink-etl-source/` (整个目录)
- `flink-etl-sink/` (整个目录)
- `flink-etl-core/src/main/java/com/etl/core/dialect/` (已迁移到 connector-jdbc)
- `flink-etl-core/src/main/java/com/etl/core/jdbc/TypeConverter.java` (已迁移)

### 第 6 步：更新文档

更新以下文档文件：
- `CLAUDE.md` - 架构概览部分
- `PLUGINS.md` - 插件列表部分
- 所有 `docs/superpowers/plans/*.md` 中涉及模块路径的说明

### 第 7 步：编译验证

执行 `mvn clean compile` 确保编译成功，无 import 错误。

### 第 8 步：运行测试

执行 `mvn test` 确保所有测试通过。

## 影响分析

### 不涉及的内容

- **代码逻辑**: 所有类的实现代码保持不变
- **配置文件**: JSON 配置文件的格式和使用方式不变
- **SPI 机制**: `@AutoService` 注解和 META-INF/services 文件位置保持不变（只是物理路径迁移）
- **运行时行为**: Job 执行流程和数据流转机制不变

### 涉及的内容

- **包名调整**: dialect 和 converter 相关类的包名需要调整，其他类保持原包名
- **Import 语句**: JdbcSource、JdbcSink 等类需要更新 import 语句（引用 dialect 和 converter）
- **Maven 依赖**: flink-etl-client 和其他模块的 pom.xml 依赖声明需要调整
- **文档路径**: 文档中提及的模块路径需要更新

## 注意事项

1. **Git 操作**: 使用 `git mv` 命令迁移文件，保留文件历史记录

2. **IDE 配置**: 迁移后可能需要刷新 IDE 的项目结构配置

3. **测试文件**: 测试文件和源文件同步迁移，保持相同的相对路径结构

4. **META-INF/services**: SPI 配置文件位置保持不变，继续放在各自的 resources 目录中

5. **依赖传递**: connector-jdbc 合并了 source-jdbc 和 sink-jdbc 的依赖，需要确保依赖版本一致性

6. **反向依赖检查**: 确认没有其他模块依赖 `flink-etl-source-*` 或 `flink-etl-sink-*` 的 artifactId（除了 flink-etl-client）

## 成功标准

重构成功的判断标准：

1. ✓ `mvn clean compile` 编译成功
2. ✓ `mvn test` 所有测试通过
3. ✓ `mvn clean package` 打包成功，生成的 JAR 包含所有 connector
4. ✓ 运行示例任务成功（如 `docs/examples/mysql-to-console.json`）
5. ✓ 旧的 source/sink 模块目录已删除
6. ✓ dialect 相关代码已从 core 模块移除
7. ✓ 文档已更新

## 附录：包名调整对照表

| 原包名 | 新包名 | 备注 |
|--------|--------|------|
| `com.etl.source.jdbc.*` | `com.etl.source.jdbc.*` | 包名不变，只迁移物理路径 |
| `com.etl.sink.jdbc.*` | `com.etl.sink.jdbc.*` | 包名不变，只迁移物理路径 |
| `com.etl.core.dialect.*` | `com.etl.connector.jdbc.dialect.*` | 包名调整 |
| `com.etl.core.jdbc.TypeConverter` | `com.etl.connector.jdbc.converter.TypeConverter` | 包名调整 |
| `com.etl.source.kafka.*` | `com.etl.source.kafka.*` | 包名不变 |
| `com.etl.sink.kafka.*` | `com.etl.sink.kafka.*` | 包名不变 |
| `com.etl.source.localfile.*` | `com.etl.source.localfile.*` | 包名不变 |
| `com.etl.sink.console.*` | `com.etl.sink.console.*` | 包名不变 |
| `com.etl.source.http.*` | `com.etl.source.http.*` | 包名不变 |
| `com.etl.source.mock.*` | `com.etl.source.mock.*` | 包名不变 |