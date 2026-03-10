# Flink ETL 工具实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标:** 构建一个基于 Flink 的配置驱动的 ETL 工具，支持 MySQL、文件数据源和控制台输出，通过 SPI 机制实现插件化扩展。

**架构:** 采用分层架构（CLI → 配置解析 → Job 编排 → 插件层 → Flink 执行），使用 SPI 加载插件，对 Flink Source API 进行二次封装简化开发。

**技术栈:** Apache Flink 1.19+, Maven 3.6+, Jackson 2.15+, SLF4J + Logback, MySQL Connector/J 8.0+

---

## 文件结构总览

### Maven 父项目
- `pom.xml` - Maven 父项目配置，管理依赖版本

### 核心模块 (flink-etl-core)
- `flink-etl-core/pom.xml` - 核心模块配置
- `flink-etl-core/src/main/java/com/etl/core/spi/SourcePlugin.java` - Source 插件接口
- `flink-etl-core/src/main/java/com/etl/core/spi/TransformPlugin.java` - Transform 插件接口
- `flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java` - Sink 插件接口
- `flink-etl-core/src/main/java/com/etl/core/spi/SplitStrategy.java` - 分片策略枚举
- `flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java` - 插件加载器
- `flink-etl-core/src/main/java/com/etl/core/config/SourceConfig.java` - Source 配置对象
- `flink-etl-core/src/main/java/com/etl/core/config/TransformConfig.java` - Transform 配置对象
- `flink-etl-core/src/main/java/com/etl/core/config/SinkConfig.java` - Sink 配置对象
- `flink-etl-core/src/main/java/com/etl/core/config/JobMeta.java` - Job 元信息
- `flink-etl-core/src/main/java/com/etl/core/config/JobConfig.java` - Job 完整配置
- `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java` - 配置解析器
- `flink-etl-core/src/main/java/com/etl/core/source/RangeSplit.java` - 范围分片对象
- `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java` - 分片 Source 抽象基类
- `flink-etl-core/src/main/java/com/etl/core/source/AbstractRangeSplitSource.java` - 范围分片 Source 抽象基类
- `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java` - Job 构建器
- `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java` - Job 执行器
- `flink-etl-core/src/main/java/com/etl/core/EtlApplication.java` - CLI 入口

### Console Sink 插件模块
- `flink-etl-sink/pom.xml` - Sink 父模块配置
- `flink-etl-sink/flink-etl-sink-console/pom.xml` - Console Sink 模块配置
- `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java` - Console Sink 插件实现
- `flink-etl-sink/flink-etl-sink-console/src/main/resources/META-INF/services/com.etl.core.spi.SinkPlugin` - SPI 配置文件

### Transform 插件模块
- `flink-etl-transform/pom.xml` - Transform 模块配置
- `flink-etl-transform/src/main/java/com/etl/transform/FieldMappingTransformPlugin.java` - 字段映射转换插件
- `flink-etl-transform/src/main/resources/META-INF/services/com.etl.core.spi.TransformPlugin` - SPI 配置文件

### 示例配置文件
- `docs/examples/mysql-to-console.json` - MySQL 到控制台配置示例
- `docs/examples/csv-to-console.json` - CSV 到控制台配置示例

---

## Chunk 1: 项目初始化和核心 SPI 接口

### Task 1: 创建 Maven 父项目

**文件:**
- 创建: `pom.xml`

- [ ] **Step 1: 创建 Maven 父项目 pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.etl</groupId>
    <artifactId>flink-etl-tool</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>Flink ETL Tool</name>
    <description>基于 Flink 的配置驱动 ETL 工具</description>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
        <flink.version>1.19.0</flink.version>
        <jackson.version>2.15.2</jackson.version>
        <slf4j.version>1.7.36</slf4j.version>
        <logback.version>1.2.11</logback.version>
        <mysql.connector.version>8.0.33</mysql.connector.version>
        <junit.version>5.9.3</junit.version>
    </properties>

    <modules>
        <module>flink-etl-core</module>
        <module>flink-etl-sink</module>
        <module>flink-etl-transform</module>
    </modules>

    <dependencyManagement>
        <dependencies>
            <!-- Flink 依赖 -->
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-streaming-java</artifactId>
                <version>${flink.version}</version>
                <scope>provided</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-clients</artifactId>
                <version>${flink.version}</version>
                <scope>provided</scope>
            </dependency>

            <!-- Jackson JSON 解析 -->
            <dependency>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
                <version>${jackson.version}</version>
            </dependency>

            <!-- 日志 -->
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>${slf4j.version}</version>
            </dependency>
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
                <version>${logback.version}</version>
            </dependency>

            <!-- MySQL 驱动 -->
            <dependency>
                <groupId>mysql</groupId>
                <artifactId>mysql-connector-java</artifactId>
                <version>${mysql.connector.version}</version>
            </dependency>

            <!-- 测试 -->
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <source>11</source>
                    <target>11</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: 提交初始化项目配置**

```bash
git add pom.xml
git commit -m "feat: 初始化 Maven 父项目配置"
```

---

### Task 2: 创建核心模块和 SPI 接口

**文件:**
- 创建: `flink-etl-core/pom.xml`
- 创建: `flink-etl-core/src/main/java/com/etl/core/spi/SplitStrategy.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/spi/SourcePlugin.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/spi/TransformPlugin.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java`

- [ ] **Step 1: 创建核心模块 pom.xml**

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

    <artifactId>flink-etl-core</artifactId>
    <name>Flink ETL Core</name>
    <description>核心框架模块</description>

    <dependencies>
        <!-- Flink -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
        </dependency>

        <!-- Jackson -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <!-- 日志 -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </dependency>

        <!-- 测试 -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 创建分片策略枚举**

创建文件: `flink-etl-core/src/main/java/com/etl/core/spi/SplitStrategy.java`

```java
package com.etl.core.spi;

/**
 * 分片策略枚举
 * 定义不同数据源支持的分片方式
 */
public enum SplitStrategy {
    /**
     * 不支持分片（如控制台）
     */
    NONE,

    /**
     * 范围分片（MySQL 主键）
     */
    RANGE,

    /**
     * 哈希分片
     */
    HASH,

    /**
     * 基于文件的分片
     */
    FILE_BASED
}
```

- [ ] **Step 3: 创建 SourcePlugin 接口**

创建文件: `flink-etl-core/src/main/java/com/etl/core/spi/SourcePlugin.java`

```java
package com.etl.core.spi;

import com.etl.core.config.SourceConfig;
import org.apache.flink.api.connector.source.Source;

/**
 * Source 插件接口
 * 所有数据源插件必须实现此接口
 */
public interface SourcePlugin {

    /**
     * 获取插件类型标识
     * 例如：mysql、file、kafka
     *
     * @return 插件类型标识
     */
    String getType();

    /**
     * 创建 Flink Source
     *
     * @param config Source 配置
     * @return Flink Source 实例
     */
    Source<?, ?, ?> createSource(SourceConfig config);

    /**
     * 获取分片策略描述
     *
     * @return 该数据源支持的分片方式
     */
    SplitStrategy getSplitStrategy();
}
```

- [ ] **Step 4: 创建 TransformPlugin 接口**

创建文件: `flink-etl-core/src/main/java/com/etl/core/spi/TransformPlugin.java`

```java
package com.etl.core.spi;

import com.etl.core.config.TransformConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Transform 插件接口
 * 所有数据转换插件必须实现此接口
 */
public interface TransformPlugin {

    /**
     * 获取插件类型标识
     *
     * @return 插件类型标识
     */
    String getType();

    /**
     * 创建转换函数
     *
     * @param config Transform 配置
     * @return Flink MapFunction
     */
    MapFunction<?, ?> createTransform(TransformConfig config);
}
```

- [ ] **Step 5: 创建 SinkPlugin 接口**

创建文件: `flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java`

```java
package com.etl.core.spi;

import com.etl.core.config.SinkConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Sink 插件接口
 * 所有数据写入插件必须实现此接口
 */
public interface SinkPlugin {

    /**
     * 获取插件类型标识
     *
     * @return 插件类型标识
     */
    String getType();

    /**
     * 创建 Sink 函数
     *
     * @param config Sink 配置
     * @return Flink SinkFunction
     */
    SinkFunction<?> createSink(SinkConfig config);
}
```

- [ ] **Step 6: 提交核心 SPI 接口**

```bash
git add flink-etl-core/
git commit -m "feat: 添加核心 SPI 接口定义"
```

---

### Task 3: 创建配置对象和解析器

**文件:**
- 创建: `flink-etl-core/src/main/java/com/etl/core/config/SourceConfig.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/config/TransformConfig.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/config/SinkConfig.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/config/JobMeta.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/config/JobConfig.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java`

- [ ] **Step 1: 创建 SourceConfig**

创建文件: `flink-etl-core/src/main/java/com/etl/core/config/SourceConfig.java`

```java
package com.etl.core.config;

import java.util.Map;

/**
 * Source 配置
 */
public class SourceConfig {
    private String type;
    private Map<String, Object> config;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * 获取字符串配置
     */
    public String getString(String key) {
        return config != null ? (String) config.get(key) : null;
    }

    /**
     * 获取整数配置
     */
    public Integer getInteger(String key) {
        return config != null ? (Integer) config.get(key) : null;
    }

    /**
     * 获取对象配置
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }
}
```

- [ ] **Step 2: 创建 TransformConfig**

创建文件: `flink-etl-core/src/main/java/com/etl/core/config/TransformConfig.java`

```java
package com.etl.core.config;

import java.util.Map;

/**
 * Transform 配置
 */
public class TransformConfig {
    private String type;
    private Map<String, Object> config;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * 获取对象配置
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }
}
```

- [ ] **Step 3: 创建 SinkConfig**

创建文件: `flink-etl-core/src/main/java/com/etl/core/config/SinkConfig.java`

```java
package com.etl.core.config;

import java.util.Map;

/**
 * Sink 配置
 */
public class SinkConfig {
    private String type;
    private Map<String, Object> config;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * 获取字符串配置
     */
    public String getString(String key) {
        return config != null ? (String) config.get(key) : null;
    }

    /**
     * 获取整数配置
     */
    public Integer getInteger(String key) {
        return config != null ? (Integer) config.get(key) : null;
    }

    /**
     * 获取对象配置
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }
}
```

- [ ] **Step 4: 创建 JobMeta**

创建文件: `flink-etl-core/src/main/java/com/etl/core/config/JobMeta.java`

```java
package com.etl.core.config;

/**
 * Job 元信息
 */
public class JobMeta {
    private String name;
    private String mode;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
```

- [ ] **Step 5: 创建 JobConfig**

创建文件: `flink-etl-core/src/main/java/com/etl/core/config/JobConfig.java`

```java
package com.etl.core.config;

/**
 * Job 完整配置
 */
public class JobConfig {
    private JobMeta job;
    private SourceConfig source;
    private TransformConfig transform;
    private SinkConfig sink;

    public JobMeta getJob() {
        return job;
    }

    public void setJob(JobMeta job) {
        this.job = job;
    }

    public SourceConfig getSource() {
        return source;
    }

    public void setSource(SourceConfig source) {
        this.source = source;
    }

    public TransformConfig getTransform() {
        return transform;
    }

    public void setTransform(TransformConfig transform) {
        this.transform = transform;
    }

    public SinkConfig getSink() {
        return sink;
    }

    public void setSink(SinkConfig sink) {
        this.sink = sink;
    }
}
```

- [ ] **Step 6: 创建 ConfigParser**

创建文件: `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java`

```java
package com.etl.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 配置文件解析器
 */
public class ConfigParser {
    private static final Logger logger = LoggerFactory.getLogger(ConfigParser.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 从文件解析 Job 配置
     *
     * @param configPath 配置文件路径
     * @return Job 配置对象
     */
    public static JobConfig parse(String configPath) {
        logger.info("解析配置文件: {}", configPath);

        try {
            JobConfig config = mapper.readValue(new File(configPath), JobConfig.class);
            validate(config);
            logger.info("配置文件解析成功");
            return config;
        } catch (Exception e) {
            String errorMsg = String.format("配置文件解析失败: %s", e.getMessage());
            logger.error(errorMsg, e);
            throw new IllegalArgumentException(errorMsg, e);
        }
    }

    /**
     * 校验配置完整性
     *
     * @param config Job 配置
     */
    private static void validate(JobConfig config) {
        if (config.getJob() == null) {
            throw new IllegalArgumentException("缺少 job 配置");
        }
        if (config.getJob().getName() == null || config.getJob().getName().isEmpty()) {
            throw new IllegalArgumentException("缺少 job.name 配置");
        }
        if (config.getJob().getMode() == null || config.getJob().getMode().isEmpty()) {
            throw new IllegalArgumentException("缺少 job.mode 配置");
        }
        if (config.getSource() == null) {
            throw new IllegalArgumentException("缺少 source 配置");
        }
        if (config.getSource().getType() == null || config.getSource().getType().isEmpty()) {
            throw new IllegalArgumentException("缺少 source.type 配置");
        }
        if (config.getSink() == null) {
            throw new IllegalArgumentException("缺少 sink 配置");
        }
        if (config.getSink().getType() == null || config.getSink().getType().isEmpty()) {
            throw new IllegalArgumentException("缺少 sink.type 配置");
        }
    }
}
```

- [ ] **Step 7: 提交配置解析层**

```bash
git add flink-etl-core/src/main/java/com/etl/core/config/
git commit -m "feat: 添加配置对象和解析器"
```

---

### Task 4: 创建插件加载器

**文件:**
- 创建: `flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java`

- [ ] **Step 1: 创建 PluginLoader**

创建文件: `flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java`

```java
package com.etl.core.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

/**
 * SPI 插件加载器
 * 通过 Java SPI 机制动态加载插件
 */
public class PluginLoader {
    private static final Logger logger = LoggerFactory.getLogger(PluginLoader.class);

    /**
     * 加载 Source 插件
     *
     * @param type 插件类型
     * @return Source 插件实例
     */
    public SourcePlugin loadSourcePlugin(String type) {
        logger.info("加载 Source 插件: {}", type);

        ServiceLoader<SourcePlugin> loader = ServiceLoader.load(SourcePlugin.class);
        for (SourcePlugin plugin : loader) {
            if (plugin.getType().equals(type)) {
                logger.info("Source 插件加载成功: {}", plugin.getClass().getName());
                return plugin;
            }
        }

        String errorMsg = String.format("未找到 Source 插件: %s", type);
        logger.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
    }

    /**
     * 加载 Transform 插件
     *
     * @param type 插件类型
     * @return Transform 插件实例
     */
    public TransformPlugin loadTransformPlugin(String type) {
        logger.info("加载 Transform 插件: {}", type);

        ServiceLoader<TransformPlugin> loader = ServiceLoader.load(TransformPlugin.class);
        for (TransformPlugin plugin : loader) {
            if (plugin.getType().equals(type)) {
                logger.info("Transform 插件加载成功: {}", plugin.getClass().getName());
                return plugin;
            }
        }

        String errorMsg = String.format("未找到 Transform 插件: %s", type);
        logger.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
    }

    /**
     * 加载 Sink 插件
     *
     * @param type 插件类型
     * @return Sink 插件实例
     */
    public SinkPlugin loadSinkPlugin(String type) {
        logger.info("加载 Sink 插件: {}", type);

        ServiceLoader<SinkPlugin> loader = ServiceLoader.load(SinkPlugin.class);
        for (SinkPlugin plugin : loader) {
            if (plugin.getType().equals(type)) {
                logger.info("Sink 插件加载成功: {}", plugin.getClass().getName());
                return plugin;
            }
        }

        String errorMsg = String.format("未找到 Sink 插件: %s", type);
        logger.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
    }
}
```

- [ ] **Step 2: 提交插件加载器**

```bash
git add flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java
git commit -m "feat: 添加 SPI 插件加载器"
```

---

## Chunk 2: Flink Source API 封装层

### Task 5: 创建分片相关类

**文件:**
- 创建: `flink-etl-core/src/main/java/com/etl/core/source/RangeSplit.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/source/AbstractRangeSplitSource.java`

- [ ] **Step 1: 创建 RangeSplit**

创建文件: `flink-etl-core/src/main/java/com/etl/core/source/RangeSplit.java`

```java
package com.etl.core.source;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * 范围分片
 * 表示一个数据范围，如 [1, 10000]
 */
public class RangeSplit implements SourceSplit {
    private final String splitId;
    private final String columnName;
    private final long start;
    private final long end;

    public RangeSplit(String columnName, long start, long end) {
        this.columnName = columnName;
        this.start = start;
        this.end = end;
        this.splitId = columnName + "_" + start + "_" + end;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getColumnName() {
        return columnName;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "RangeSplit{" +
                "splitId='" + splitId + '\'' +
                ", columnName='" + columnName + '\'' +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
```

- [ ] **Step 2: 创建 AbstractSplitSource**

创建文件: `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java`

```java
package com.etl.core.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * 支持分片的 Source 抽象基类
 * 简化 Flink FLIP-27 Source API 的实现
 *
 * @param <T> 输出记录类型
 * @param <SplitT> 分片类型
 */
public abstract class AbstractSplitSource<T, SplitT extends SourceSplit>
        implements Source<T, SplitT, PendingSplitsCheckpoint<SplitT>> {

    @Override
    public abstract SplitEnumerator<SplitT, PendingSplitsCheckpoint<SplitT>>
    createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, PendingSplitsCheckpoint<SplitT>>
    restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext,
                      PendingSplitsCheckpoint<SplitT> checkpoint);

    @Override
    public abstract SourceReader<T, SplitT> createReader(SourceReaderContext readerContext);

    @Override
    public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

    @Override
    public abstract SimpleVersionedSerializer<PendingSplitsCheckpoint<SplitT>>
    getEnumeratorCheckpointSerializer();
}
```

- [ ] **Step 3: 创建 AbstractRangeSplitSource**

创建文件: `flink-etl-core/src/main/java/com/etl/core/source/AbstractRangeSplitSource.java`

```java
package com.etl.core.source;

import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 范围分片 Source 抽象基类
 * 适用于 MySQL、PostgreSQL 等关系型数据库
 *
 * @param <T> 输出记录类型
 */
public abstract class AbstractRangeSplitSource<T> extends AbstractSplitSource<T, RangeSplit> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRangeSplitSource.class);

    protected final String splitColumn;
    protected final int splitSize;

    public AbstractRangeSplitSource(String splitColumn, int splitSize) {
        this.splitColumn = splitColumn;
        this.splitSize = splitSize;
    }

    /**
     * 子类实现：获取分片列的最小值和最大值
     * 例如 MySQL: SELECT MIN(id), MAX(id) FROM table
     *
     * @return 分片列的范围
     */
    protected abstract Range<Long> getSplitColumnRange();

    /**
     * 根据最小值、最大值和分片大小，计算所有分片
     * 框架自动实现，子类无需重写
     *
     * @return 分片列表
     */
    protected List<RangeSplit> calculateSplits() {
        Range<Long> range = getSplitColumnRange();
        List<RangeSplit> splits = new ArrayList<>();

        long start = range.getMinimum();
        long end = range.getMaximum();

        logger.info("计算分片: splitColumn={}, range=[{}, {}], splitSize={}",
                splitColumn, start, end, splitSize);

        while (start <= end) {
            long splitEnd = Math.min(start + splitSize - 1, end);
            splits.add(new RangeSplit(splitColumn, start, splitEnd));
            start = splitEnd + 1;
        }

        logger.info("共计算出 {} 个分片", splits.size());
        return splits;
    }
}
```

- [ ] **Step 4: 添加 Apache Commons Lang 依赖到核心模块**

修改文件: `flink-etl-core/pom.xml`，在 `<dependencies>` 中添加：

```xml
<!-- Apache Commons Lang -->
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-lang3</artifactId>
    <version>3.12.0</version>
</dependency>
```

同时在父 `pom.xml` 的 `<dependencyManagement>` 中添加：

```xml
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-lang3</artifactId>
    <version>3.12.0</version>
</dependency>
```

- [ ] **Step 5: 提交 Flink Source 封装层**

```bash
git add flink-etl-core/src/main/java/com/etl/core/source/
git add flink-etl-core/pom.xml
git add pom.xml
git commit -m "feat: 添加 Flink Source API 封装层"
```

---

## Chunk 3: Job 编排层和 CLI 入口

### Task 6: 创建 Job 编排层

**文件:**
- 创建: `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java`
- 创建: `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java`

- [ ] **Step 1: 创建 JobBuilder**

创建文件: `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java`

```java
package com.etl.core.job;

import com.etl.core.config.JobConfig;
import com.etl.core.spi.PluginLoader;
import com.etl.core.spi.SinkPlugin;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.TransformPlugin;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job 构建器
 * 负责将配置转换为 Flink Job
 */
public class JobBuilder {
    private static final Logger logger = LoggerFactory.getLogger(JobBuilder.class);

    private final PluginLoader pluginLoader;

    public JobBuilder(PluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    /**
     * 构建 Flink Job
     *
     * @param env    Flink 执行环境
     * @param config Job 配置
     */
    public void build(StreamExecutionEnvironment env, JobConfig config) {
        logger.info("开始构建 Flink Job: {}", config.getJob().getName());

        // 1. 加载 Source 插件
        SourcePlugin sourcePlugin = pluginLoader.loadSourcePlugin(config.getSource().getType());
        Source<?, ?, ?> source = sourcePlugin.createSource(config.getSource());

        // 2. 创建 DataStream
        DataStream<?> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "source-" + config.getSource().getType()
        );

        logger.info("Source 创建成功");

        // 3. 应用 Transform（如果配置）
        if (config.getTransform() != null) {
            TransformPlugin transformPlugin = pluginLoader.loadTransformPlugin(config.getTransform().getType());
            stream = stream.map(transformPlugin.createTransform(config.getTransform()));
            logger.info("Transform 应用成功");
        }

        // 4. 加载 Sink 插件并写入
        SinkPlugin sinkPlugin = pluginLoader.loadSinkPlugin(config.getSink().getType());
        stream.addSink(sinkPlugin.createSink(config.getSink()));

        logger.info("Sink 创建成功");
        logger.info("Flink Job 构建完成");
    }
}
```

- [ ] **Step 2: 创建 JobExecutor**

创建文件: `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java`

```java
package com.etl.core.job;

import com.etl.core.config.ConfigParser;
import com.etl.core.config.JobConfig;
import com.etl.core.spi.PluginLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job 执行器
 * 负责执行完整的 ETL Job
 */
public class JobExecutor {
    private static final Logger logger = LoggerFactory.getLogger(JobExecutor.class);

    private final PluginLoader pluginLoader;

    public JobExecutor(PluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    /**
     * 执行 Job
     *
     * @param configPath 配置文件路径
     */
    public void execute(String configPath) {
        logger.info("开始执行 Job");

        try {
            // 1. 解析配置
            JobConfig config = ConfigParser.parse(configPath);

            // 2. 创建 Flink 执行环境
            StreamExecutionEnvironment env = createExecutionEnvironment(config);

            // 3. 构建 Job
            JobBuilder jobBuilder = new JobBuilder(pluginLoader);
            jobBuilder.build(env, config);

            // 4. 执行 Job
            logger.info("提交 Job 到 Flink 执行引擎");
            env.execute(config.getJob().getName());

            logger.info("Job 执行成功");
        } catch (Exception e) {
            String errorMsg = String.format("Job 执行失败: %s", e.getMessage());
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    /**
     * 根据配置创建执行环境
     *
     * @param config Job 配置
     * @return Flink 执行环境
     */
    private StreamExecutionEnvironment createExecutionEnvironment(JobConfig config) {
        String mode = config.getJob().getMode();
        logger.info("创建 Flink 执行环境: mode={}", mode);

        if ("batch".equals(mode)) {
            // 批处理模式
            Configuration configuration = new Configuration();
            configuration.setString("execution.runtime-mode", "BATCH");
            return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        } else {
            // 流处理模式
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }
}
```

- [ ] **Step 3: 提交 Job 编排层**

```bash
git add flink-etl-core/src/main/java/com/etl/core/job/
git commit -m "feat: 添加 Job 编排层"
```

---

### Task 7: 创建 CLI 入口

**文件:**
- 创建: `flink-etl-core/src/main/java/com/etl/core/EtlApplication.java`

- [ ] **Step 1: 创建 EtlApplication**

创建文件: `flink-etl-core/src/main/java/com/etl/core/EtlApplication.java`

```java
package com.etl.core;

import com.etl.core.job.JobExecutor;
import com.etl.core.spi.PluginLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL 工具主入口
 */
public class EtlApplication {
    private static final Logger logger = LoggerFactory.getLogger(EtlApplication.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("用法: java -jar etl-tool.jar <config.json>");
            System.err.println("示例: java -jar etl-tool.jar config/mysql-to-console.json");
            System.exit(1);
        }

        String configPath = args[0];
        logger.info("ETL 工具启动，配置文件: {}", configPath);

        try {
            // 初始化插件加载器
            PluginLoader pluginLoader = new PluginLoader();

            // 创建并执行 Job
            JobExecutor executor = new JobExecutor(pluginLoader);
            executor.execute(configPath);

            logger.info("Job 执行成功");
            System.exit(0);
        } catch (Exception e) {
            logger.error("Job 执行失败", e);
            System.err.println("Job 执行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
```

- [ ] **Step 2: 提交 CLI 入口**

```bash
git add flink-etl-core/src/main/java/com/etl/core/EtlApplication.java
git commit -m "feat: 添加 CLI 入口"
```

---

## Chunk 4: Console Sink 插件实现

### Task 8: 创建 Sink 父模块和 Console Sink 插件

**文件:**
- 创建: `flink-etl-sink/pom.xml`
- 创建: `flink-etl-sink/flink-etl-sink-console/pom.xml`
- 创建: `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java`
- 创建: `flink-etl-sink/flink-etl-sink-console/src/main/resources/META-INF/services/com.etl.core.spi.SinkPlugin`

- [ ] **Step 1: 创建 Sink 父模块 pom.xml**

创建文件: `flink-etl-sink/pom.xml`

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

    <artifactId>flink-etl-sink</artifactId>
    <packaging>pom</packaging>
    <name>Flink ETL Sink</name>
    <description>Sink 插件父模块</description>

    <modules>
        <module>flink-etl-sink-console</module>
    </modules>

    <dependencies>
        <!-- 核心模块 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-core</artifactId>
            <version>${project.version}</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 创建 Console Sink 模块 pom.xml**

创建文件: `flink-etl-sink/flink-etl-sink-console/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-sink</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flink-etl-sink-console</artifactId>
    <name>Flink ETL Sink - Console</name>
    <description>控制台输出 Sink 插件</description>
</project>
```

- [ ] **Step 3: 创建 ConsoleSinkPlugin**

创建文件: `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java`

```java
package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Console Sink 插件
 * 将数据输出到控制台
 */
public class ConsoleSinkPlugin implements SinkPlugin {
    private static final Logger logger = LoggerFactory.getLogger(ConsoleSinkPlugin.class);

    @Override
    public String getType() {
        return "console";
    }

    @Override
    public SinkFunction<?> createSink(SinkConfig config) {
        String format = config.getString("format");
        logger.info("创建 Console Sink, format={}", format);

        return new ConsoleSinkFunction(format);
    }

    /**
     * Console Sink Function
     */
    private static class ConsoleSinkFunction implements SinkFunction<Object> {
        private final String format;

        public ConsoleSinkFunction(String format) {
            this.format = format != null ? format : "json";
        }

        @Override
        public void invoke(Object value, Context context) {
            // 简单实现：直接打印对象
            System.out.println(value.toString());
        }
    }
}
```

- [ ] **Step 4: 创建 SPI 配置文件**

创建文件: `flink-etl-sink/flink-etl-sink-console/src/main/resources/META-INF/services/com.etl.core.spi.SinkPlugin`

内容：
```
com.etl.sink.console.ConsoleSinkPlugin
```

- [ ] **Step 5: 提交 Console Sink 插件**

```bash
git add flink-etl-sink/
git commit -m "feat: 添加 Console Sink 插件"
```

---

## Chunk 5: Transform 插件实现

### Task 9: 创建 Transform 插件模块

**文件:**
- 创建: `flink-etl-transform/pom.xml`
- 创建: `flink-etl-transform/src/main/java/com/etl/transform/FieldMappingTransformPlugin.java`
- 创建: `flink-etl-transform/src/main/resources/META-INF/services/com.etl.core.spi.TransformPlugin`

- [ ] **Step 1: 创建 Transform 模块 pom.xml**

创建文件: `flink-etl-transform/pom.xml`

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

    <artifactId>flink-etl-transform</artifactId>
    <name>Flink ETL Transform</name>
    <description>Transform 插件模块</description>

    <dependencies>
        <!-- 核心模块 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-core</artifactId>
            <version>${project.version}</version>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 创建 FieldMappingTransformPlugin**

创建文件: `flink-etl-transform/src/main/java/com/etl/transform/FieldMappingTransformPlugin.java`

```java
package com.etl.transform;

import com.etl.core.config.TransformConfig;
import com.etl.core.spi.TransformPlugin;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 字段映射转换插件
 * 支持字段重命名和字段过滤
 */
public class FieldMappingTransformPlugin implements TransformPlugin {
    private static final Logger logger = LoggerFactory.getLogger(FieldMappingTransformPlugin.class);

    @Override
    public String getType() {
        return "field-mapping";
    }

    @Override
    public MapFunction<?, ?> createTransform(TransformConfig config) {
        logger.info("创建字段映射转换插件");

        // 简化实现：这里返回一个简单的 MapFunction
        // 实际实现需要根据配置进行字段映射和过滤
        return new FieldMappingFunction();
    }

    /**
     * 字段映射函数
     * TODO: 实现完整的字段映射和过滤逻辑
     */
    private static class FieldMappingFunction implements MapFunction<Object, Object> {
        @Override
        public Object map(Object value) throws Exception {
            // 简化实现：直接返回原值
            // 后续需要实现：字段重命名、字段过滤
            return value;
        }
    }
}
```

- [ ] **Step 3: 创建 SPI 配置文件**

创建文件: `flink-etl-transform/src/main/resources/META-INF/services/com.etl.core.spi.TransformPlugin`

内容：
```
com.etl.transform.FieldMappingTransformPlugin
```

- [ ] **Step 4: 提交 Transform 插件**

```bash
git add flink-etl-transform/
git commit -m "feat: 添加字段映射转换插件"
```

---

## Chunk 6: 示例配置和文档

### Task 10: 创建示例配置文件

**文件:**
- 创建: `docs/examples/mysql-to-console.json`
- 创建: `docs/examples/csv-to-console.json`

- [ ] **Step 1: 创建 MySQL 到控制台配置示例**

创建文件: `docs/examples/mysql-to-console.json`

```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch"
  },
  "source": {
    "type": "mysql",
    "config": {
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
    "config": {
      "mappings": [
        { "from": "id", "to": "user_id" },
        { "from": "name", "to": "user_name" }
      ],
      "filters": ["age", "email"]
    }
  },
  "sink": {
    "type": "console",
    "config": {
      "format": "json"
    }
  }
}
```

- [ ] **Step 2: 创建 CSV 到控制台配置示例**

创建文件: `docs/examples/csv-to-console.json`

```json
{
  "job": {
    "name": "csv-to-console",
    "mode": "batch"
  },
  "source": {
    "type": "file",
    "config": {
      "path": "/data/input/users.csv",
      "format": "csv",
      "fields": ["id", "name", "age", "email"]
    }
  },
  "sink": {
    "type": "console",
    "config": {
      "format": "json"
    }
  }
}
```

- [ ] **Step 3: 提交示例配置**

```bash
git add docs/examples/
git commit -m "docs: 添加配置文件示例"
```

---

## 构建和测试指南

### 构建项目

```bash
# 编译整个项目
mvn clean install

# 只编译核心模块
mvn clean install -pl flink-etl-core
```

### 运行测试

```bash
# 运行所有测试
mvn test

# 运行指定模块的测试
mvn test -pl flink-etl-core
```

### 打包运行

```bash
# 打包
mvn clean package

# 运行（需要先实现 Source 插件）
java -cp flink-etl-core/target/flink-etl-core-1.0.0-SNAPSHOT.jar:flink-etl-sink/flink-etl-sink-console/target/flink-etl-sink-console-1.0.0-SNAPSHOT.jar:flink-etl-transform/target/flink-etl-transform-1.0.0-SNAPSHOT.jar com.etl.core.EtlApplication docs/examples/mysql-to-console.json
```

---

## 后续任务（未在此次实现计划中）

以下任务将在后续迭代中实现：

1. **MySQL Source 插件实现**
   - 实现 MySQLSourcePlugin
   - 实现 MySQLSource（继承 AbstractRangeSplitSource）
   - 实现 MySQLSplitEnumerator
   - 实现 MySQLSourceReader

2. **File Source 插件实现**
   - 实现 FileSourcePlugin
   - 实现文件读取逻辑（CSV、JSON）

3. **MySQL Sink 插件实现**
   - 实现 MySQLSinkPlugin
   - 实现批量写入 MySQL

4. **单元测试**
   - 为所有核心类编写单元测试
   - 集成测试

5. **性能优化**
   - 连接池管理
   - 批量写入优化
   - 内存管理

---

## 注意事项

1. **TDD 原则**: 实际开发时应该先写测试，再写实现。本计划为了简化展示，省略了测试编写步骤。

2. **渐进式开发**: 按照 Chunk 顺序逐步实现，每个 Chunk 完成后进行集成测试。

3. **错误处理**: 所有关键操作都应该有完善的错误处理和日志记录。

4. **配置验证**: ConfigParser 的 validate 方法可以扩展，添加更多配置项的验证。

5. **SPI 机制**: 确保每个插件模块都有正确的 `META-INF/services` 配置文件。

---

**计划完成！准备开始执行。**