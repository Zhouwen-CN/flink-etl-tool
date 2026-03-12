# 程序参数传递优化实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 支持两种配置传递方式（文件路径和 JSON 字符串），使用 Flink ParameterTool 处理命令行参数，提供清晰的错误信息。

**Architecture:** 创建 ConfigLoader 类封装配置加载逻辑（文件或 JSON 字符串），修改 EtlClient 使用 ParameterTool 解析参数，重构 JobExecutor 接受 JobConfig 对象而非文件路径。保持向后兼容性，支持旧的参数格式。

**Tech Stack:** Java 11, Apache Flink 1.19.0 ParameterTool, Jackson 2.15.2, JUnit 5

---

## 文件结构

**新增文件：**
- `flink-etl-core/src/main/java/com/etl/core/config/ConfigLoader.java` - 配置加载器，支持文件和 JSON 字符串两种方式
- `flink-etl-core/src/test/java/com/etl/core/config/ConfigLoaderTest.java` - ConfigLoader 单元测试
- `flink-etl-core/src/test/java/com/etl/core/config/ConfigParserTest.java` - ConfigParser 单元测试

**修改文件：**
- `flink-etl-client/src/main/java/com/etl/client/EtlClient.java` - 使用 ParameterTool 解析参数
- `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java` - 接受 JobConfig 对象
- `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java` - 添加 parseFromString 方法

---

## Chunk 1: 核心配置加载功能

### Task 1: 为 ConfigParser 添加 parseFromString 方法

**Files:**
- Create: `flink-etl-core/src/test/java/com/etl/core/config/ConfigParserTest.java`
- Modify: `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java`

- [ ] **Step 1: 编写测试 - testParseFromString**

创建测试文件 `flink-etl-core/src/test/java/com/etl/core/config/ConfigParserTest.java`：

```java
package com.etl.core.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConfigParser 测试
 */
class ConfigParserTest {

    @Test
    void testParseFromString() {
        String json = "{\n" +
                "  \"job\": {\n" +
                "    \"name\": \"test-job\",\n" +
                "    \"mode\": \"batch\"\n" +
                "  },\n" +
                "  \"source\": {\n" +
                "    \"type\": \"mysql\",\n" +
                "    \"config\": {}\n" +
                "  },\n" +
                "  \"sink\": {\n" +
                "    \"type\": \"console\",\n" +
                "    \"config\": {}\n" +
                "  }\n" +
                "}";

        JobConfig config = ConfigParser.parseFromString(json);

        assertNotNull(config);
        assertEquals("test-job", config.getJob().getName());
        assertEquals("batch", config.getJob().getMode());
        assertEquals("mysql", config.getSource().getType());
        assertEquals("console", config.getSink().getType());
    }

    @Test
    void testParseFromStringInvalidJson() {
        String invalidJson = "{ invalid json }";

        assertThrows(IllegalArgumentException.class, () -> {
            ConfigParser.parseFromString(invalidJson);
        });
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -Dtest=ConfigParserTest -pl flink-etl-core`
Expected: FAIL - 方法 `parseFromString` 不存在

- [ ] **Step 3: 实现 parseFromString 方法**

修改 `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java`，在第 35 行后添加：

```java
/**
 * 从 JSON 字符串解析 Job 配置
 *
 * @param json JSON 字符串
 * @return Job 配置对象
 */
public static JobConfig parseFromString(String json) {
    logger.info("从字符串解析配置");

    try {
        JobConfig config = mapper.readValue(json, JobConfig.class);
        validate(config);
        logger.info("配置解析成功");
        return config;
    } catch (Exception e) {
        String errorMsg = String.format("配置解析失败: %s", e.getMessage());
        logger.error(errorMsg, e);
        throw new IllegalArgumentException(errorMsg, e);
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -Dtest=ConfigParserTest -pl flink-etl-core`
Expected: PASS - 两个测试都通过

- [ ] **Step 5: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java flink-etl-core/src/test/java/com/etl/core/config/ConfigParserTest.java
git commit -m "feat: 为 ConfigParser 添加 parseFromString 方法"
```

---

### Task 2: 创建 ConfigLoader 类

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/config/ConfigLoader.java`
- Create: `flink-etl-core/src/test/java/com/etl/core/config/ConfigLoaderTest.java`

- [ ] **Step 1: 编写 ConfigLoader 测试**

创建 `flink-etl-core/src/test/java/com/etl/core/config/ConfigLoaderTest.java`：

```java
package com.etl.core.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConfigLoader 测试
 */
class ConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void testLoadFromFile() throws Exception {
        File configFile = tempDir.resolve("test-config.json").toFile();
        String jsonContent = "{\n" +
                "  \"job\": {\n" +
                "    \"name\": \"test-job\",\n" +
                "    \"mode\": \"batch\"\n" +
                "  },\n" +
                "  \"source\": {\n" +
                "    \"type\": \"mysql\",\n" +
                "    \"config\": {}\n" +
                "  },\n" +
                "  \"sink\": {\n" +
                "    \"type\": \"console\",\n" +
                "    \"config\": {}\n" +
                "  }\n" +
                "}";
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write(jsonContent);
        }

        JobConfig config = ConfigLoader.loadFromFile(configFile.getAbsolutePath());

        assertNotNull(config);
        assertEquals("test-job", config.getJob().getName());
        assertEquals("batch", config.getJob().getMode());
    }

    @Test
    void testLoadFromFileNotFound() {
        String nonExistentPath = "/non/existent/path/config.json";

        assertThrows(IllegalArgumentException.class, () -> {
            ConfigLoader.loadFromFile(nonExistentPath);
        });
    }

    @Test
    void testLoadFromJsonString() {
        String json = "{\n" +
                "  \"job\": {\n" +
                "    \"name\": \"test-job\",\n" +
                "    \"mode\": \"batch\"\n" +
                "  },\n" +
                "  \"source\": {\n" +
                "    \"type\": \"mysql\",\n" +
                "    \"config\": {}\n" +
                "  },\n" +
                "  \"sink\": {\n" +
                "    \"type\": \"console\",\n" +
                "    \"config\": {}\n" +
                "  }\n" +
                "}";

        JobConfig config = ConfigLoader.loadFromJsonString(json);

        assertNotNull(config);
        assertEquals("test-job", config.getJob().getName());
    }

    @Test
    void testLoadFromJsonStringInvalid() {
        String invalidJson = "{ invalid json }";

        assertThrows(IllegalArgumentException.class, () -> {
            ConfigLoader.loadFromJsonString(invalidJson);
        });
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -Dtest=ConfigLoaderTest -pl flink-etl-core`
Expected: FAIL - 类 `ConfigLoader` 不存在

- [ ] **Step 3: 实现 ConfigLoader 类**

创建 `flink-etl-core/src/main/java/com/etl/core/config/ConfigLoader.java`：

```java
package com.etl.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 配置加载器
 * 支持从文件或 JSON 字符串加载配置
 */
public class ConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    /**
     * 从文件加载配置
     *
     * @param filePath 配置文件路径
     * @return Job 配置对象
     * @throws IllegalArgumentException 文件不存在或解析失败
     */
    public static JobConfig loadFromFile(String filePath) {
        logger.info("从文件加载配置: {}", filePath);

        if (!Files.exists(Paths.get(filePath))) {
            String errorMsg = String.format("配置文件不存在: %s", filePath);
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        File file = new File(filePath);
        if (!file.isFile()) {
            String errorMsg = String.format("路径不是文件: %s", filePath);
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        return ConfigParser.parse(filePath);
    }

    /**
     * 从 JSON 字符串加载配置
     *
     * @param json JSON 字符串
     * @return Job 配置对象
     * @throws IllegalArgumentException 解析失败
     */
    public static JobConfig loadFromJsonString(String json) {
        logger.info("从 JSON 字符串加载配置");
        return ConfigParser.parseFromString(json);
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -Dtest=ConfigLoaderTest -pl flink-etl-core`
Expected: PASS - 四个测试都通过

- [ ] **Step 5: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/config/ConfigLoader.java flink-etl-core/src/test/java/com/etl/core/config/ConfigLoaderTest.java
git commit -m "feat: 创建 ConfigLoader 支持文件和 JSON 字符串加载"
```

---

## Chunk 2: 重构 JobExecutor 和 EtlClient

### Task 3: 重构 JobExecutor 支持 JobConfig

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java`

- [ ] **Step 1: 添加 execute(JobConfig) 方法**

修改 `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java`，在第 57 行后添加：

```java
/**
 * 执行 Job（使用 JobConfig 对象）
 *
 * @param config Job 配置对象
 */
public void execute(JobConfig config) {
    logger.info("开始执行 Job: {}", config.getJob().getName());

    try {
        StreamExecutionEnvironment env = createExecutionEnvironment(config);

        JobBuilder jobBuilder = new JobBuilder(pluginLoader);
        jobBuilder.build(env, config);

        logger.info("提交 Job 到 Flink 执行引擎");
        env.execute(config.getJob().getName());

        logger.info("Job 执行成功");
    } catch (Exception e) {
        String errorMsg = String.format("Job 执行失败: %s", e.getMessage());
        logger.error(errorMsg, e);
        throw new RuntimeException(errorMsg, e);
    }
}
```

- [ ] **Step 2: 标记旧方法为 @Deprecated**

修改 `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java`，更新 execute(String) 方法：

```java
/**
 * 执行 Job
 *
 * @param configPath 配置文件路径
 * @deprecated 请使用 {@link #execute(JobConfig)} 方法
 */
@Deprecated
public void execute(String configPath) {
    logger.info("开始执行 Job（从文件: {}）", configPath);

    try {
        JobConfig config = ConfigParser.parse(configPath);
        execute(config);
    } catch (Exception e) {
        String errorMsg = String.format("Job 执行失败: %s", e.getMessage());
        logger.error(errorMsg, e);
        throw new RuntimeException(errorMsg, e);
    }
}
```

- [ ] **Step 3: 编译验证**

Run: `mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java
git commit -m "refactor: JobExecutor 支持 JobConfig 对象参数"
```

---

### Task 4: 修改 EtlClient 使用 ParameterTool

**Files:**
- Modify: `flink-etl-client/src/main/java/com/etl/client/EtlClient.java`

- [ ] **Step 1: 重构 main 方法**

修改 `flink-etl-client/src/main/java/com/etl/client/EtlClient.java`：

```java
package com.etl.client;

import com.etl.core.config.ConfigLoader;
import com.etl.core.config.JobConfig;
import com.etl.core.job.JobExecutor;
import com.etl.core.spi.PluginLoader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL 客户端启动器
 */
public class EtlClient {
    private static final Logger logger = LoggerFactory.getLogger(EtlClient.class);

    public static void main(String[] args) {
        logger.info("ETL 工具启动");

        try {
            ParameterTool params = ParameterTool.fromArgs(args);

            JobConfig config = null;

            if (params.has("file")) {
                String filePath = params.get("file");
                logger.info("从文件加载配置: {}", filePath);
                config = ConfigLoader.loadFromFile(filePath);
            } else if (params.has("config")) {
                String jsonString = params.get("config");
                logger.info("从命令行 JSON 字符串加载配置");
                config = ConfigLoader.loadFromJsonString(jsonString);
            } else if (args.length == 1 && !args[0].startsWith("--")) {
                logger.warn("使用已弃用的参数格式，建议使用 --file 或 --config 参数");
                String configPath = args[0];
                logger.info("从文件加载配置: {}", configPath);
                config = ConfigLoader.loadFromFile(configPath);
            } else {
                printUsage();
                System.exit(1);
            }

            PluginLoader pluginLoader = new PluginLoader();

            JobExecutor executor = new JobExecutor(pluginLoader);
            executor.execute(config);

            logger.info("Job 执行成功");
            System.exit(0);
        } catch (IllegalArgumentException e) {
            logger.error("配置错误: {}", e.getMessage());
            System.err.println("配置错误: " + e.getMessage());
            printUsage();
            System.exit(1);
        } catch (Exception e) {
            logger.error("Job 执行失败", e);
            System.err.println("Job 执行失败: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("用法:");
        System.err.println("  java -jar flink-etl-tool.jar --file <config.json>");
        System.err.println("  java -jar flink-etl-tool.jar --config '<json-string>'");
        System.err.println();
        System.err.println("参数:");
        System.err.println("  --file <path>      从文件加载配置");
        System.err.println("  --config <json>    从 JSON 字符串加载配置");
        System.err.println();
        System.err.println("示例:");
        System.err.println("  java -jar flink-etl-tool.jar --file config/mysql-to-console.json");
        System.err.println("  java -jar flink-etl-tool.jar --config '{\"job\":{\"name\":\"test\",\"mode\":\"batch\"},...}'");
        System.err.println();
        System.err.println("注意:");
        System.err.println("  旧格式（直接传文件路径）已弃用，建议使用 --file 参数");
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn clean compile`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-client/src/main/java/com/etl/client/EtlClient.java
git commit -m "feat: EtlClient 使用 ParameterTool 支持多种参数格式"
```

---

## Chunk 3: 测试和文档

### Task 5: 创建测试脚本

**Files:**
- Create: `docs/examples/test-parameters.sh`

- [ ] **Step 1: 创建测试脚本**

创建 `docs/examples/test-parameters.sh`：

```bash
#!/bin/bash

set -e

JAR_FILE="flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar"
CONFIG_FILE="docs/examples/mysql-to-console.json"

echo "===== 测试 1: 使用 --file 参数 ====="
java -jar $JAR_FILE --file $CONFIG_FILE

echo ""
echo "===== 测试 2: 使用 --config 参数 ====="
JSON_STRING=$(cat $CONFIG_FILE | tr '\n' ' ')
java -jar $JAR_FILE --config "$JSON_STRING"

echo ""
echo "===== 测试 3: 文件不存在错误 ====="
java -jar $JAR_FILE --file /non/existent/file.json || echo "预期错误"

echo ""
echo "===== 测试 4: JSON 解析错误 ====="
java -jar $JAR_FILE --config "{ invalid json }" || echo "预期错误"

echo ""
echo "===== 测试 5: 向后兼容性 ====="
java -jar $JAR_FILE $CONFIG_FILE

echo ""
echo "===== 所有测试完成 ====="
```

- [ ] **Step 2: 添加执行权限**

Run: `chmod +x docs/examples/test-parameters.sh`

- [ ] **Step 3: 提交**

```bash
git add docs/examples/test-parameters.sh
git commit -m "test: 添加参数传递测试脚本"
```

---

### Task 6: 更新文档

**Files:**
- Modify: `CLAUDE.md`
- Create: `docs/parameter-usage.md`

- [ ] **Step 1: 创建参数使用文档**

创建 `docs/parameter-usage.md`：

```markdown
# 参数传递使用说明

## 概述

ETL 工具支持两种配置传递方式：

1. **文件路径方式**：使用 `--file` 参数传递配置文件路径
2. **JSON 字符串方式**：使用 `--config` 参数直接传递 JSON 配置

## 使用方法

### 方式一：从文件加载配置

```bash
java -jar flink-etl-client-1.0.0-SNAPSHOT.jar --file config/mysql-to-console.json
```

**优点：**
- 配置文件易于维护和版本控制
- 支持复杂的配置结构
- 推荐用于生产环境

### 方式二：从 JSON 字符串加载配置

```bash
java -jar flink-etl-client-1.0.0-SNAPSHOT.jar --config '{"job":{"name":"test","mode":"batch"},"source":{...},"sink":{...}}'
```

**优点：**
- 无需创建配置文件
- 适用于临时测试和动态配置生成

## 错误处理

### 文件不存在

```bash
配置错误: 配置文件不存在: /non/existent/file.json
```

### JSON 解析失败

```bash
配置错误: 配置解析失败: Unexpected character...
```

## 向后兼容性

旧版本参数格式仍然支持（已弃用）：

```bash
java -jar flink-etl-tool.jar config/mysql-to-console.json
```

建议尽快迁移到新参数格式。
```

- [ ] **Step 2: 更新 CLAUDE.md**

在 "常用命令" 部分更新：

```markdown
## 常用命令

```bash
# 编译项目
mvn clean compile

# 打包项目
mvn clean package

# 运行 ETL 任务
# 方式一：从文件加载配置（推荐）
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-console.json

# 方式二：从 JSON 字符串加载配置
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --config '{"job":{...},"source":{...},"sink":{...}}'

# 安装到本地仓库
mvn clean install -DskipTests
```
```

- [ ] **Step 3: 提交**

```bash
git add docs/parameter-usage.md CLAUDE.md
git commit -m "docs: 更新参数传递使用文档"
```

---

### Task 7: 最终验证

- [ ] **Step 1: 运行完整测试**

Run: `mvn clean test`
Expected: BUILD SUCCESS

- [ ] **Step 2: 打包项目**

Run: `mvn clean package -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: 验证实际运行**

测试各种参数格式和错误处理。

- [ ] **Step 4: 最终提交**

```bash
git add -A
git commit -m "feat: 完成参数传递优化功能"
```

---

## 验收标准

- [x] 支持 `--file` 参数传递配置文件路径
- [x] 支持 `--config` 参数传递 JSON 字符串
- [x] 文件不存在时抛出明确的错误信息
- [x] JSON 解析失败时抛出明确的错误信息
- [x] 使用 Flink ParameterTool 处理参数
- [x] 保持向后兼容性
- [x] 所有测试通过
- [x] 文档更新完整