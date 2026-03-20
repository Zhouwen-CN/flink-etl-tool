# 多 Source 和多 Sink 支持 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 ETL 框架从单 source/sink 扩展为支持多个 source/sink，通过 `outputTable`/`inputTable` 关联数据流。

**Architecture:** 修改 JobConfig 配置结构为数组形式，更新 ConfigParser 校验逻辑支持表名唯一性校验，JobBuilder 遍历数组构建多个 source/sink。

**Tech Stack:** Java 11, Flink 1.19.0, Jackson, Lombok, JUnit 5

---

## 涉及文件

| 操作 | 文件 | 说明 |
|------|------|------|
| 修改 | `flink-etl-core/src/main/java/com/etl/core/config/JobConfig.java` | 改为数组 |
| 修改 | `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java` | 更新校验逻辑 |
| 修改 | `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java` | 遍历数组 |
| 创建 | `flink-etl-core/src/test/java/com/etl/core/config/ConfigParserTest.java` | 配置解析单元测试 |
| 修改 | `docs/examples/mysql-to-console.json` | 更新示例 |
| 修改 | `docs/examples/mysql-custom-sql-to-console.json` | 更新示例 |
| 修改 | `docs/examples/mysql-to-mysql.json` | 更新示例 |
| 修改 | `docs/examples/csv-to-console.json` | 更新示例 |
| 更新 | `CLAUDE.md` | 更新文档 |

---

### Task 1: 修改 JobConfig 配置结构

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/config/JobConfig.java`

- [ ] **Step 1: 修改 JobConfig.java**

将 `source` 改为 `sources`，`sink` 改为 `sinks`：

```java
package com.etl.core.config;

import lombok.Data;

import java.util.List;

/**
 * Job 完整配置
 */
@Data
public class JobConfig {
    private JobMeta job;
    private List<SourceConfig> sources;      // source 改为 sources 数组
    private List<TransformConfig> transforms;
    private List<SinkConfig> sinks;          // sink 改为 sinks 数组
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

---

### Task 2: 更新 ConfigParser 校验逻辑

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java`

- [ ] **Step 1: 更新 validate 方法**

替换整个 `validate` 方法，支持数组格式和表名唯一性校验：

```java
/**
 * 校验配置完整性
 *
 * @param config Job 配置
 */
private static void validate(JobConfig config) {
    // 校验 job 配置
    if (config.getJob() == null) {
        throw new IllegalArgumentException("缺少 job 配置");
    }
    if (config.getJob().getName() == null || config.getJob().getName().isEmpty()) {
        throw new IllegalArgumentException("缺少 job.name 配置");
    }
    if (config.getJob().getMode() == null) {
        throw new IllegalArgumentException("缺少 job.mode 配置");
    }

    // 校验 sources 数组
    if (config.getSources() == null || config.getSources().isEmpty()) {
        throw new IllegalArgumentException("缺少 sources 配置");
    }

    Set<String> outputTables = new HashSet<>();
    for (int i = 0; i < config.getSources().size(); i++) {
        SourceConfig source = config.getSources().get(i);
        if (source.getType() == null || source.getType().isEmpty()) {
            throw new IllegalArgumentException("缺少 sources[" + i + "].type 配置");
        }
        if (source.getOutputTable() == null || source.getOutputTable().isEmpty()) {
            throw new IllegalArgumentException("缺少 sources[" + i + "].outputTable 配置");
        }
        if (!outputTables.add(source.getOutputTable())) {
            throw new IllegalArgumentException("sources 中 outputTable 重复: " + source.getOutputTable());
        }
    }

    // 校验 transforms
    if (config.getTransforms() != null) {
        for (int i = 0; i < config.getTransforms().size(); i++) {
            TransformConfig transform = config.getTransforms().get(i);
            if (transform.getType() == null || transform.getType().isEmpty()) {
                throw new IllegalArgumentException("缺少 transforms[" + i + "].type 配置");
            }
            if (transform.getOutputTable() == null || transform.getOutputTable().isEmpty()) {
                throw new IllegalArgumentException("缺少 transforms[" + i + "].outputTable 配置");
            }
            if (!outputTables.add(transform.getOutputTable())) {
                throw new IllegalArgumentException("transforms 中 outputTable 重复或与 sources 冲突: " + transform.getOutputTable());
            }
        }
    }

    // 校验 sinks 数组
    if (config.getSinks() == null || config.getSinks().isEmpty()) {
        throw new IllegalArgumentException("缺少 sinks 配置");
    }
    for (int i = 0; i < config.getSinks().size(); i++) {
        SinkConfig sink = config.getSinks().get(i);
        if (sink.getType() == null || sink.getType().isEmpty()) {
            throw new IllegalArgumentException("缺少 sinks[" + i + "].type 配置");
        }
        if (sink.getInputTable() == null || sink.getInputTable().isEmpty()) {
            throw new IllegalArgumentException("缺少 sinks[" + i + "].inputTable 配置");
        }
    }
}
```

- [ ] **Step 2: 添加 import**

在文件头部添加：

```java
import java.util.HashSet;
import java.util.Set;
```

- [ ] **Step 3: 编译验证**

Run: `mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

---

### Task 3: 更新 JobBuilder 构建逻辑

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java`

- [ ] **Step 1: 修改 build 方法**

更新 `build` 方法，遍历 sources 和 sinks 数组：

```java
/**
 * 构建 Flink Job
 *
 * @param env    Flink 执行环境
 * @param config Job 配置
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public static void build(StreamExecutionEnvironment env, JobConfig config) {
    log.info("开始构建 Flink Job: {}", config.getJob().getName());
    // 创建 Table 环境
    StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

    // 1. 遍历所有 Source -> DataStream -> Table
    for (SourceConfig sourceConfig : config.getSources()) {
        String sourceType = sourceConfig.getType();
        SourcePlugin sourcePlugin = PluginLoader.loadSourcePlugin(sourceType);
        Source source = sourcePlugin.createSource(sourceConfig);
        DataStream<Row> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceType);

        String outputTable = sourceConfig.getOutputTable();
        stEnv.createTemporaryView(outputTable, sourceStream);
        log.info("注册 Table: {}", outputTable);
    }

    // 2. Transform 链式处理
    List<TransformConfig> transforms = config.getTransforms();
    if (transforms != null && !transforms.isEmpty()) {
        for (TransformConfig transformConfig : transforms) {
            TransformPlugin transformPlugin = PluginLoader.loadTransformPlugin(transformConfig.getType());
            Table transformed = transformPlugin.transform(transformConfig, stEnv);

            // 将 Transform 结果注册为中间表，供后续 SQL 引用
            String transformOutputTable = transformConfig.getOutputTable();
            stEnv.createTemporaryView(transformOutputTable, transformed);
            log.info("注册中间表：{}", transformOutputTable);

            log.info("Transform 应用成功：{}", transformConfig.getType());
        }
    }

    // 3. 遍历所有 Sink: Table -> DataStream -> Sink
    for (SinkConfig sinkConfig : config.getSinks()) {
        String inputTable = sinkConfig.getInputTable();
        DataStream<Row> resultStream;
        try {
            Table sinkTable = stEnv.from(inputTable);
            resultStream = stEnv.toDataStream(sinkTable);
            log.info("Table 转换为 DataStream: {}", inputTable);
        } catch (Exception e) {
            throw new IllegalArgumentException("无法从表 '" + inputTable + "' 读取数据，请检查 inputTable 配置是否正确，或上游 source.outputTable / transform.outputTable 是否已正确配置", e);
        }

        SinkPlugin sinkPlugin = PluginLoader.loadSinkPlugin(sinkConfig.getType());
        SinkFunction<Row> sink = sinkPlugin.createSink(sinkConfig);
        resultStream.addSink(sink);
        log.info("Sink 创建成功: {} -> {}", inputTable, sinkConfig.getType());
    }

    log.info("Flink Job 构建完成");
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

---

### Task 4: 添加单元测试

**Files:**
- Create: `flink-etl-core/src/test/java/com/etl/core/config/ConfigParserTest.java`

- [ ] **Step 1: 创建测试目录**

Run: `mkdir -p flink-etl-core/src/test/java/com/etl/core/config`

- [ ] **Step 2: 创建测试类**

```java
package com.etl.core.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConfigParser 单元测试
 */
class ConfigParserTest {

    @Test
    void testParseValidMultiSourceConfig() {
        String json = """
            {
              "job": { "name": "test", "mode": "batch" },
              "sources": [
                { "type": "mysql", "outputTable": "users", "config": {} }
              ],
              "sinks": [
                { "type": "console", "inputTable": "users", "config": {} }
              ]
            }
            """;

        JobConfig config = ConfigParser.parseFromString(json);

        assertNotNull(config);
        assertEquals("test", config.getJob().getName());
        assertEquals(1, config.getSources().size());
        assertEquals("mysql", config.getSources().get(0).getType());
        assertEquals("users", config.getSources().get(0).getOutputTable());
        assertEquals(1, config.getSinks().size());
        assertEquals("console", config.getSinks().get(0).getType());
        assertEquals("users", config.getSinks().get(0).getInputTable());
    }

    @Test
    void testParseMultipleSourcesAndSinks() {
        String json = """
            {
              "job": { "name": "multi", "mode": "batch" },
              "sources": [
                { "type": "mysql", "outputTable": "users", "config": {} },
                { "type": "mysql", "outputTable": "orders", "config": {} }
              ],
              "sinks": [
                { "type": "console", "inputTable": "users", "config": {} },
                { "type": "mysql", "inputTable": "orders", "config": {} }
              ]
            }
            """;

        JobConfig config = ConfigParser.parseFromString(json);

        assertEquals(2, config.getSources().size());
        assertEquals(2, config.getSinks().size());
    }

    @Test
    void testMissingSourcesThrowsException() {
        String json = """
            {
              "job": { "name": "test", "mode": "batch" },
              "sinks": [
                { "type": "console", "inputTable": "users", "config": {} }
              ]
            }
            """;

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ConfigParser.parseFromString(json)
        );
        assertTrue(ex.getMessage().contains("缺少 sources"));
    }

    @Test
    void testMissingSinksThrowsException() {
        String json = """
            {
              "job": { "name": "test", "mode": "batch" },
              "sources": [
                { "type": "mysql", "outputTable": "users", "config": {} }
              ]
            }
            """;

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ConfigParser.parseFromString(json)
        );
        assertTrue(ex.getMessage().contains("缺少 sinks"));
    }

    @Test
    void testDuplicateOutputTableThrowsException() {
        String json = """
            {
              "job": { "name": "test", "mode": "batch" },
              "sources": [
                { "type": "mysql", "outputTable": "users", "config": {} },
                { "type": "mysql", "outputTable": "users", "config": {} }
              ],
              "sinks": [
                { "type": "console", "inputTable": "users", "config": {} }
              ]
            }
            """;

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ConfigParser.parseFromString(json)
        );
        assertTrue(ex.getMessage().contains("outputTable 重复"));
    }
}
```

- [ ] **Step 3: 运行测试**

Run: `mvn test -pl flink-etl-core -Dtest=ConfigParserTest`
Expected: Tests run: 5, Failures: 0, Errors: 0

- [ ] **Step 4: 提交测试代码**

```bash
git add flink-etl-core/src/test/java/com/etl/core/config/ConfigParserTest.java
git commit -m "test: 添加 ConfigParser 多 Source/Sink 单元测试"
```

---

### Task 5: 更新示例配置文件

**Files:**
- Modify: `docs/examples/mysql-to-console.json`
- Modify: `docs/examples/mysql-custom-sql-to-console.json`
- Modify: `docs/examples/mysql-to-mysql.json`
- Modify: `docs/examples/csv-to-console.json`

- [ ] **Step 1: 更新 mysql-to-console.json**

```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch",
    "parallelism": 1
  },
  "sources": [
    {
      "type": "mysql",
      "outputTable": "users",
      "config": {
        "url": "jdbc:mysql://118.145.119.51:3306/test_db",
        "table": "user_table",
        "username": "root",
        "password": "Zw$8rb8*",
        "splitColumn": "id",
        "batchSize": 1,
        "schema": [
          { "name": "id", "type": "INT" },
          { "name": "name", "type": "STRING" }
        ]
      }
    }
  ],
  "transforms": [
    {
      "type": "sql",
      "outputTable": "users_transformed",
      "config": {
        "sql": "SELECT * FROM users WHERE id > 0"
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "users_transformed",
      "config": {}
    }
  ]
}
```

- [ ] **Step 2: 更新 mysql-custom-sql-to-console.json**

将 `source` 改为 `sources`，`sink` 改为 `sinks`（数组格式）

- [ ] **Step 3: 更新 mysql-to-mysql.json**

将 `source` 改为 `sources`，`sink` 改为 `sinks`（数组格式）

- [ ] **Step 4: 更新 csv-to-console.json**

将 `source` 改为 `sources`，`sink` 改为 `sinks`（数组格式）

- [ ] **Step 5: 提交示例更新**

```bash
git add docs/examples/*.json
git commit -m "docs: 更新示例配置为新数组格式"
```

---

### Task 6: 更新 CLAUDE.md 文档

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: 更新配置文件格式说明**

在配置文件格式章节，将 `source` 改为 `sources`，`sink` 改为 `sinks`，并说明数组格式。

更新示例配置：

```json
{
  "job": {
    "name": "job-name",
    "mode": "batch",
    "parallelism": 4
  },
  "sources": [
    {
      "type": "mysql",
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

- [ ] **Step 2: 提交文档更新**

```bash
git add CLAUDE.md
git commit -m "docs: 更新 CLAUDE.md 配置格式说明"
```

---

### Task 7: 集成测试

- [ ] **Step 1: 编译整个项目**

Run: `mvn clean compile`
Expected: BUILD SUCCESS

- [ ] **Step 2: 运行所有测试**

Run: `mvn test -pl flink-etl-core`
Expected: Tests run: 5, Failures: 0, Errors: 0

- [ ] **Step 3: 打包项目**

Run: `mvn clean package -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 4: 运行示例任务验证**

Run: `java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-console.json`
Expected: 任务正常执行

---

## 完成标准

- [x] JobConfig 支持多 source/sink 数组格式
- [x] ConfigParser 校验表名唯一性
- [x] JobBuilder 遍历数组构建拓扑
- [x] 单元测试覆盖配置解析逻辑
- [x] 示例配置更新为新格式
- [x] 文档更新
- [x] 编译和测试通过