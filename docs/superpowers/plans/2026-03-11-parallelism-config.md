# 并行度配置功能实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 Flink ETL 配置文件新增并行度配置功能，允许用户通过 JSON 配置控制 Job 的并行度。

**Architecture:** 在现有配置系统基础上扩展，在 `JobMeta` 类中添加 `parallelism` 字段，在 `JobExecutor` 创建 Flink 执行环境时应用该配置。

**Tech Stack:** Java 11, Jackson (JSON 解析), Apache Flink 1.19.0

---

## 文件结构

| 文件                                                                                 | 职责                           | 操作 |
|------------------------------------------------------------------------------------|------------------------------|----|
| `flink-etl-core/src/main/java/com/etl/core/localFileSourceConfig/JobMeta.java`     | Job 元信息配置类，新增 parallelism 字段 | 修改 |
| `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java`                   | Flink 执行环境创建，应用并行度配置         | 修改 |
| `flink-etl-core/src/test/java/com/etl/core/localFileSourceConfig/JobMetaTest.java` | JobMeta 配置解析单元测试             | 创建 |
| `docs/examples/mysql-to-console.json`                                              | 示例配置文件，添加 parallelism 配置     | 修改 |

---

## Chunk 1: 配置模型扩展

### Task 1: JobMeta 添加 parallelism 字段

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/localFileSourceConfig/JobMeta.java`

- [ ] **Step 1: 为 JobMeta 添加 parallelism 字段**

在 `JobMeta.java` 中添加 `parallelism` 字段及 getter/setter：

```java
package com.etl.core.localFileSourceConfig;

/**
 * Job 元信息配置
 */
public class JobMeta {
    private String name;
    private String mode;
    private Integer parallelism;  // 新增：并行度配置，null 表示使用 Flink 默认值

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

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交配置模型变更**

```bash
git add flink-etl-core/src/main/java/com/etl/core/localFileSourceConfig/JobMeta.java
git commit -m "feat: JobMeta 添加 parallelism 并行度配置字段"
```

---

### Task 2: JobMeta 单元测试

**Files:**

- Create: `flink-etl-core/src/test/java/com/etl/core/localFileSourceConfig/JobMetaTest.java`

- [ ] **Step 1: 创建 JobMeta 单元测试**

```java
package com.etl.core.localFileSourceConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JobMeta 配置解析测试
 */
class JobMetaTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testParseJobMetaWithParallelism() throws Exception {
        String json = "{\"name\":\"test-job\",\"mode\":\"batch\",\"parallelism\":4}";

        JobMeta meta = mapper.readValue(json, JobMeta.class);

        assertEquals("test-job", meta.getName());
        assertEquals("batch", meta.getMode());
        assertEquals(4, meta.getParallelism());
    }

    @Test
    void testParseJobMetaWithoutParallelism() throws Exception {
        String json = "{\"name\":\"test-job\",\"mode\":\"batch\"}";

        JobMeta meta = mapper.readValue(json, JobMeta.class);

        assertEquals("test-job", meta.getName());
        assertEquals("batch", meta.getMode());
        assertNull(meta.getParallelism());
    }

    @Test
    void testSerializeJobMetaWithParallelism() throws Exception {
        JobMeta meta = new JobMeta();
        meta.setName("test-job");
        meta.setMode("batch");
        meta.setParallelism(8);

        String json = mapper.writeValueAsString(meta);

        assertTrue(json.contains("\"name\":\"test-job\""));
        assertTrue(json.contains("\"mode\":\"batch\""));
        assertTrue(json.contains("\"parallelism\":8"));
    }
}
```

- [ ] **Step 2: 运行测试验证**

Run: `mvn test -pl flink-etl-core -Dtest=JobMetaTest`
Expected: 3 tests passed

- [ ] **Step 3: 提交测试代码**

```bash
git add flink-etl-core/src/test/java/com/etl/core/localFileSourceConfig/JobMetaTest.java
git commit -m "test: 添加 JobMeta 并行度配置解析测试"
```

---

## Chunk 2: 执行环境并行度配置

### Task 3: JobExecutor 应用并行度配置

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java`

- [ ] **Step 1: 修改 createExecutionEnvironment 方法**

在创建 Flink 执行环境后，检查并应用并行度配置：

```java
private StreamExecutionEnvironment createExecutionEnvironment(JobConfig localFileSourceConfig) {
    String mode = localFileSourceConfig.getJob().getMode();
    Integer parallelism = localFileSourceConfig.getJob().getParallelism();
    logger.info("创建 Flink 执行环境: mode={}, parallelism={}", mode, parallelism);

    StreamExecutionEnvironment env;
    if ("batch".equals(mode)) {
        // 批处理模式
        Configuration configuration = new Configuration();
        configuration.setString("execution.runtime-mode", "BATCH");
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    } else {
        // 流处理模式
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    // 设置并行度（如果配置了）
    if (parallelism != null) {
        env.setParallelism(parallelism);
        logger.info("设置 Job 并行度: {}", parallelism);
    }

    return env;
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交执行环境变更**

```bash
git add flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java
git commit -m "feat: JobExecutor 支持从配置设置 Flink 并行度"
```

---

### Task 4: JobExecutor 集成测试

**Files:**
- Create: `flink-etl-core/src/test/java/com/etl/core/job/JobExecutorTest.java`

- [ ] **Step 1: 创建 JobExecutor 集成测试**

```java
package com.etl.core.job;

import com.etl.core.localFileSourceConfig.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JobExecutor 集成测试
 */
class JobExecutorTest {

    @Test
    void testCreateExecutionEnvironmentWithParallelism() {
        // 准备配置
        JobConfig localFileSourceConfig = new JobConfig();
        JobMeta jobMeta = new JobMeta();
        jobMeta.setName("test-job");
        jobMeta.setMode("batch");
        jobMeta.setParallelism(4);
        localFileSourceConfig.setJob(jobMeta);
        localFileSourceConfig.setSource(new SourceConfig());
        localFileSourceConfig.setSink(new SinkConfig());

        // 创建执行环境
        JobExecutor executor = new JobExecutor();
        StreamExecutionEnvironment env = executor.createExecutionEnvironment(localFileSourceConfig);

        // 验证并行度
        assertEquals(4, env.getParallelism());
    }

    @Test
    void testCreateExecutionEnvironmentWithoutParallelism() {
        // 准备配置（不设置并行度）
        JobConfig localFileSourceConfig = new JobConfig();
        JobMeta jobMeta = new JobMeta();
        jobMeta.setName("test-job");
        jobMeta.setMode("batch");
        // 不设置 parallelism
        localFileSourceConfig.setJob(jobMeta);
        localFileSourceConfig.setSource(new SourceConfig());
        localFileSourceConfig.setSink(new SinkConfig());

        // 创建执行环境
        JobExecutor executor = new JobExecutor();
        StreamExecutionEnvironment env = executor.createExecutionEnvironment(localFileSourceConfig);

        // 验证使用默认并行度（Flink 默认值，通常是 CPU 核心数）
        assertTrue(env.getParallelism() > 0);
    }
}
```

**注意：** `createExecutionEnvironment` 方法是 private 的，需要改为 package-private 或 public 以便测试。或者使用反射进行测试。

- [ ] **Step 2: 修改 createExecutionEnvironment 可见性**

将 `JobExecutor.java` 中的 `createExecutionEnvironment` 方法从 `private` 改为包级可见：

```java
StreamExecutionEnvironment createExecutionEnvironment(JobConfig localFileSourceConfig) {
    // ... 方法体不变
}
```

- [ ] **Step 3: 运行测试验证**

Run: `mvn test -pl flink-etl-core -Dtest=JobExecutorTest`
Expected: 2 tests passed

- [ ] **Step 4: 提交测试代码**

```bash
git add flink-etl-core/src/test/java/com/etl/core/job/JobExecutorTest.java
git add flink-etl-core/src/main/java/com/etl/core/job/JobExecutor.java
git commit -m "test: 添加 JobExecutor 并行度配置集成测试"
```

---

## Chunk 3: 文档和示例更新

### Task 5: 更新示例配置文件

**Files:**
- Modify: `docs/examples/mysql-to-console.json`

- [ ] **Step 1: 更新 mysql-to-console.json 添加并行度配置**

```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch",
    "parallelism": 4
  },
  "source": {
    "type": "mysql",
    "localFileSourceConfig": {
      "url": "jdbc:mysql://localhost:3306/test",
      "username": "root",
      "password": "root",
      "table": "user",
      "splitColumn": "id"
    }
  },
  "sink": {
    "type": "console",
    "localFileSourceConfig": {}
  }
}
```

- [ ] **Step 2: 提交示例配置更新**

```bash
git add docs/examples/mysql-to-console.json
git commit -m "docs: 示例配置添加 parallelism 并行度配置"
```

---

### Task 6: 更新 CLAUDE.md 文档

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: 更新配置文件格式说明**

在 `CLAUDE.md` 的「配置文件格式」章节更新：

```markdown
## 配置文件格式

配置采用 DataX 风格的 JSON 结构：

```json
{
  "job": {
    "name": "job-name",
    "mode": "batch",
    "parallelism": 4
  },
  "source": { "type": "mysql", "localFileSourceConfig": { ... } },
  "transform": { "type": "field-mapping", "localFileSourceConfig": { ... } },
  "sink": { "type": "console", "localFileSourceConfig": { ... } }
}
```

**job 配置项说明：**
- `name`: Job 名称
- `mode`: 执行模式，支持 `batch`（批处理）或 `streaming`（流处理）
- `parallelism`: 并行度配置（可选），不设置则使用 Flink 默认值

示例配置位于 `docs/examples/` 目录。
```

- [ ] **Step 2: 提交文档更新**

```bash
git add CLAUDE.md
git commit -m "docs: 更新 CLAUDE.md 并行度配置说明"
```

---

### Task 7: 端到端验证

- [ ] **Step 1: 打包项目**

Run: `mvn clean package -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 2: 运行示例 Job 验证并行度**

Run: `java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar docs/examples/mysql-to-console.json`
Expected: 日志中显示 "设置 Job 并行度: 4"

- [ ] **Step 3: 最终提交**

```bash
git add .
git commit -m "feat: 完成并行度配置功能"
```

---

## 验收标准

- [ ] `JobMeta` 类包含 `parallelism` 字段
- [ ] `JobExecutor` 正确应用并行度配置到 Flink 执行环境
- [ ] 单元测试覆盖配置解析场景
- [ ] 集成测试验证并行度生效
- [ ] 示例配置文件更新
- [ ] 文档更新
- [ ] 端到端测试通过