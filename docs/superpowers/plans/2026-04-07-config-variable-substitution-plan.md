# 配置文件变量替换功能实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 ETL 工具添加配置变量替换功能，支持 `${variable}` 占位符通过命令行参数传递值

**Architecture:** 在 CliArgumentParser 中集中处理变量替换，loadFromFile/loadFromJsonString 返回 JSON String，parse 方法统一执行变量替换和检查，ConfigParser 只负责解析 JSON String

**Tech Stack:** Apache Commons Text (StrSubstitutor 1.10.0), JUnit 5, ParameterTool (Flink)

---

## 文件结构

**创建文件：**
- `flink-etl-core/src/test/java/com/etl/core/config/CliArgumentParserTest.java` - CliArgumentParser 单元测试

**修改文件：**
- `pom.xml` - 添加版本管理和依赖声明
- `flink-etl-core/pom.xml` - 添加 commons-text 依赖
- `flink-etl-core/src/main/java/com/etl/core/config/CliArgumentParser.java` - 新增方法和重构现有方法
- `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java` - 删除废弃方法
- `CLAUDE.md` - 更新文档
- `PLUGINS.md` - 更新文档

---

### Task 1: 添加依赖到 pom.xml

**Files:**
- Modify: `pom.xml` (properties 和 dependencyManagement 部分)
- Modify: `flink-etl-core/pom.xml` (dependencies 部分)

- [ ] **Step 1: 在父 pom.xml 中添加版本号**

在 `pom.xml` 的 `<properties>` 部分添加（约第 30 行后）：

```xml
<commons-text.version>1.10.0</commons-text.version>
```

- [ ] **Step 2: 在父 pom.xml 中添加依赖管理**

在 `pom.xml` 的 `<dependencyManagement>` 部分，在 `json-path` 依赖后添加（约第 74 行后）：

```xml
<!-- Apache Commons Text - 变量替换 -->
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-text</artifactId>
    <version>${commons-text.version}</version>
</dependency>
```

- [ ] **Step 3: 在 flink-etl-core/pom.xml 中添加依赖**

在 `flink-etl-core/pom.xml` 的 `<dependencies>` 部分，在 `json-path` 依赖后添加（约第 23 行后）：

```xml
<!-- Apache Commons Text - 变量替换 -->
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-text</artifactId>
</dependency>
```

- [ ] **Step 4: 验证依赖可用**

运行：`mvn dependency:tree -Dverbose -pl flink-etl-core | grep commons-text`

预期输出：包含 `org.apache.commons:commons-text:jar:1.10.0`

- [ ] **Step 5: 提交依赖更改**

```bash
git add pom.xml flink-etl-core/pom.xml
git commit -m "feat: 添加 commons-text 依赖用于配置变量替换

- 在父 pom.xml 中添加版本管理和依赖声明
- 在 flink-etl-core/pom.xml 中添加 commons-text 依赖

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 2: 编写 CliArgumentParserTest 测试

**Files:**
- Create: `flink-etl-core/src/test/java/com/etl/core/config/CliArgumentParserTest.java`

- [ ] **Step 1: 创建测试文件骨架**

创建 `flink-etl-core/src/test/java/com/etl/core/config/CliArgumentParserTest.java`：

```java
package com.etl.core.config;

import com.etl.core.config.JobConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CliArgumentParser 单元测试 - 变量替换功能
 */
class CliArgumentParserTest {

    // 测试将在此逐步添加
}
```

- [ ] **Step 2: 编写测试 - 单个变量替换**

在 `CliArgumentParserTest.java` 中添加测试：

```java
@Test
void testSingleVariableSubstitution() {
    String[] args = {
        "--config", "{\"job\":{\"name\":\"${job_name}\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"console\",\"outputTable\":\"t\",\"config\":{}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}",
        "--job_name", "test-job"
    };

    JobConfig config = CliArgumentParser.parse(args);

    assertNotNull(config);
    assertEquals("test-job", config.getJob().getName());
}
```

- [ ] **Step 3: 运行测试验证失败**

运行：`mvn test -Dtest=CliArgumentParserTest#testSingleVariableSubstitution -pl flink-etl-core`

预期：FAIL（CliArgumentParser 尚未实现变量替换）

- [ ] **Step 4: 编写测试 - 多个变量替换**

添加测试：

```java
@Test
void testMultipleVariablesSubstitution() {
    String[] args = {
        "--config", "{\"job\":{\"name\":\"${job_name}\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"jdbc\",\"outputTable\":\"t\",\"config\":{\"url\":\"${db_url}\"}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}",
        "--job_name", "test",
        "--db_url", "jdbc:mysql://localhost:3306/test"
    };

    JobConfig config = CliArgumentParser.parse(args);

    assertEquals("test", config.getJob().getName());
    assertEquals("jdbc:mysql://localhost:3306/test",
        config.getSources().get(0).getConfig().get("url"));
}
```

- [ ] **Step 5: 编写测试 - 变量带默认值**

添加测试：

```java
@Test
void testVariableWithDefaultValue() {
    String[] args = {
        "--config", "{\"job\":{\"name\":\"${job_name:-default-job}\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"console\",\"outputTable\":\"t\",\"config\":{}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}"
    };

    JobConfig config = CliArgumentParser.parse(args);

    assertEquals("default-job", config.getJob().getName());
}
```

- [ ] **Step 6: 编写测试 - 变量未定义抛异常**

添加测试：

```java
@Test
void testUndefinedVariableThrowsException() {
    String[] args = {
        "--config", "{\"job\":{\"name\":\"${undefined_var}\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"console\",\"outputTable\":\"t\",\"config\":{}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}"
    };

    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> CliArgumentParser.parse(args)
    );

    assertTrue(ex.getMessage().contains("变量 'undefined_var' 未定义"));
    assertTrue(ex.getMessage().contains("--undefined_var"));
}
```

- [ ] **Step 7: 编写测试 - 变量值为空字符串**

添加测试：

```java
@Test
void testEmptyVariableValue() {
    String[] args = {
        "--config", "{\"job\":{\"name\":\"test\",\"mode\":\"batch\"},\"sources\":[{\"type\":\"jdbc\",\"outputTable\":\"t\",\"config\":{\"url\":\"${db_url}\"}}],\"sinks\":[{\"type\":\"console\",\"inputTable\":\"t\",\"config\":{}}]}",
        "--db_url", ""
    };

    JobConfig config = CliArgumentParser.parse(args);

    assertEquals("", config.getSources().get(0).getConfig().get("url"));
}
```

- [ ] **Step 9: 提交测试文件**

```bash
git add flink-etl-core/src/test/java/com/etl/core/config/CliArgumentParserTest.java
git commit -m "test: 新增 CliArgumentParser 变量替换功能测试

测试覆盖：
- 单个/多个变量替换
- 默认值支持
- 未定义变量严格检查
- 空值处理

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 3: 实现 CliArgumentParser 变量替换功能

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/config/CliArgumentParser.java`

- [ ] **Step 1: 添加导入语句**

在 `CliArgumentParser.java` 文件顶部添加导入：

```java
import org.apache.commons.text.StrSubstitutor;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
```

- [ ] **Step 2: 实现 substituteVariables 方法**

在 `CliArgumentParser.java` 中添加私有方法（在 `tryDecodeBase64` 方法后）：

```java
/**
 * 使用 StrSubstitutor 替换配置中的变量
 *
 * 支持格式：
 * - ${variable} - 变量不存在时保留占位符
 * - ${variable:-default} - 变量不存在时使用默认值
 *
 * @param json JSON 配置字符串
 * @param variables 变量映射（从 ParameterTool.getProperties() 获取）
 * @return 替换后的 JSON 字符串
 */
private static String substituteVariables(String json, Map<String, String> variables) {
    StrSubstitutor substitutor = new StrSubstitutor(variables);
    return substitutor.replace(json);
}
```

- [ ] **Step 3: 实现 checkUnresolvedVariables 方法**

添加私有方法：

```java
/**
 * 检查 JSON 字符串中是否存在未替换的变量占位符
 *
 * 严格模式：发现任何 ${...} 格式的占位符都会抛出异常
 *
 * @param json 替换后的 JSON 字符串
 * @throws IllegalArgumentException 如果存在未替换的变量
 */
private static void checkUnresolvedVariables(String json) {
    Pattern pattern = Pattern.compile("\\$\\{[^}]+\\}");
    Matcher matcher = pattern.matcher(json);

    if (matcher.find()) {
        String unresolvedVar = matcher.group();
        // 提取变量名（去掉 ${ 和 }）
        String varName = unresolvedVar.substring(2, unresolvedVar.length() - 1);
        // 提取实际变量名（去掉默认值部分 :-default）
        String actualVarName = varName.split(":-")[0];

        throw new IllegalArgumentException(
            String.format("配置变量替换失败：变量 '%s' 未定义，请通过 --%s 参数传递",
                actualVarName, actualVarName)
        );
    }
}
```

- [ ] **Step 4: 实现 readFileContent 方法**

添加私有方法：

```java
/**
 * 读取文件内容为字符串
 *
 * @param filePath 文件路径
 * @return 文件内容
 * @throws IllegalArgumentException 读取失败时抛出
 */
private static String readFileContent(String filePath) {
    try {
        return new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
    } catch (IOException e) {
        throw new IllegalArgumentException("读取配置文件失败: " + e.getMessage(), e);
    }
}
```

- [ ] **Step 5: 修改 loadFromFile 方法**

修改 `loadFromFile` 方法，改为返回 `String`（约第 78-98 行）：

```java
/**
 * 从文件加载配置为 JSON 字符串
 *
 * @param filePath 配置文件路径
 * @return JSON 字符串
 */
private static String loadFromFile(String filePath) {
    if (filePath == null || filePath.trim().isEmpty()) {
        throw new IllegalArgumentException("--file 参数值不能为空");
    }

    if (!Files.exists(Paths.get(filePath))) {
        throw new IllegalArgumentException("配置文件不存在: " + filePath);
    }

    if (!new File(filePath).isFile()) {
        throw new IllegalArgumentException("路径不是文件: " + filePath);
    }

    log.info("从文件加载配置: {}", filePath);
    return readFileContent(filePath);
}
```

- [ ] **Step 6: 修改 loadFromJsonString 方法**

修改 `loadFromJsonString` 方法，改为返回 `String`（约第 109-124 行）：

```java
/**
 * 从命令行参数加载配置为 JSON 字符串
 * 支持 JSON 字符串或 Base64 编码
 *
 * @param input JSON 字符串或 Base64 编码
 * @return JSON 字符串
 */
private static String loadFromJsonString(String input) {
    if (input == null || input.trim().isEmpty()) {
        throw new IllegalArgumentException("--config 参数值不能为空");
    }

    String json = tryDecodeBase64(input);
    log.info("从命令行 JSON 字符串加载配置");
    return json;
}
```

- [ ] **Step 7: 重构 parse 方法**

修改 `parse` 方法，统一执行变量替换（约第 39-50 行）：

```java
/**
 * 解析命令行参数并返回 Job 配置
 *
 * @param args 命令行参数
 * @return Job 配置
 */
public static JobConfig parse(String[] args) {
    ParameterTool params = ParameterTool.fromArgs(args);

    // 加载配置源为 JSON 字符串
    String json;
    if (params.has("file")) {
        json = loadFromFile(params.get("file"));
    } else if (params.has("config")) {
        json = loadFromJsonString(params.get("config"));
    } else {
        printUsage();
        throw new IllegalArgumentException("缺少必要参数：请指定 --file 或 --config");
    }

    // 统一进行变量替换
    String substitutedJson = substituteVariables(json, params.getProperties());
    checkUnresolvedVariables(substitutedJson);

    // 解析和校验 JSON
    return ConfigParser.parseFromString(substitutedJson);
}
```

- [ ] **Step 8: 添加 Map 导入**

在文件顶部添加：

```java
import java.util.Map;
```

- [ ] **Step 9: 运行测试验证实现**

运行：`mvn test -Dtest=CliArgumentParserTest -pl flink-etl-core`

预期：所有测试通过（5-6 个测试，排除文件测试）

- [ ] **Step 10: 提交实现**

```bash
git add flink-etl-core/src/main/java/com/etl/core/config/CliArgumentParser.java
git commit -m "feat: 实现 CliArgumentParser 配置变量替换功能

新增方法：
- substituteVariables: 使用 StrSubstitutor 替换变量
- checkUnresolvedVariables: 严格检查未替换占位符
- readFileContent: 读取文件内容为字符串

重构方法：
- loadFromFile/loadFromJsonString: 返回 JSON String
- parse: 统一执行变量替换和检查

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 4: 删除 ConfigParser.parse(String configPath) 方法

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java`

- [ ] **Step 1: 删除 parse 方法**

在 `ConfigParser.java` 中删除 `parse(String configPath)` 方法（约第 23-36 行）：

删除整个方法：
```java
// 删除这部分代码
public static JobConfig parse(String configPath) {
    log.info("解析配置文件: {}", configPath);
    ...
}
```

- [ ] **Step 2: 验证编译成功**

运行：`mvn compile -pl flink-etl-core`

预期：编译成功，无错误

- [ ] **Step 3: 运行所有 ConfigParser 测试**

运行：`mvn test -Dtest=ConfigParserTest -pl flink-etl-core`

预期：所有测试通过（parse 方法未被测试使用）

- [ ] **Step 4: 提交更改**

```bash
git add flink-etl-core/src/main/java/com/etl/core/config/ConfigParser.java
git commit -m "refactor: 删除 ConfigParser.parse(String configPath) 废弃方法

职责调整：配置文件加载和变量替换由 CliArgumentParser 负责，
ConfigParser 只处理 JSON String 解析

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 5: 更新 CLAUDE.md 文档

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: 更新常用命令部分**

在 `CLAUDE.md` 的 "常用命令" 部分，在现有示例后添加（约第 20 行后）：

```markdown
# 运行带变量替换的 ETL 任务
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file docs/examples/mysql-to-console.json \
  --db_url jdbc:mysql://localhost:3306/test \
  --db_user root \
  --db_password secret
```

- [ ] **Step 2: 更新配置文件格式部分**

在 "配置文件格式" 部分添加变量替换说明（约第 50 行后）：

```markdown
**变量替换：**
配置支持变量替换，通过命令行参数动态传递值：
- 格式：`${variable}` 或 `${variable:-default}`（带默认值）
- 变量值通过命令行参数传递（例如：`--db_url xxx`）
- 未定义变量（无默认值）会抛异常，明确提示缺失参数

示例配置：
```json
{
  "sources": [{
    "config": {
      "url": "${db_url}",
      "user": "${db_user:-root}",
      "password": "${db_password}"
    }
  }]
}
```

运行命令：
```bash
--db_url jdbc:mysql://localhost:3306/test --db_password secret
```
```

- [ ] **Step 3: 提交文档更新**

```bash
git add CLAUDE.md
git commit -m "docs: 更新 CLAUDE.md - 补充配置变量替换说明

- 常用命令示例：带变量参数的运行方式
- 配置文件格式：变量语法和使用说明

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 6: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 添加变量替换章节**

在 `PLUGINS.md` 的 "配置文件格式" 章节后添加新章节：

```markdown
## 配置变量替换

### 功能说明

支持在配置文件中使用变量占位符，运行时通过命令行参数动态传递值。适用于：
- 不同环境（开发/测试/生产）的配置切换
- 敏感信息（密码等）不暴露在配置文件中
- 参数化配置，无需维护多份配置文件

### 变量格式

- **`${variable}`** - 变量不存在时保留占位符（严格模式下抛异常）
- **`${variable:-default}`** - 变量不存在时使用默认值

### 使用方式

**命令行传参：**
```bash
java -jar app.jar --file config.json \
  --db_url jdbc:mysql://localhost:3306/test \
  --db_user root \
  --db_password secret
```

**配置文件示例：**
```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch"
  },
  "sources": [{
    "type": "jdbc",
    "outputTable": "users",
    "config": {
      "url": "${db_url}",
      "username": "${db_user:-root}",
      "password": "${db_password}",
      "table": "users"
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "users"
  }]
}
```

### 变量参数规则

1. **所有非 `--file` 和 `--config` 的参数都会作为变量**
   - 拼写错误的参数也会被收集，需自行检查
   - 建议使用明确的参数名

2. **变量未定义时的行为**
   - 有默认值：使用默认值
   - 无默认值：抛异常 `"变量 'xxx' 未定义，请通过 --xxx 参数传递"`

3. **变量值为空字符串**
   - `--db_url ""` 会将变量设置为空字符串
   - 空字符串与未定义不同（空字符串不会触发异常）

### 错误处理示例

**错误：变量未定义**
```
配置变量替换失败：变量 'db_url' 未定义，请通过 --db_url 参数传递
```

**正确：提供参数**
```bash
--db_url jdbc:mysql://localhost:3306/test
```

### 注意事项

1. **变量替换在所有配置源生效**
   - `--file` 参数：文件内容先进行变量替换
   - `--config` 参数：JSON 字符串或 Base64 编码都支持变量替换

2. **JSON 必须是标准格式**
   - 不支持注释（`//` 或 `/* */`）
   - 变量替换后的内容必须能解析为有效 JSON

3. **特殊字符处理**
   - 变量值包含特殊字符（如 URL 参数 `&`）无需转义
   - ParameterTool 自动处理参数值
```

- [ ] **Step 2: 提交文档更新**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md - 新增配置变量替换章节

详细说明：
- 变量格式和默认值支持
- 命令行传参方式
- 错误处理和注意事项

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 7: 运行完整测试验证

**Files:**
- 无文件修改

- [ ] **Step 1: 运行所有单元测试**

运行：`mvn test -pl flink-etl-core`

预期：所有测试通过（包括 CliArgumentParserTest 和 ConfigParserTest）

- [ ] **Step 2: 编译打包项目**

运行：`mvn clean package -DskipTests`

预期：成功生成 `flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar`

- [ ] **Step 3: 手动验证功能（可选）**

创建测试配置文件 `test-var-substitution.json`：
```json
{
  "job": {
    "name": "${job_name:-test}",
    "mode": "batch"
  },
  "sources": [{
    "type": "console",
    "outputTable": "t",
    "config": {}
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "t",
    "config": {}
  }]
}
```

运行：`java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file test-var-substitution.json`

预期：成功启动，Job 名称显示为 "test"

---

### Task 8: 最终提交和清理

**Files:**
- 无文件修改

- [ ] **Step 1: 清理测试文件（如有）**

删除临时测试配置文件（如果创建了）：
```bash
rm test-var-substitution.json
```

- [ ] **Step 2: 检查 git 状态**

运行：`git status`

预期：工作目录干净，所有更改已提交

- [ ] **Step 3: 验证提交历史**

运行：`git log --oneline -10`

预期：看到 8 个新提交（依赖、测试、实现、重构、文档）

---

## 成功标准

✅ 所有单元测试通过
✅ 变量替换功能正常工作（单个/多个/默认值/未定义检测）
✅ ConfigParser.parse(filePath) 方法已删除
✅ 文档已更新（CLAUDE.md 和 PLUGINS.md）
✅ 代码编译成功，无错误
✅ 提交历史清晰，每个任务独立提交

---

## 技术要点

- **StrSubstitutor 默认规则**：`${var}` 和 `${var:-default}`
- **严格模式**：正则检查 `\$\{[^}]+\}` 未替换占位符
- **职责分离**：CliArgumentParser 负责加载+替换，ConfigParser 负责解析
- **TDD 流程**：先写测试，再实现，确保功能正确性