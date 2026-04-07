# 配置文件变量替换功能设计

## 概述

为 ETL 工具添加配置变量替换功能，支持在配置文件中使用 `${variable}` 占位符，通过命令行参数传递变量值。这使得用户可以根据环境动态配置参数（如数据库连接信息、文件路径等），无需修改配置文件本身。

## 背景

### 当前问题

现有配置加载流程不支持动态参数化：
- 配置文件中的连接信息、文件路径等参数硬编码
- 不同环境（开发/测试/生产）需要维护多份配置文件
- 敏感信息（密码等）暴露在配置文件中，不安全

### 解决方案

引入变量替换机制：
- 配置文件中使用 `${variable}` 占位符
- 运行时通过命令行参数 `--variable value` 传递
- 加载配置时自动替换，支持默认值 `${variable:-default}`

## 需求明确

### 功能需求

1. **配置源支持**：file、base64、json string 三种配置源均支持变量替换
2. **变量格式**：使用 StrSubstitutor 默认规则
   - `${variable}` - 变量不存在时保留占位符
   - `${variable:-default}` - 变量不存在时使用默认值
3. **变量来源**：从命令行参数传递（通过 ParameterTool.getProperties() 获取）
4. **严格模式**：检测未替换的 `${...}` 占位符，发现则抛异常明确提示

### 设计约束

- **职责分离**：变量替换在 CliArgumentParser 中完成，ConfigParser 只处理 JSON string
- **废弃方法**：删除 `ConfigParser.parse(String configPath)` 方法
- **依赖管理**：新增依赖 Apache Commons Text（StrSubstitutor），版本 1.10.0

## 设计方案

### 架构设计

**改动范围：**
- `CliArgumentParser.java` - 新增变量替换逻辑
- `ConfigParser.java` - 删除 `parse(String configPath)` 方法

**执行流程：**

```
CliArgumentParser.parse(args)
  ↓
ParameterTool.fromArgs(args)  // 获取所有参数（包括 --file/--config 和变量参数）
  ↓
加载配置源（file/base64/json string）
  ↓
substituteVariables(jsonString, ParameterTool.getProperties())
  ↓
checkUnresolvedVariables(jsonString)  // 严格检查
  ↓
ConfigParser.parseFromString(jsonString)  // 解析和校验 JSON
```

### 详细设计

**导入依赖：**
```java
import org.apache.commons.text.StrSubstitutor;  // 变量替换
import java.util.regex.Pattern;  // 正则检查未替换占位符
import java.util.regex.Matcher;
```

#### 1. substituteVariables 方法

**职责**：使用 StrSubstitutor 替换配置中的变量占位符

**实现**：
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

#### 2. checkUnresolvedVariables 方法

**职责**：严格检查未替换的 `${...}` 占位符

**实现**：
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
        throw new IllegalArgumentException(
            String.format("配置变量替换失败：变量 '%s' 未定义，请通过 --%s 参数传递",
                varName, varName.split(":-")[0])
        );
    }
}
```

#### 3. readFileContent 方法

**职责**：读取文件内容为字符串（为变量替换做准备）

**实现**：
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

#### 4. 主流程修改

**loadFromFile 方法**：
```java
/**
 * 从文件加载配置为 JSON 字符串
 *
 * @param filePath 配置文件路径
 * @return JSON 字符串
 */
private static String loadFromFile(String filePath) {
    // 文件存在性校验（保持不变）

    return readFileContent(filePath);
}
```

**loadFromJsonString 方法**：
```java
/**
 * 从命令行参数加载配置为 JSON 字符串
 * 支持 JSON 字符串或 Base64 编码
 *
 * @param input JSON 字符串或 Base64 编码
 * @return JSON 字符串
 */
private static String loadFromJsonString(String input) {
    return tryDecodeBase64(input);  // 保持不变
}
```

**parse 方法修改**：
```java
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

#### 5. ConfigParser 废弃方法删除

删除 `ConfigParser.parse(String configPath)` 方法及其相关文档。

### 错误处理策略

| 场景 | 处理方式 | 错误信息示例 |
|------|---------|-------------|
| 文件不存在 | 抛出 IllegalArgumentException | "配置文件不存在: /path/to/file.json" |
| Base64 解码失败 | 返回原字符串，当作 JSON 直接处理 | 不抛异常，后续解析失败由 ConfigParser 处理 |
| 变量未定义（无默认值） | 抛出 IllegalArgumentException | "配置变量替换失败：变量 'db_url' 未定义，请通过 --db_url 参数传递" |
| JSON 格式错误 | 抛出 IllegalArgumentException | "配置解析失败: ..." |

### 边界情况处理

#### 1. 变量参数为空值
```bash
--db_url ""
```
- ParameterTool 会将 `db_url` 设置为空字符串
- StrSubstitutor 替换为 `""`
- 检查通过，由后续校验处理

#### 2. 变量参数值包含特殊字符
```bash
--db_url "jdbc:mysql://localhost:3306/test?user=root&password=123"
```
- ParameterTool 正确处理，无需转义
- StrSubstitutor 直接替换

#### 3. 配置中变量名拼写错误
```json
"url": "${dburl}"  // 用户传了 --db_url
```
- 变量未定义，严格模式检测到 `${dburl}`
- 抛出异常："变量 'dburl' 未定义，请通过 --dburl 参数传递"

#### 4. 嵌套变量
```json
"url": "${host}:${port}"
```
- StrSubstitutor 支持多次替换
- 需要传递 `--host localhost --port 3306`

#### 5. JSON 中包含非变量的 `${` 符号
```json
"query": "SELECT * FROM users WHERE name LIKE '${prefix}%'"
```
- 如果 `prefix` 未定义，严格模式会检测并报错
- **解决方案**：用户必须传递 `--prefix` 或使用默认值 `${prefix:-}`

#### 6. 配置文件中包含注释（非标准 JSON）
- JSON 标准不支持注释
- 如果文件包含 `//` 或 `/* */`，ConfigParser 解析时会失败
- **建议**：在文档中说明配置必须是标准 JSON

## 使用示例

### 示例配置文件

**config/mysql-to-console.json**：
```json
{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch"
  },
  "sources": [{
    "type": "jdbc",
    "outputTable": "source_table",
    "config": {
      "url": "${db_url}",
      "username": "${db_user:-root}",
      "password": "${db_password}",
      "table": "users",
      "columns": ["id", "name", "email"]
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "source_table"
  }]
}
```

### 运行命令

**传递必需参数**：
```bash
java -jar flink-etl-client.jar \
  --file config/mysql-to-console.json \
  --db_url "jdbc:mysql://localhost:3306/test" \
  --db_password "secret123"
```
- `db_url` 必需，未传递会报错
- `db_user` 有默认值 `root`，未传递时使用默认值
- `db_password` 必需，未传递会报错

**覆盖默认值**：
```bash
java -jar flink-etl-client.jar \
  --file config/mysql-to-console.json \
  --db_url "jdbc:mysql://prod-server:3306/prod_db" \
  --db_user "prod_user" \
  --db_password "prod_secret"
```

### Base64 配置示例

**编码配置**：
```bash
# 原始配置
config='{"job":{"name":"test","mode":"batch"},"sources":[...],"sinks":[...]}'

# Base64 编码
encoded=$(echo -n "$config" | base64)

# 运行
java -jar flink-etl-client.jar \
  --config "$encoded" \
  --db_url "jdbc:mysql://localhost:3306/test" \
  --db_password "secret123"
```

## 测试策略

### 单元测试

**CliArgumentParserTest 新增测试用例**：

1. **变量替换基础功能**：
   - 测试单个变量替换
   - 测试多个变量替换
   - 测试变量带默认值
   - 测试变量未定义抛异常

2. **边界情况**：
   - 变量值为空字符串
   - 变量值包含特殊字符（URL、密码等）
   - 配置中多个变量
   - 变量名拼写错误检测

3. **配置源兼容性**：
   - `--file` 参数支持变量替换
   - `--config` JSON string 支持变量替换
   - `--config` Base64 支持变量替换

**ConfigParser 废弃方法测试删除**：
- 删除 `parse(String configPath)` 相关测试

### 集成测试

使用实际配置文件测试完整流程：
- 创建包含变量的测试配置文件
- 通过命令行传递参数
- 验证 Job 执行正确性

## 文档更新

### CLAUDE.md 更新

在 "常用命令" 部分，补充变量参数使用示例：
```bash
# 运行带变量替换的 ETL 任务
java -jar flink-etl-client.jar \
  --file config/mysql-to-console.json \
  --db_url jdbc:mysql://localhost:3306/test \
  --db_user root \
  --db_password secret
```

在 "配置文件格式" 部分，补充变量替换说明：
```
配置支持变量替换：
- 格式：${variable} 或 ${variable:-default}
- 变量值通过命令行参数传递（例如：--db_url xxx）
- 未定义变量（无默认值）会抛异常提示
```

### PLUGINS.md 更新

在 "配置文件格式" 部分补充变量替换章节，包含：
- 变量格式说明
- 使用示例
- 错误处理说明

### README 或用户文档更新

创建独立的用户文档说明变量替换功能使用方法。

## 依赖管理

需要添加 Apache Commons Text 库依赖（StrSubstitutor 所在包）。

**版本管理：**

在父 `pom.xml` 的 `<properties>` 中添加版本号：
```xml
<commons-text.version>1.10.0</commons-text.version>
```

在父 `pom.xml` 的 `<dependencyManagement>` 中声明：
```xml
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-text</artifactId>
    <version>${commons-text.version}</version>
</dependency>
```

**添加依赖到 flink-etl-core/pom.xml：**
```xml
<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-text</artifactId>
</dependency>
```

**导入语句：**
```java
import org.apache.commons.text.StrSubstitutor;
```

**注意：** StrSubstitutor 在 commons-lang3 早期版本中存在，但在 3.x 版本后已迁移到 commons-text 包。使用 commons-text 是当前最佳实践。

## 实现步骤

1. 添加依赖到 flink-etl-core/pom.xml：
   - 添加 commons-text 依赖（版本 1.10.0）

2. 修改 `CliArgumentParser.java`：
   - 添加导入语句：`import org.apache.commons.text.StrSubstitutor;`
   - 新增 `substituteVariables` 方法
   - 新增 `checkUnresolvedVariables` 方法
   - 新增 `readFileContent` 方法
   - 修改 `loadFromFile` 方法：返回 JSON String，移除 ParameterTool 参数
   - 修改 `loadFromJsonString` 方法：返回 JSON String，移除 ParameterTool 参数
   - 修改 `parse` 方法：统一进行变量替换和检查

3. 修改 `ConfigParser.java`：
   - 删除 `parse(String configPath)` 方法
   - 保留 `parseFromString` 方法不变

4. 更新文档：
   - CLAUDE.md
   - PLUGINS.md

5. 新增单元测试：
   - CliArgumentParserTest 变量替换相关测试

6. 运行测试验证：
   - 单元测试
   - 集成测试

## 成功标准

1. 功能完整性：
   - 三种配置源均支持变量替换
   - 支持默认值 `${variable:-default}`
   - 严格模式检测未定义变量

2. 错误处理清晰：
   - 变量未定义时错误信息明确
   - 指导用户如何传递参数

3. 测试覆盖完整：
   - 单元测试覆盖核心逻辑
   - 集成测试验证实际场景

4. 文档完整：
   - 用户文档清晰说明使用方法
   - 开发文档说明设计原理

5. 向后兼容：
   - 不影响现有配置文件使用（无变量时正常解析）
   - 删除废弃方法不影响调用方（parseFromFile 内部改为使用 parseFromString）