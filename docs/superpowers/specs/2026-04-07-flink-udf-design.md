---
name: Flink UDF 自定义函数功能设计
description: 基于 SPI 机制实现 Flink 自定义函数（UDF）自动加载和注册功能
created_at: 2026-04-07
---

# Flink UDF 自定义函数功能设计

## 一、需求概述

### 1.1 背景

项目当前已实现 Source、Sink、Transform 三种插件的 SPI 加载机制。为了支持更灵活的数据转换逻辑，需要新增 UDF（User Defined Function）自定义函数功能，允许用户在 SQL Transform 中使用自定义的函数进行数据处理。

### 1.2 目标

- 支持 Flink 的四种 UDF 类型：标量函数（ScalarFunction）、表值函数（TableFunction）、聚合函数（AggregateFunction）、表值聚合函数（TableAggregateFunction）
- 使用 SPI 机制自动加载所有 UDF 实现，无需手动配置
- 在 Job 启动时批量注册所有 UDF 到 Flink TableEnvironment
- 先实现简单的标量函数示例，后续逐步支持其他类型
- UDF 注册性能要求：每个 UDF 注册耗时 < 5ms，10 个 UDF 总耗时 < 50ms

### 1.3 设计约束

- UDF 实现必须放在 `flink-etl-core` 模块内部，不支持外部独立模块扩展
- 使用现有的 SPI 插件体系架构，保持与 Source/Sink/Transform 插件的一致性
- 函数名由用户通过 `identifier()` 方法自由定义，避免强制前缀
- 函数名必须唯一，重复注册会导致 Job 启动失败
- 支持 Java 1.8+，与项目技术栈保持一致

---

## 二、设计方案选择

### 2.1 考虑的方案

我们考虑了三种设计方案：

**方案 1：完全遵循现有 Plugin 体系**
- 定义 `UdfPlugin` 接口继承 `Plugin`
- `PluginLoader` 新增批量加载方法
- 在 `JobBuilder` 中批量注册 UDF

**方案 2：独立的 UDF 加载体系**
- 定义轻量级 `UdfFunction` 接口，不继承 `Plugin`
- 新建独立的 `UdfLoader` 类
- 完全解耦 Plugin 和 UDF 体系

**方案 3：混合设计**
- `UdfPlugin` 继承 `Plugin` 但独立加载
- `PluginLoader` 泛化支持批量加载

### 2.2 最终选择

选择 **方案 1：完全遵循现有 Plugin 体系**

**选择理由：**
- Plugin 接口已改名为 `identifier()` 方法，语义更通用，适合 UDF
- 与现有 Source/Sink/Transform 插件架构完全一致，用户理解成本低
- 代码复用度高，修改范围小
- 符合项目统一的 SPI 插件机制风格

---

## 三、架构设计

### 3.1 核心组件

**新增接口：**
- `UdfPlugin` - UDF 插件接口，继承 `Plugin`

**扩展组件：**
- `PluginLoader` - 新增 `loadAllUdfPlugins()` 方法
- `JobBuilder` - 新增 `registerAllUdfs()` 方法

**示例实现：**
- `HashUdf` - 标量函数示例（ScalarFunction）

### 3.2 包结构设计

```
flink-etl-core/
└── src/main/java/com/etl/core/
    ├── spi/                     # SPI 接口层
    │   ├── Plugin.java          # 已有（identifier 方法）
    │   ├── SourcePlugin.java    # 已有
    │   ├── SinkPlugin.java      # 已有
    │   ├── TransformPlugin.java # 已有
    │   ├── UdfPlugin.java       # 新增：UDF 插件接口
    │   └── PluginLoader.java    # 扩展：新增 loadAllUdfPlugins() 方法
    │
    ├── udf/                     # 新增：UDF 功能包
    │   ├── scalar/              # 新增：标量函数目录（ScalarFunction）
    │   │   └── HashUdf.java     # 示例：Hash 函数实现
    │   │
    │   ├── table/               # 新增：表值函数目录（TableFunction）
    │   │   └── .gitkeep         # 暂无实现，保留空文件夹
    │   │
    │   ├── agg/                 # 新增：聚合函数目录（AggregateFunction）
    │   │   └── .gitkeep         # 暂无实现，保留空文件夹
    │   │
    │   └── tagg/                # 新增：表值聚合函数目录（TableAggregateFunction）
    │       └── .gitkeep         # 暂无实现，保留空文件夹
    │
    └── job/
        └── JobBuilder.java      # 扩展：新增 registerAllUdfs() 方法
```

**文件夹命名说明：**

| 文件夹名 | UDF 类型 | Flink 基类 | 说明 |
|---------|----------|-----------|------|
| `scalar` | 标量函数 | `ScalarFunction` | 输入→输出单个值 |
| `table` | 表值函数 | `TableFunction` | 输入→输出多行 |
| `agg` | 聚合函数 | `AggregateFunction` | 多行→单个聚合值 |
| `tagg` | 表值聚合函数 | `TableAggregateFunction` | 多行→多行聚合结果 |

**.gitkeep 文件作用：**
- Git 默认不跟踪空文件夹
- `.gitkeep` 是空文件，Git 会跟踪它
- 确保四个 UDF 类型文件夹都存在于仓库中
- 方便用户直接在对应文件夹下添加新 UDF

---

## 四、接口设计

### 4.1 UdfPlugin 接口

```java
package com.etl.core.spi;

import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * UDF 插件接口
 * 所有自定义函数需要实现此接口，并使用 @AutoService 注解
 */
public interface UdfPlugin extends Plugin {

    /**
     * 创建 UDF 实例
     * 返回 Flink 的 UserDefinedFunction 实例（ScalarFunction、TableFunction 等）
     *
     * @return UDF 实例
     */
    UserDefinedFunction createFunction();
}
```

**设计要点：**
- 继承 `Plugin` 接口，复用 `identifier()` 方法作为函数名
- 新增 `createFunction()` 方法返回实际的 Flink UDF 实例
- 支持所有 Flink UDF 类型（ScalarFunction、TableFunction、AggregateFunction、TableAggregateFunction）

---

### 4.2 PluginLoader 扩展

新增批量加载方法：

```java
/**
 * 批量加载所有 UDF 插件
 *
 * @return 所有 UDF 插件实例列表
 * @throws IllegalStateException 如果 SPI 配置文件加载失败
 */
public static List<UdfPlugin> loadAllUdfPlugins() {
    log.info("批量加载所有 UDF 插件");

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
        classLoader = PluginLoader.class.getClassLoader();
    }

    ServiceLoader<UdfPlugin> loader = ServiceLoader.load(UdfPlugin.class, classLoader);
    List<UdfPlugin> plugins = new ArrayList<>();

    try {
        for (UdfPlugin plugin : loader) {
            String functionName = plugin.identifier();

            // 校验函数名非空
            if (functionName == null || functionName.trim().isEmpty()) {
                log.warn("UDF 插件 {} 的 identifier() 返回空值，跳过加载",
                         plugin.getClass().getName());
                continue;
            }

            log.info("UDF 插件加载成功：{} -> {}",
                     functionName, plugin.getClass().getName());
            plugins.add(plugin);
        }
    } catch (ServiceConfigurationError e) {
        throw new IllegalStateException(
            "SPI 配置文件加载失败，请检查 META-INF/services/com.etl.core.spi.UdfPlugin", e);
    }

    log.info("共加载 {} 个 UDF 插件", plugins.size());
    return plugins;
}
```

**设计要点：**
- 使用 `ServiceLoader` 批量加载所有 `UdfPlugin` 实现
- 记录每个 UDF 的函数名和类名
- 返回列表供批量注册使用
- 校验函数名非空，跳过无效插件
- 捕获 `ServiceConfigurationError` 异常，提供清晰的错误信息

**设计差异说明：**

| 插件类型 | 加载方式 | 原因 |
|---------|---------|------|
| Source/Sink/Transform | 单个加载（通过 type 参数） | 配置文件中指定具体插件类型 |
| UDF | 批量加载所有插件 | SQL 中可能使用多个函数，需一次性注册 |

**为什么 UDF 采用批量加载？**
- 用户在 SQL 中自由使用函数，无法预先知道有哪些函数
- 配置文件中无需声明 UDF，自动加载所有实现
- 与 Flink TableEnvironment 的注册机制一致（批量注册函数）

---

### 4.3 JobBuilder 扩展

在 `JobBuilder.build()` 方法中集成 UDF 注册：

```java
public static void build(StreamExecutionEnvironment env, JobConfig config) {
    log.info("开始构建 Flink Job: {}", config.getJob().getName());

    // 创建 Table 环境
    StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

    // 批量注册所有 UDF
    registerAllUdfs(stEnv);

    // 1. 处理所有 Source -> DataStream
    // ... 原有逻辑不变

    // 2. Transform 链式处理
    // ... 原有逻辑不变

    // 3. 处理所有 Sink
    // ... 原有逻辑不变

    log.info("Flink Job 构建完成");
}

/**
 * 批量注册所有 UDF 到 TableEnvironment
 *
 * @param stEnv Table 环境
 * @throws IllegalStateException 如果 UDF 注册失败或函数名冲突
 */
private static void registerAllUdfs(StreamTableEnvironment stEnv) {
    List<UdfPlugin> udfPlugins = PluginLoader.loadAllUdfPlugins();

    Set<String> registeredFunctions = new HashSet<>();

    for (UdfPlugin udf : udfPlugins) {
        String functionName = udf.identifier();

        // 校验函数名唯一性
        if (registeredFunctions.contains(functionName)) {
            throw new IllegalStateException(
                String.format("函数名冲突：'%s' 已被注册，请检查 UDF 插件的 identifier() 方法",
                              functionName)
            );
        }

        // 创建 UDF 实例
        UserDefinedFunction functionInstance = udf.createFunction();
        if (functionInstance == null) {
            throw new IllegalStateException(
                String.format("UDF 插件 '%s' 的 createFunction() 返回 null",
                              udf.getClass().getName())
            );
        }

        // 注册函数
        try {
            stEnv.createTemporaryFunction(functionName, functionInstance);
            registeredFunctions.add(functionName);
            log.info("UDF 注册成功：{} -> {}",
                     functionName, functionInstance.getClass().getSimpleName());
        } catch (Exception e) {
            throw new IllegalStateException(
                String.format("UDF 注册失败：%s", functionName), e
            );
        }
    }

    if (udfPlugins.isEmpty()) {
        log.info("未发现任何 UDF 插件");
    } else {
        log.info("成功注册 {} 个 UDF 函数", udfPlugins.size());
    }
}
```

**设计要点：**
- 在创建 `StreamTableEnvironment` 后立即注册 UDF
- 确保后续 SQL Transform 可以使用这些函数
- 使用 `createTemporaryFunction()` 注册函数实例
- 日志记录每个注册的函数名，便于调试
- 校验函数名唯一性，避免冲突
- 校验 createFunction() 返回值非空
- 捕获注册异常，提供清晰的错误信息

---

## 五、实现示例

### 5.1 标量函数示例（ScalarFunction）

```java
package com.etl.core.udf.scalar;

import com.etl.core.spi.UdfPlugin;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * Hash 函数示例
 * 返回输入值的哈希码
 */
@AutoService(UdfPlugin.class)
public class HashUdf implements UdfPlugin {

    @Override
    public String identifier() {
        return "hash_code";
    }

    @Override
    public UserDefinedFunction createFunction() {
        return new HashFunction();
    }

    /**
     * 实际的 Flink ScalarFunction 实现
     */
    public static class HashFunction extends ScalarFunction {

        /**
         * 计算输入对象的哈希码
         *
         * @param input 输入对象，可以为 null
         * @return 哈希码，null 输入返回 0
         */
        public int eval(Object input) {
            if (input == null) {
                return 0;
            }
            return input.hashCode();
        }

        @Override
        public String toString() {
            return "hash_code()";
        }
    }
}
```

**说明：**
- 外部类实现 `UdfPlugin` 接口
- 内部静态类继承 `ScalarFunction`，实现实际函数逻辑
- 使用 `@AutoService` 注解自动生成 SPI 配置文件
- `identifier()` 返回 "hash_code"，在 SQL 中使用：`SELECT hash_code(field)`

---

## 六、使用流程

### 6.1 用户添加新 UDF 的步骤

**步骤 1：在对应的文件夹下创建新 UDF 类**

- 标量函数 → `flink-etl-core/src/main/java/com/etl/core/udf/scalar/`
- 表值函数 → `flink-etl-core/src/main/java/com/etl/core/udf/table/`
- 聚合函数 → `flink-etl-core/src/main/java/com/etl/core/udf/agg/`
- 表值聚合函数 → `flink-etl-core/src/main/java/com/etl/core/udf/tagg/`

**示例：添加字符串处理函数**

```java
package com.etl.core.udf.scalar;

import com.etl.core.spi.UdfPlugin;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(UdfPlugin.class)
public class MySubstringUdf implements UdfPlugin {

    @Override
    public String identifier() {
        return "substring_custom";
    }

    @Override
    public UserDefinedFunction createFunction() {
        return new SubstringFunction();
    }

    public static class SubstringFunction extends ScalarFunction {

        public String eval(String str, Integer begin, Integer end) {
            if (str == null) return null;
            return str.substring(begin, end);
        }
    }
}
```

**步骤 2：编译项目（生成 SPI 配置文件）**

```bash
mvn clean install -DskipTests
```

编译时 `@AutoService` 注解处理器会自动生成：
```
flink-etl-core/target/classes/META-INF/services/com.etl.core.spi.UdfPlugin
```

内容示例：
```
com.etl.core.udf.scalar.HashUdf
com.etl.core.udf.scalar.MySubstringUdf
```

**步骤 3：在 SQL Transform 中使用**

```json
{
  "transforms": [{
    "type": "sql",
    "outputTable": "result_table",
    "config": {
      "sql": "SELECT substring_custom(name, 0, 10) AS short_name, hash_code(id) AS id_hash FROM source_table"
    }
  }]
}
```

**步骤 4：运行 Job**

```bash
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file my-job.json
```

程序启动时自动：
1. 通过 SPI 加载所有 `UdfPlugin` 实现
2. 批量注册到 `StreamTableEnvironment`
3. SQL 中可以直接使用 `substring_custom` 和 `hash_code` 函数

---

## 七、文件清单

### 7.1 新增文件

| 文件路径 | 说明 |
|---------|------|
| `flink-etl-core/src/main/java/com/etl/core/spi/UdfPlugin.java` | UDF 插件接口定义 |
| `flink-etl-core/src/main/java/com/etl/core/udf/scalar/HashUdf.java` | 标量函数示例实现 |
| `flink-etl-core/src/main/java/com/etl/core/udf/table/.gitkeep` | 表值函数目录占位文件 |
| `flink-etl-core/src/main/java/com/etl/core/udf/agg/.gitkeep` | 聚合函数目录占位文件 |
| `flink-etl-core/src/main/java/com/etl/core/udf/tagg/.gitkeep` | 表值聚合函数目录占位文件 |

### 7.2 修改文件

| 文件路径 | 修改内容 |
|---------|---------|
| `flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java` | 新增 `loadAllUdfPlugins()` 方法 |
| `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java` | 新增 `registerAllUdfs()` 方法，并在 `build()` 中调用 |

---

## 八、依赖说明

### 8.1 Maven 依赖

UDF 功能依赖以下库（已包含在项目中）：

```xml
<!-- Flink Table API -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-api-java-bridge</artifactId>
    <version>1.15.2</version>
</dependency>

<!-- AutoService 注解处理器 -->
<dependency>
    <groupId>com.google.auto.service</groupId>
    <artifactId>auto-service</artifactId>
    <version>1.0.1</version>
    <scope>provided</scope>
</dependency>
```

---

## 九、后续扩展

### 9.1 表值函数（TableFunction）扩展计划

未来可在 `flink-etl-core/src/main/java/com/etl/core/udf/table/` 下添加表值函数实现。

**示例：字符串分隔函数**

```java
package com.etl.core.udf.table;

import com.etl.core.spi.UdfPlugin;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@AutoService(UdfPlugin.class)
public class SplitUdf implements UdfPlugin {

    @Override
    public String identifier() {
        return "split";
    }

    @Override
    public UserDefinedFunction createFunction() {
        return new SplitFunction();
    }

    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str, String delimiter) {
            if (str == null) return;
            for (String s : str.split(delimiter)) {
                collect(Row.of(s));
            }
        }
    }
}
```

**SQL 使用：**
```sql
SELECT t.word
FROM source_table, LATERAL TABLE(split(content, ',')) AS t(word)
```

### 9.2 聚合函数（AggregateFunction）扩展计划

未来可在 `flink-etl-core/src/main/java/com/etl/core/udf/agg/` 下添加聚合函数实现。

参考 Flink 文档中的 `WeightedAvg` 示例实现。

---

## 十、测试策略

### 10.1 单元测试

**测试类：** `UdfPluginTest.java`

**测试场景：**
1. `identifier()` 方法返回值校验（空值、非法字符）
2. `createFunction()` 方法返回 null 校验
3. HashUdf 示例函数的正确性测试
4. 函数名冲突检测

**测试示例：**
```java
package com.etl.core.udf;

import com.etl.core.spi.UdfPlugin;
import com.etl.core.udf.scalar.HashUdf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UdfPluginTest {

    @Test
    void testHashUdfIdentifier() {
        HashUdf hashUdf = new HashUdf();
        assertEquals("hash_code", hashUdf.identifier());
    }

    @Test
    void testHashUdfCreateFunction() {
        HashUdf hashUdf = new HashUdf();
        assertNotNull(hashUdf.createFunction());
    }

    @Test
    void testHashFunctionEval() {
        HashUdf.HashFunction function = new HashUdf.HashFunction();

        // 测试 null 输入
        assertEquals(0, function.eval(null));

        // 测试正常输入
        assertEquals("hello".hashCode(), function.eval("hello"));
        assertEquals(Integer.valueOf(123).hashCode(), function.eval(123));
    }
}
```

### 10.2 集成测试

**测试类：** `JobBuilderUdfTest.java`

**测试场景：**
1. UDF 注册成功后的 SQL 查询测试
2. SQL Transform 中使用自定义函数
3. 函数名冲突异常测试

**测试示例：**
```java
package com.etl.core.job;

import com.etl.core.config.JobConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JobBuilderUdfTest {

    @Test
    void testUdfRegistration() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

        // 验证 UDF 注册
        assertDoesNotThrow(() -> {
            // 使用反射调用 private 方法
            Method method = JobBuilder.class.getDeclaredMethod("registerAllUdfs", StreamTableEnvironment.class);
            method.setAccessible(true);
            method.invoke(null, stEnv);
        });
    }

    @Test
    void testHashFunctionInSql() {
        // 创建测试数据
        // 注册 HashUdf
        // 执行 SQL: SELECT hash_code(name) FROM source_table
        // 验证结果
    }
}
```

### 10.3 验证步骤

**手动验证：**
1. 编译项目：`mvn clean install -DskipTests`
2. 检查 SPI 配置文件：`target/classes/META-INF/services/com.etl.core.spi.UdfPlugin`
3. 运行示例 Job：使用包含 `hash_code()` 函数的 SQL
4. 检查日志：确认 UDF 注册成功

---

## 十一、风险与限制

### 11.1 潜在风险

**风险 1：函数名冲突**
- **现象**：两个 UDF 插件的 identifier() 返回相同函数名
- **影响**：Job 启动失败，抛出 IllegalStateException
- **缓解措施**：代码审查时检查函数名唯一性；日志记录重复函数名

**风险 2：SPI 配置文件缺失**
- **现象**：编译后未生成 META-INF/services 文件
- **原因**：未使用 @AutoService 注解或编译失败
- **缓解措施**：编译后检查 target/classes 目录

**风险 3：UDF 实现错误**
- **现象**：createFunction() 返回 null 或异常对象
- **影响**：函数注册失败
- **缓解措施**：单元测试覆盖 UDF 实现

### 11.2 功能限制

**限制 1：不支持外部模块扩展**
- UDF 必须放在 flink-etl-core 模块内部
- **原因**：简化设计，避免复杂的类加载器隔离问题
- **未来扩展**：可考虑支持外部 JAR 包加载（参考 SeaTunnel 插件机制）

**限制 2：不支持动态加载**
- UDF 在 Job 启动时一次性加载
- **影响**：无法在运行时动态添加新函数
- **缓解措施**：重启 Job 加载新 UDF

**限制 3：当前仅实现标量函数示例**
- TableFunction、AggregateFunction、TableAggregateFunction 待实现
- **原因**：优先实现最常用的 ScalarFunction，后续逐步扩展

### 11.3 性能影响

**UDF 注册耗时：**
- 预期耗时：每个 UDF < 5ms
- 批量注册：10 个 UDF < 50ms
- **影响**：Job 启动延迟轻微增加

**UDF 执行性能：**
- 与 Flink 内置函数性能一致
- 用户实现质量决定实际性能

### 11.4 回滚计划

**如果 UDF 功能引入问题：**
1. **短期回滚**：注释 JobBuilder.registerAllUdfs() 调用
2. **中期回滚**：删除 UdfPlugin 接口和相关代码
3. **数据影响**：无影响（UDF 功能仅影响转换逻辑，不涉及数据存储）

---

## 十二、文档维护

### 12.1 需要更新的文档

**PLUGINS.md 更新：**
- 新增 "UDF 插件" 章节
- 记录内置 UDF 函数列表（如 `hash_code`）
- 说明如何在 SQL Transform 中使用 UDF

**更新位置：** 在 "Transform 插件" 章节后新增：

```markdown
## UDF 插件

### 内置 UDF 函数

项目提供以下内置 UDF 函数，可在 SQL Transform 中直接使用：

#### 标量函数（ScalarFunction）

| 函数名 | 说明 | 示例 |
|--------|------|------|
| `hash_code` | 返回输入值的哈希码 | `SELECT hash_code(name) FROM users` |

**使用示例：**
```json
{
  "transforms": [{
    "type": "sql",
    "outputTable": "result_table",
    "config": {
      "sql": "SELECT hash_code(id) AS id_hash, name FROM source_table"
    }
  }]
}
```

### 扩展新 UDF

参考设计文档：`docs/superpowers/specs/2026-04-07-flink-udf-design.md`
```

### 12.2 示例配置文件

在 `docs/examples/` 目录下新增示例：
- `udf-demo.json` - 包含 hash_code 函数的示例配置

---

## 十三、总结

本设计基于现有的 SPI 插件体系，通过定义 `UdfPlugin` 接口和扩展 `PluginLoader`、`JobBuilder`，实现了 Flink UDF 的自动加载和注册功能。

**核心特性：**
- 完全遵循现有 Plugin 体系架构
- 支持 Flink 四种 UDF 类型
- 自动加载所有 UDF，无需手动配置
- 用户在对应文件夹下实现 UDF 即可使用
- 完善的异常处理和边界情况校验

**实现优先级：**
- 第一阶段：实现标量函数（ScalarFunction）示例
- 第二阶段：扩展支持表值函数（TableFunction）
- 第三阶段：扩展支持聚合函数（AggregateFunction）和表值聚合函数（TableAggregateFunction）