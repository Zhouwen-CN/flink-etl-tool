# Flink UDF 自定义函数功能实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现基于 SPI 的 Flink UDF 自动加载和注册功能，支持用户在 SQL Transform 中使用自定义函数。

**Architecture:** 采用 SPI 插件机制，定义 `UdfPlugin` 接口继承 `Plugin`，扩展 `PluginLoader` 和 `JobBuilder` 实现 UDF 的批量加载和自动注册。先实现标量函数示例（HashUdf），预留其他类型 UDF 的扩展目录。

**Tech Stack:** Java 1.8, Flink 1.15.2, Flink Table API, Google AutoService

---

## 文件结构

### 新增文件

```
flink-etl-core/src/main/java/com/etl/core/
├── spi/
│   └── UdfPlugin.java                    # UDF 插件接口
└── udf/
    ├── scalar/
    │   └── HashUdf.java                  # 标量函数示例
    ├── table/
    │   └── .gitkeep                      # 表值函数目录占位
    ├── agg/
    │   └── .gitkeep                      # 聚合函数目录占位
    └── tagg/
        └── .gitkeep                      # 表值聚合函数目录占位

flink-etl-core/src/test/java/com/etl/core/
└── udf/
    └── UdfPluginTest.java                # 单元测试
```

### 修改文件

```
flink-etl-core/src/main/java/com/etl/core/
├── spi/
│   └── PluginLoader.java                 # 新增 loadAllUdfPlugins() 方法
└── job/
    └── JobBuilder.java                   # 新增 registerAllUdfs() 方法
```

---

## 任务分解

### Task 1: 创建 UdfPlugin 接口

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/spi/UdfPlugin.java`

**目标:** 定义 UDF 插件接口，继承 Plugin 接口，提供 `createFunction()` 方法。

- [ ] **Step 1: 创建 UdfPlugin 接口**

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
     * @return UDF 实例，不能为 null
     */
    UserDefinedFunction createFunction();
}
```

- [ ] **Step 2: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交代码**

```bash
git add flink-etl-core/src/main/java/com/etl/core/spi/UdfPlugin.java
git commit -m "feat: 新增 UdfPlugin 接口定义

- UdfPlugin 继承 Plugin 接口
- 新增 createFunction() 方法用于创建 UDF 实例
- 支持所有 Flink UDF 类型（ScalarFunction、TableFunction 等）"
```

---

### Task 2: 创建 UDF 目录结构

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/udf/scalar/.gitkeep`
- Create: `flink-etl-core/src/main/java/com/etl/core/udf/table/.gitkeep`
- Create: `flink-etl-core/src/main/java/com/etl/core/udf/agg/.gitkeep`
- Create: `flink-etl-core/src/main/java/com/etl/core/udf/tagg/.gitkeep`

**目标:** 创建 UDF 功能包的目录结构，按类型分类存放 UDF 实现。

- [ ] **Step 1: 创建 UDF 目录和 .gitkeep 文件**

```bash
mkdir -p flink-etl-core/src/main/java/com/etl/core/udf/scalar
mkdir -p flink-etl-core/src/main/java/com/etl/core/udf/table
mkdir -p flink-etl-core/src/main/java/com/etl/core/udf/agg
mkdir -p flink-etl-core/src/main/java/com/etl/core/udf/tagg

# 创建空的 .gitkeep 文件
touch flink-etl-core/src/main/java/com/etl/core/udf/scalar/.gitkeep
touch flink-etl-core/src/main/java/com/etl/core/udf/table/.gitkeep
touch flink-etl-core/src/main/java/com/etl/core/udf/agg/.gitkeep
touch flink-etl-core/src/main/java/com/etl/core/udf/tagg/.gitkeep
```

- [ ] **Step 2: 提交目录结构**

```bash
git add flink-etl-core/src/main/java/com/etl/core/udf/
git commit -m "feat: 创建 UDF 目录结构

- scalar/: 标量函数目录（ScalarFunction）
- table/: 表值函数目录（TableFunction）
- agg/: 聚合函数目录（AggregateFunction）
- tagg/: 表值聚合函数目录（TableAggregateFunction）
- 使用 .gitkeep 保留空文件夹"
```

---

### Task 3: 实现 HashUdf 标量函数示例

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/udf/scalar/HashUdf.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/udf/UdfPluginTest.java`

**目标:** 实现一个简单的标量函数示例（hash_code），返回输入值的哈希码。

- [ ] **Step 1: 编写 HashUdf 单元测试**

```java
package com.etl.core.udf;

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

- [ ] **Step 2: 运行测试确认失败**

Run: `cd d:/work/idea/flink-etl-tool && mvn test -Dtest=UdfPluginTest -pl flink-etl-core`
Expected: FAIL - HashUdf class not found

- [ ] **Step 3: 实现 HashUdf**

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

- [ ] **Step 4: 运行测试确认通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn test -Dtest=UdfPluginTest -pl flink-etl-core`
Expected: BUILD SUCCESS, all tests pass

- [ ] **Step 5: 提交代码**

```bash
git add flink-etl-core/src/main/java/com/etl/core/udf/scalar/HashUdf.java
git add flink-etl-core/src/test/java/com/etl/core/udf/UdfPluginTest.java
git commit -m "feat: 实现 HashUdf 标量函数示例

- 返回输入值的哈希码，null 输入返回 0
- 使用 @AutoService 注解自动生成 SPI 配置
- 提供完整的单元测试覆盖"
```

---

### Task 4: 扩展 PluginLoader 支持批量加载 UDF

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/spi/PluginLoaderUdfTest.java`

**目标:** 在 PluginLoader 中新增 `loadAllUdfPlugins()` 方法，批量加载所有 UDF 插件。

- [ ] **Step 1: 编写 PluginLoader UDF 加载测试**

```java
package com.etl.core.spi;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PluginLoaderUdfTest {

    @Test
    void testLoadAllUdfPlugins() {
        List<UdfPlugin> plugins = PluginLoader.loadAllUdfPlugins();
        
        // 验证加载成功
        assertNotNull(plugins);
        
        // 验证至少包含 HashUdf
        assertTrue(plugins.stream().anyMatch(p -> "hash_code".equals(p.identifier())));
    }

    @Test
    void testLoadAllUdfPluginsNotEmpty() {
        List<UdfPlugin> plugins = PluginLoader.loadAllUdfPlugins();
        
        // 验证列表不为空
        assertFalse(plugins.isEmpty());
        
        // 验证每个插件的 identifier 非空
        for (UdfPlugin plugin : plugins) {
            assertNotNull(plugin.identifier());
            assertFalse(plugin.identifier().trim().isEmpty());
        }
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd d:/work/idea/flink-etl-tool && mvn test -Dtest=PluginLoaderUdfTest -pl flink-etl-core`
Expected: FAIL - method not found

- [ ] **Step 3: 在 PluginLoader 中实现 loadAllUdfPlugins() 方法**

在 `PluginLoader.java` 中添加以下方法：

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

需要添加的 import:
```java
import java.util.ArrayList;
import java.util.ServiceConfigurationError;
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn test -Dtest=PluginLoaderUdfTest -pl flink-etl-core`
Expected: BUILD SUCCESS, all tests pass

- [ ] **Step 5: 提交代码**

```bash
git add flink-etl-core/src/main/java/com/etl/core/spi/PluginLoader.java
git add flink-etl-core/src/test/java/com/etl/core/spi/PluginLoaderUdfTest.java
git commit -m "feat: PluginLoader 新增批量加载 UDF 功能

- 新增 loadAllUdfPlugins() 方法批量加载所有 UDF
- 校验函数名非空，跳过无效插件
- 捕获 ServiceConfigurationError 提供清晰错误信息
- 提供完整的单元测试"
```

---

### Task 5: 扩展 JobBuilder 集成 UDF 注册

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java`

**目标:** 在 JobBuilder 中新增 `registerAllUdfs()` 方法，在 Job 构建时批量注册所有 UDF。

- [ ] **Step 1: 在 JobBuilder.build() 中集成 UDF 注册**

修改 `JobBuilder.java` 的 `build()` 方法：

```java
public static void build(StreamExecutionEnvironment env, JobConfig config) {
    log.info("开始构建 Flink Job: {}", config.getJob().getName());

    // 创建 Table 环境
    StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

    // 批量注册所有 UDF
    registerAllUdfs(stEnv);

    // 1. 处理所有 Source -> DataStream
    for (SourceConfig sourceConfig : config.getSources()) {
        // ... 原有逻辑不变
    }

    // 2. Transform 链式处理
    // ... 原有逻辑不变

    // 3. 处理所有 Sink
    // ... 原有逻辑不变

    log.info("Flink Job 构建完成");
}
```

- [ ] **Step 2: 实现 registerAllUdfs() 方法**

在 `JobBuilder.java` 中添加以下方法：

```java
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

需要添加的 import:
```java
import com.etl.core.spi.UdfPlugin;
import org.apache.flink.table.functions.UserDefinedFunction;
import java.util.HashSet;
import java.util.Set;
```

- [ ] **Step 3: 验证编译通过**

Run: `cd d:/work/idea/flink-etl-tool && mvn clean compile -pl flink-etl-core`
Expected: BUILD SUCCESS

- [ ] **Step 4: 提交代码**

```bash
git add flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java
git commit -m "feat: JobBuilder 集成 UDF 自动注册功能

- 新增 registerAllUdfs() 方法批量注册所有 UDF
- 在 build() 方法中调用 UDF 注册
- 校验函数名唯一性，避免冲突
- 校验 createFunction() 返回值非空
- 捕获注册异常提供清晰错误信息"
```

---

### Task 6: 编译验证和 SPI 配置检查

**Files:**
- Check: `flink-etl-core/target/classes/META-INF/services/com.etl.core.spi.UdfPlugin`

**目标:** 编译项目，验证 SPI 配置文件是否正确生成。

- [ ] **Step 1: 编译项目**

Run: `cd d:/work/idea/flink-etl-tool && mvn clean install -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 2: 检查 SPI 配置文件**

Run: `cat flink-etl-core/target/classes/META-INF/services/com.etl.core.spi.UdfPlugin`
Expected: 
```
com.etl.core.udf.scalar.HashUdf
```

- [ ] **Step 3: 提交最终验证**

```bash
git add -A
git commit -m "chore: 编译验证 UDF 功能完成

- SPI 配置文件正确生成
- HashUdf 成功注册
- 所有单元测试通过"
```

---

### Task 7: 功能测试和文档更新

**目标:** 运行所有测试，确保功能完整且文档已更新。

- [ ] **Step 1: 运行所有单元测试**

Run: `cd d:/work/idea/flink-etl-tool && mvn test -pl flink-etl-core`
Expected: BUILD SUCCESS, all tests pass

- [ ] **Step 2: 验证文档已更新**

检查 `PLUGINS.md` 是否包含 UDF 插件章节。

Run: `grep -n "UDF 插件" PLUGINS.md`
Expected: 找到 UDF 插件章节

- [ ] **Step 3: 最终提交**

```bash
git add -A
git commit -m "feat: Flink UDF 自定义函数功能实现完成

核心特性：
- UdfPlugin 接口继承 Plugin，支持所有 UDF 类型
- PluginLoader 批量加载所有 UDF
- JobBuilder 自动注册 UDF 到 TableEnvironment
- HashUdf 标量函数示例实现
- 完整的异常处理和边界情况校验
- 完整的单元测试覆盖

测试：
- UdfPluginTest: 测试 HashUdf 功能
- PluginLoaderUdfTest: 测试 UDF 加载逻辑

文档：
- 设计文档: docs/superpowers/specs/2026-04-07-flink-udf-design.md
- 用户文档: PLUGINS.md 已更新 UDF 章节"
```

---

## 验收标准

实现完成后，应满足以下标准：

- [ ] 所有新增文件已创建，代码符合设计规范
- [ ] 所有修改文件已更新，功能正确实现
- [ ] 所有单元测试通过（测试覆盖率 > 80%）
- [ ] 编译成功，SPI 配置文件正确生成
- [ ] 代码已提交到 Git，commit message 清晰
- [ ] 设计文档和用户文档已更新

---

## 后续扩展

本实现仅包含标量函数（ScalarFunction）示例。后续可扩展：

1. **表值函数（TableFunction）** - 在 `udf/table/` 目录下添加实现
2. **聚合函数（AggregateFunction）** - 在 `udf/agg/` 目录下添加实现
3. **表值聚合函数（TableAggregateFunction）** - 在 `udf/tagg/` 目录下添加实现

扩展方式相同：实现 `UdfPlugin` 接口 + `@AutoService` 注解 + 编译生成 SPI 配置。