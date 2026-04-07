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

### 1.3 设计约束

- UDF 实现必须放在 `flink-etl-core` 模块内部，不支持外部独立模块扩展
- 使用现有的 SPI 插件体系架构，保持与 Source/Sink/Transform 插件的一致性
- 函数名由用户通过 `identifier()` 方法自由定义，避免强制前缀

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
 */
public static List<UdfPlugin> loadAllUdfPlugins() {
    log.info("批量加载所有 UDF 插件");

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
        classLoader = PluginLoader.class.getClassLoader();
    }

    ServiceLoader<UdfPlugin> loader = ServiceLoader.load(UdfPlugin.class, classLoader);
    List<UdfPlugin> plugins = new ArrayList<>();

    for (UdfPlugin plugin : loader) {
        log.info("UDF 插件加载成功：{} -> {}",
                 plugin.identifier(), plugin.getClass().getName());
        plugins.add(plugin);
    }

    log.info("共加载 {} 个 UDF 插件", plugins.size());
    return plugins;
}
```

**设计要点：**
- 使用 `ServiceLoader` 批量加载所有 `UdfPlugin` 实现
- 记录每个 UDF 的函数名和类名
- 返回列表供批量注册使用

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
 */
private static void registerAllUdfs(StreamTableEnvironment stEnv) {
    List<UdfPlugin> udfPlugins = PluginLoader.loadAllUdfPlugins();

    for (UdfPlugin udf : udfPlugins) {
        String functionName = udf.identifier();
        UserDefinedFunction functionInstance = udf.createFunction();

        // 注册为临时系统函数
        stEnv.createTemporaryFunction(functionName, functionInstance);
        log.info("UDF 注册成功：{}", functionName);
    }

    if (udfPlugins.isEmpty()) {
        log.info("未发现任何 UDF 插件");
    }
}
```

**设计要点：**
- 在创建 `StreamTableEnvironment` 后立即注册 UDF
- 确保后续 SQL Transform 可以使用这些函数
- 使用 `createTemporaryFunction()` 注册函数实例
- 日志记录每个注册的函数名，便于调试

---

## 五、实现示例

### 5.1 标量函数示例（ScalarFunction）

```java
package com.etl.core.udf.scalar;

import com.etl.core.spi.UdfPlugin;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

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

        public int eval(Object input) {
            if (input == null) {
                return 0;
            }
            return input.hashCode();
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

## 十、总结

本设计基于现有的 SPI 插件体系，通过定义 `UdfPlugin` 接口和扩展 `PluginLoader`、`JobBuilder`，实现了 Flink UDF 的自动加载和注册功能。

**核心特性：**
- 完全遵循现有 Plugin 体系架构
- 支持 Flink 四种 UDF 类型
- 自动加载所有 UDF，无需手动配置
- 用户在对应文件夹下实现 UDF 即可使用

**实现优先级：**
- 第一阶段：实现标量函数（ScalarFunction）示例
- 第二阶段：扩展支持表值函数（TableFunction）
- 第三阶段：扩展支持聚合函数（AggregateFunction）和表值聚合函数（TableAggregateFunction）