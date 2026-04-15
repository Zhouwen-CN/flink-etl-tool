# Google Auto Service 集成实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 使用 Google Auto Service 注解处理器简化 SPI 插件开发，消除手动维护 META-INF/services 文件的繁琐工作。

**Architecture:** 在核心模块添加 auto-service 依赖，所有插件实现类添加 `@AutoService` 注解，编译时自动生成 SPI 配置文件。保留手动文件作为兼容层。

**Tech Stack:** Java 11, Google Auto Service 1.1.1, Maven, Java SPI

---

## 调研结论

### Google Auto Service 是什么？

Google Auto Service 是一个注解处理器（Annotation Processor），用于在编译时自动生成 Java SPI 所需的 `META-INF/services/` 配置文件。

### 对比：使用前 vs 使用后

**使用前（当前方式）：**
```java
// MySQLSourcePlugin.java
public class MySQLSourcePlugin implements SourcePlugin {
    // ...
}
```

手动创建文件 `META-INF/services/com.etl.core.spi.SourcePlugin`：
```
com.etl.source.mysql.MySQLSourcePlugin
```

**使用后：**
```java
// MySQLSourcePlugin.java
@AutoService(SourcePlugin.class)
public class MySQLSourcePlugin implements SourcePlugin {
    // ...
}
```

编译时自动生成 `META-INF/services/com.etl.core.spi.SourcePlugin` 文件。

### 优势

1. **减少样板代码**：无需手动创建和同步 services 文件
2. **编译时安全**：注解处理器验证实现类是否正确实现接口
3. **重构友好**：重命名类时，IDE 会自动更新注解，services 文件自动重新生成
4. **避免人为错误**：不会忘记更新 services 文件

### Maven 依赖配置

```xml
<dependency>
    <groupId>com.google.auto.service</groupId>
    <artifactId>auto-service</artifactId>
    <version>1.1.1</version>
    <scope>provided</scope>  <!-- 仅编译时需要 -->
</dependency>
```

**注意：** 使用 `provided` scope 因为注解处理器只在编译时运行，不需要打包到最终 JAR。

---

## 文件结构

### 修改的文件

| 文件 | 职责 |
|------|------|
| `pom.xml` (根) | 添加 auto-service 版本属性和依赖管理 |
| `flink-etl-core/pom.xml` | 添加 auto-service 依赖（插件模块继承） |
| `MySQLSourcePlugin.java` | 添加 @AutoService 注解 |
| `ConsoleSinkPlugin.java` | 添加 @AutoService 注解 |
| `MySQLSinkPlugin.java` | 添加 @AutoService 注解 |
| `FieldMappingTransformPlugin.java` | 添加 @AutoService 注解 |

### 删除的文件（编译后）

| 文件 | 说明 |
|------|------|
| `META-INF/services/com.etl.core.spi.SourcePlugin` | 由注解处理器生成 |
| `META-INF/services/com.etl.core.spi.SinkPlugin` | 由注解处理器生成 |
| `META-INF/services/com.etl.core.spi.TransformPlugin` | 由注解处理器生成 |

---

## Chunk 1: 添加依赖配置

### Task 1: 添加 Auto Service 依赖

**Files:**
- Modify: `pom.xml:26-27` (添加版本属性)
- Modify: `pom.xml:95` (添加依赖管理)

- [ ] **Step 1: 在根 pom.xml 添加版本属性**

在 `<properties>` 中添加 auto-service 版本：

```xml
<auto-service.version>1.1.1</auto-service.version>
```

位置：在第 26 行 `lombok.version` 后面添加。

- [ ] **Step 2: 在 dependencyManagement 中添加依赖**

在 `dependencyManagement/dependencies` 中添加：

```xml
<!-- Google Auto Service - SPI 注解处理器 -->
<dependency>
    <groupId>com.google.auto.service</groupId>
    <artifactId>auto-service</artifactId>
    <version>${auto-service.version}</version>
</dependency>
```

位置：在 `commons-lang3` 依赖后面（约第 95 行）。

- [ ] **Step 3: 在 flink-etl-core 添加依赖**

在 `flink-etl-core/pom.xml` 的 `<dependencies>` 中添加：

```xml
<!-- Google Auto Service - SPI 注解处理器 -->
<dependency>
    <groupId>com.google.auto.service</groupId>
    <artifactId>auto-service</artifactId>
    <scope>provided</scope>
</dependency>
```

位置：在 `commons-lang3` 依赖后面（约第 60 行）。

- [ ] **Step 4: 验证依赖配置**

运行: `mvn dependency:tree -pl flink-etl-core | grep auto-service`

预期输出: `com.google.auto.service:auto-service:jar:1.1.1:provided`

- [ ] **Step 5: 提交**

```bash
git add pom.xml flink-etl-core/pom.xml
git commit -m "feat: 添加 Google Auto Service 依赖"
```

---

## Chunk 2: 为现有插件添加注解

### Task 2: MySQL Source Plugin 添加注解

**Files:**
- Modify: `flink-etl-source-mysql/src/main/java/com/etl/source/mysql/MySQLSourcePlugin.java`

- [ ] **Step 1: 添加 import 和注解**

修改文件：

```java
package com.etl.source.mysql;

import com.etl.core.localFileSourceConfig.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.SplitStrategy;
import source.com.etl.connector.jdbc.JdbcSource;
import com.etl.source.jdbc.dialect.MySQLDialect;
import com.google.auto.service.AutoService;  // 新增
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;

/**
 * MySQL Source 插件
 * 支持主键范围分片读取 MySQL 数据
 */
@Slf4j
@AutoService(SourcePlugin.class)  // 新增
public class MySQLSourcePlugin implements SourcePlugin {
    // ... 其余代码不变
}
```

- [ ] **Step 2: 删除手动创建的 services 文件**

删除: `flink-etl-source-mysql/src/main/resources/META-INF/services/com.etl.core.spi.SourcePlugin`

- [ ] **Step 3: 编译验证**

运行: `mvn clean compile -pl flink-etl-source/flink-etl-source-mysql -am`

- [ ] **Step 4: 验证自动生成的文件**

运行: `cat flink-etl-source/flink-etl-source-mysql/target/classes/META-INF/services/com.etl.core.spi.SourcePlugin`

预期输出: `com.etl.source.mysql.MySQLSourcePlugin`

- [ ] **Step 5: 提交**

```bash
git add flink-etl-source/flink-etl-source-mysql/src/main/java/com/etl/source/mysql/MySQLSourcePlugin.java
git add flink-etl-source/flink-etl-source-mysql/src/main/resources/META-INF/services/com.etl.core.spi.SourcePlugin
git commit -m "feat: MySQL Source 插件使用 @AutoService 注解"
```

### Task 3: Console Sink Plugin 添加注解

**Files:**
- Modify: `flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java`

- [ ] **Step 1: 添加 import 和注解**

修改文件，添加：

```java
import com.google.auto.service.AutoService;

@AutoService(SinkPlugin.class)
public class ConsoleSinkPlugin implements SinkPlugin {
    // ...
}
```

- [ ] **Step 2: 删除手动创建的 services 文件**

删除: `flink-etl-sink-console/src/main/resources/META-INF/services/com.etl.core.spi.SinkPlugin`

- [ ] **Step 3: 编译验证**

运行: `mvn clean compile -pl flink-etl-sink/flink-etl-sink-console -am`

- [ ] **Step 4: 验证自动生成的文件**

运行: `cat flink-etl-sink/flink-etl-sink-console/target/classes/META-INF/services/com.etl.core.spi.SinkPlugin`

预期输出: `com.etl.sink.console.ConsoleSinkPlugin`

- [ ] **Step 5: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java
git add flink-etl-sink/flink-etl-sink-console/src/main/resources/META-INF/services/com.etl.core.spi.SinkPlugin
git commit -m "feat: Console Sink 插件使用 @AutoService 注解"
```

### Task 4: MySQL Sink Plugin 添加注解

**Files:**
- Modify: `flink-etl-sink-mysql/src/main/java/com/etl/sink/mysql/MySQLSinkPlugin.java`

- [ ] **Step 1: 添加 import 和注解**

修改文件，添加：

```java
import com.google.auto.service.AutoService;

@AutoService(SinkPlugin.class)
public class MySQLSinkPlugin implements SinkPlugin {
    // ...
}
```

- [ ] **Step 2: 删除手动创建的 services 文件**

删除: `flink-etl-sink-mysql/src/main/resources/META-INF/services/com.etl.core.spi.SinkPlugin`

- [ ] **Step 3: 编译验证**

运行: `mvn clean compile -pl flink-etl-sink/flink-etl-sink-mysql -am`

- [ ] **Step 4: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-mysql/src/main/java/com/etl/sink/mysql/MySQLSinkPlugin.java
git add flink-etl-sink/flink-etl-sink-mysql/src/main/resources/META-INF/services/com.etl.core.spi.SinkPlugin
git commit -m "feat: MySQL Sink 插件使用 @AutoService 注解"
```

### Task 5: Transform Plugin 添加注解

**Files:**
- Modify: `flink-etl-transform/src/main/java/com/etl/transform/FieldMappingTransformPlugin.java`

- [ ] **Step 1: 添加 import 和注解**

修改文件，添加：

```java
import com.google.auto.service.AutoService;

@AutoService(TransformPlugin.class)
public class FieldMappingTransformPlugin implements TransformPlugin {
    // ...
}
```

- [ ] **Step 2: 删除手动创建的 services 文件**

删除: `flink-etl-transform/src/main/resources/META-INF/services/com.etl.core.spi.TransformPlugin`

- [ ] **Step 3: 编译验证**

运行: `mvn clean compile -pl flink-etl-transform -am`

- [ ] **Step 4: 提交**

```bash
git add flink-etl-transform/src/main/java/com/etl/transform/FieldMappingTransformPlugin.java
git add flink-etl-transform/src/main/resources/META-INF/services/com.etl.core.spi.TransformPlugin
git commit -m "feat: FieldMapping Transform 插件使用 @AutoService 注解"
```

---

## Chunk 3: 集成测试和文档更新

### Task 6: 完整项目构建测试

**Files:**
- 无文件修改

- [ ] **Step 1: 清理并完整构建**

运行: `mvn clean package -DskipTests`

预期: 构建成功，无错误。

- [ ] **Step 2: 验证生成的 services 文件**

检查所有 target 目录下的 services 文件是否正确生成：

```bash
# 检查 Source Plugin
find . -path "*/target/classes/META-INF/services/com.etl.core.spi.SourcePlugin" -exec cat {} \;

# 检查 Sink Plugin
find . -path "*/target/classes/META-INF/services/com.etl.core.spi.SinkPlugin" -exec cat {} \;

# 检查 Transform Plugin
find . -path "*/target/classes/META-INF/services/com.etl.core.spi.TransformPlugin" -exec cat {} \;
```

- [ ] **Step 3: 运行集成测试**

运行: `java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-console.json`

预期: 任务正常执行，数据正确输出到控制台。

### Task 7: 更新文档

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: 更新 CLAUDE.md 文档**

在 "扩展新数据源" 部分更新说明：

```markdown
### 扩展新数据源

添加新数据源需要：

1. 创建新模块，依赖 `flink-etl-core`
2. 实现 `SourcePlugin` 接口
3. 添加 `@AutoService(SourcePlugin.class)` 注解（自动生成 SPI 配置）
4. 继承 `AbstractRangeSplitSource` 实现分片读取（关系型数据库）
   - 实现 `getSplitColumnRange()` 方法获取分片范围
   - 分片数量根据 Job 配置的 `parallelism` 自动计算
5. 在 `flink-etl-client/pom.xml` 添加新模块依赖

**注意：** 使用 `@AutoService` 注解后，无需手动创建 `META-INF/services/` 目录下的服务配置文件，编译时会自动生成。
```

- [ ] **Step 2: 提交文档更新**

```bash
git add CLAUDE.md
git commit -m "docs: 更新 SPI 插件开发文档，说明 @AutoService 用法"
```

---

## 注意事项

1. **Lombok 兼容性**: Auto Service 和 Lombok 可以同时使用，但建议在 IDE 中启用 annotation processing。

2. **IDE 配置**: IntelliJ IDEA 需要启用 "Enable annotation processing" (Settings → Build → Compiler → Annotation Processors)。

3. **增量编译**: Auto Service 支持增量编译，不会影响构建速度。

4. **多接口实现**: 如果一个类实现多个 SPI 接口，可以使用多个 `@AutoService` 注解：
   ```java
   @AutoService({SourcePlugin.class, AnotherInterface.class})
   public class MyPlugin implements SourcePlugin, AnotherInterface { }
   ```

---

## 参考资源

- [Google Auto Service GitHub](https://github.com/google/auto/tree/master/service)
- [Java SPI 机制](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html)