# Connector 模块重构实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 flink-etl-source 和 flink-etl-sink 合并为 flink-etl-connector，统一管理连接器，并将 JDBC dialect 从 core 迁移到 connector-jdbc

**Architecture:** 创建新的 flink-etl-connector 顶级模块，包含 6 个 connector 子模块。每个 connector 统一管理 source、sink 和共用代码（dialect、converter）。保持大部分类包名和物理路径不变，只调整 dialect 和 converter 的包名和物理路径。

**Tech Stack:** Java 1.8, Maven, Git

---

## 文件结构概览

### 新增文件
- `flink-etl-connector/pom.xml` - 父模块 POM
- `flink-etl-connector/connector-jdbc/pom.xml` - JDBC connector POM
- `flink-etl-connector/connector-kafka/pom.xml` - Kafka connector POM
- `flink-etl-connector/connector-localfile/pom.xml` - LocalFile connector POM
- `flink-etl-connector/connector-console/pom.xml` - Console connector POM
- `flink-etl-connector/connector-http/pom.xml` - HTTP connector POM
- `flink-etl-connector/connector-mock/pom.xml` - Mock connector POM

### 迁移文件（使用 git mv，保持包名和物理路径不变）
- `flink-etl-source/flink-etl-source-jdbc/src/` → `flink-etl-connector/connector-jdbc/src/`
- `flink-etl-sink/flink-etl-sink-jdbc/src/` → 合并到 `flink-etl-connector/connector-jdbc/src/`
- 其他 connector 同理

注意：迁移后 connector-jdbc/src/main/java/ 下会包含多个并存的包结构：
- `com/etl/source/jdbc/` (source 类，包名 com.etl.source.jdbc)
- `com/etl/sink/jdbc/` (sink 类，包名 com.etl.sink.jdbc)
- `com/etl/connector/jdbc/dialect/` (dialect 类，包名 com.etl.connector.jdbc.dialect，新增)
- `com/etl/connector/jdbc/converter/` (converter 类，包名 com.etl.connector.jdbc.converter，新增)
- `com/etl/source/jdbc/utils/` (utils 类，包名 com.etl.jdbc.source.utils)

### 包名调整文件（需要修改 package 声明）
- `com.etl.core.dialect.*` → `com.etl.connector.jdbc.dialect.*` (物理路径：com/etl/core/dialect/ → com/etl/connector/jdbc/dialect/)
- `com.etl.core.jdbc.TypeConverter` → `com.etl.core.schema.TypeConverter` (物理路径：com/etl/core/jdbc/ → com/etl/connector/jdbc/converter/)

### 删除文件
- `flink-etl-source/` (整个目录，已迁移)
- `flink-etl-sink/` (整个目录，已迁移)
- `flink-etl-core/src/main/java/com/etl/core/dialect/` (整个目录，已迁移并调整包名)
- `flink-etl-core/src/main/java/com/etl/core/jdbc/TypeConverter.java` (已迁移并调整包名)

---

## 任务分解

### Task 1: 创建新模块结构和父 POM

**Files:**
- Create: `flink-etl-connector/pom.xml`
- Create: 各 connector 子模块目录

- [ ] **Step 1: 创建 flink-etl-connector 父模块目录**

```bash
mkdir -p flink-etl-connector/connector-jdbc
mkdir -p flink-etl-connector/connector-kafka
mkdir -p flink-etl-connector/connector-localfile
mkdir -p flink-etl-connector/connector-console
mkdir -p flink-etl-connector/connector-http
mkdir -p flink-etl-connector/connector-mock
```

- [ ] **Step 2: 创建父模块 POM 文件**

创建文件 `flink-etl-connector/pom.xml`:

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

    <artifactId>flink-etl-connector</artifactId>
    <packaging>pom</packaging>

    <name>Flink ETL Connector</name>
    <description>连接器插件集合（Source + Sink）</description>

    <modules>
        <module>connector-jdbc</module>
        <module>connector-kafka</module>
        <module>connector-localfile</module>
        <module>connector-console</module>
        <module>connector-http</module>
        <module>connector-mock</module>
    </modules>
</project>
```

- [ ] **Step 3: 提交新模块结构**

```bash
git add flink-etl-connector/pom.xml
git commit -m "feat: 创建 flink-etl-connector 父模块 POM

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 2: 创建 connector-jdbc POM

**Files:**
- Create: `flink-etl-connector/connector-jdbc/pom.xml`

- [ ] **Step 1: 创建 connector-jdbc POM 文件**

创建文件 `flink-etl-connector/connector-jdbc/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-connector</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>connector-jdbc</artifactId>

    <name>Flink ETL Connector - JDBC</name>
    <description>JDBC 连接器（Source + Sink + Dialect）</description>

    <dependencies>
        <!-- MySQL 驱动 -->
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
        </dependency>
        <!-- OceanBase 驿动 -->
        <dependency>
            <groupId>com.oceanbase</groupId>
            <artifactId>oceanbase-client</artifactId>
        </dependency>
        <!-- H2 数据库（测试用） -->
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 提交 connector-jdbc POM**

```bash
git add flink-etl-connector/connector-jdbc/pom.xml
git commit -m "feat: 创建 connector-jdbc POM

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 3: 迁移 JDBC Source 和 Sink（保持包名和物理路径）

**Files:**
- Move: `flink-etl-source/flink-etl-source-jdbc/src/` → `flink-etl-connector/connector-jdbc/src-jdbc-source/`
- Move: `flink-etl-sink/flink-etl-sink-jdbc/src/` → `flink-etl-connector/connector-jdbc/src-jdbc-sink/`

- [ ] **Step 1: 使用 git mv 迁移 JDBC Source 的 src 目录**

```bash
git mv flink-etl-source/flink-etl-source-jdbc/src flink-etl-connector/connector-jdbc/src-jdbc-source
```

这会保持所有的包结构：`com/etl/source/jdbc/` → 包名 `com.etl.source.jdbc`（不变）

- [ ] **Step 2: 使用 git mv 迁移 JDBC Sink 的 src 目录**

```bash
git mv flink-etl-sink/flink-etl-sink-jdbc/src flink-etl-connector/connector-jdbc/src-jdbc-sink
```

这会保持所有的包结构：`com/etl/sink/jdbc/` → 包名 `com.etl.sink.jdbc`（不变）

- [ ] **Step 3: 合并 source 和 sink 的目录**

由于 Maven 默认只识别 `src/main/java` 和 `src/test/java`，我们需要将临时目录合并：

```bash
# 将 source 的内容移动到标准 src 目录
mkdir -p flink-etl-connector/connector-jdbc/src
git mv flink-etl-connector/connector-jdbc/src-jdbc-source/main flink-etl-connector/connector-jdbc/src/main
git mv flink-etl-connector/connector-jdbc/src-jdbc-source/test flink-etl-connector/connector-jdbc/src/test

# 将 sink 的内容合并到 src 目录（source 和 sink 的包名不同，会并存）
cp -r flink-etl-connector/connector-jdbc/src-jdbc-sink/main/java/com/etl/sink/* flink-etl-connector/connector-jdbc/src/main/java/com/etl/
cp -r flink-etl-connector/connector-jdbc/src-jdbc-sink/test/java/com/etl/sink/* flink-etl-connector/connector-jdbc/src/test/java/com/etl/
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/sink
git add flink-etl-connector/connector-jdbc/src/test/java/com/etl/sink

# 删除临时的 sink 目录
git rm -rf flink-etl-connector/connector-jdbc/src-jdbc-sink
git rm -rf flink-etl-connector/connector-jdbc/src-jdbc-source
```

注意：Windows 环境下 cp 命令可能不适用，需要手动处理或使用 PowerShell 的 Copy-Item。

- [ ] **Step 4: 提交 JDBC Source 和 Sink 迁移**

```bash
git add flink-etl-connector/connector-jdbc/src
git commit -m "feat: 迁移 JDBC Source 和 Sink 到 connector-jdbc（保持包名）

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 4: 迁移 JDBC Dialect（调整包名和物理路径）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/*.java`
- Delete: `flink-etl-core/src/main/java/com/etl/core/dialect/` (整个目录)

- [ ] **Step 1: 创建 dialect 目录**

```bash
mkdir -p flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect
```

- [ ] **Step 2: 读取并迁移 JdbcDialect.java**

读取原文件：`flink-etl-core/src/main/java/com/etl/core/dialect/JdbcDialect.java`

创建新文件：`flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/JdbcDialect.java`

修改 package 声明：
```java
package com.etl.connector.jdbc.dialect; // 从 com.etl.core.dialect 改为新包名

// 其他内容保持不变
```

- [ ] **Step 3: 读取并迁移 JdbcDialectLoader.java**

同理，读取原文件并创建新文件，修改 package 为 `com.etl.connector.jdbc.dialect`

- [ ] **Step 4: 读取并迁移其他 dialect 类**

同理处理：
- `MySQLDialect.java`
- `PostgreSQLDialect.java`
- `OracleDialect.java`
- `H2Dialect.java`
- `WriteMode.java`

每个文件都需要读取原内容，修改 package 声明，创建新文件。

- [ ] **Step 5: 提交 dialect 迁移**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/dialect/
git commit -m "feat: 迁移 JDBC Dialect 到 connector-jdbc（调整包名）

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 5: 迁移 TypeConverter（调整包名和物理路径）

**Files:**
- Create: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/converter/TypeConverter.java`
- Delete: `flink-etl-core/src/main/java/com/etl/core/jdbc/TypeConverter.java`

- [ ] **Step 1: 创建 converter 目录**

```bash
mkdir -p flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/converter
```

- [ ] **Step 2: 读取并迁移 TypeConverter.java**

读取原文件：`flink-etl-core/src/main/java/com/etl/core/jdbc/TypeConverter.java`

创建新文件：`flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/converter/TypeConverter.java`

修改 package 声明：
```java
package com.etl.connector.jdbc.converter; // 从 com.etl.core.jdbc 改为新包名

// 其他内容保持不变
```

- [ ] **Step 3: 提交 converter 迁移**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/converter/
git commit -m "feat: 迁移 TypeConverter 到 connector-jdbc（调整包名）

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 6: 更新 JDBC 相关类的 import 语句

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/source/jdbc/utils/JdbcSplitHelper.java`
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/source/jdbc/config/JdbcSourceConfig.java`
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/sink/jdbc/JdbcSink.java`
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/sink/jdbc/config/JdbcSinkConfig.java`

- [ ] **Step 1: 查找所有引用 dialect 的文件**

使用 Grep tool 搜索：

Pattern: `import com.etl.core.dialect`

Path: `flink-etl-connector/connector-jdbc/src/main/java`

预期找到：JdbcSource.java, JdbcSink.java, JdbcSplitHelper.java, JdbcSourceConfig.java, JdbcSinkConfig.java

- [ ] **Step 2: 查找所有引用 TypeConverter 的文件**

使用 Grep tool 搜索：

Pattern: `import com.etl.core.jdbc.TypeConverter`

Path: `flink-etl-connector/connector-jdbc/src/main/java`

- [ ] **Step 3: 更新 JdbcSource.java 的 import 语句**

使用 Read tool 读取文件，然后使用 Edit tool 替换：

```
old_string: import com.etl.core.dialect.JdbcDialect;
new_string: import dialect.com.etl.connector.jdbc.JdbcDialect;
```

```
old_string: import com.etl.core.dialect.JdbcDialectLoader;
new_string: import dialect.com.etl.connector.jdbc.JdbcDialectLoader;
```

```
old_string: import com.etl.core.jdbc.TypeConverter;
new_string: import com.etl.core.schema.TypeConverter;
```

- [ ] **Step 4: 更新 JdbcSink.java 的 import 语句**

同理更新 JdbcSink.java 的 dialect 和 TypeConverter import

- [ ] **Step 5: 更新其他 JDBC 相关文件的 import 语句**

同理更新：
- JdbcSplitHelper.java
- JdbcSourceConfig.java
- JdbcSinkConfig.java

- [ ] **Step 6: 提交 import 语句更新**

```bash
git add flink-etl-connector/connector-jdbc/src/main/java/
git commit -m "refactor: 更新 JDBC 类的 import 语句（dialect 和 converter 包名调整）

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 7: 创建其他 connector 的 POM 文件

**Files:**
- Create: `flink-etl-connector/connector-kafka/pom.xml`
- Create: `flink-etl-connector/connector-localfile/pom.xml`
- Create: `flink-etl-connector/connector-console/pom.xml`
- Create: `flink-etl-connector/connector-http/pom.xml`
- Create: `flink-etl-connector/connector-mock/pom.xml`

- [ ] **Step 1: 创建 connector-kafka POM**

创建文件 `flink-etl-connector/connector-kafka/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-connector</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>connector-kafka</artifactId>

    <name>Flink ETL Connector - Kafka</name>
    <description>Kafka 连接器（Source + Sink）</description>
</project>
```

- [ ] **Step 2: 创建 connector-localfile POM**

同理创建 `flink-etl-connector/connector-localfile/pom.xml`

- [ ] **Step 3: 创建 connector-console POM**

同理创建 `flink-etl-connector/connector-console/pom.xml`

- [ ] **Step 4: 创建 connector-http POM**

同理创建 `flink-etl-connector/connector-http/pom.xml`

- [ ] **Step 5: 创建 connector-mock POM**

同理创建 `flink-etl-connector/connector-mock/pom.xml`

- [ ] **Step 6: 提交其他 connector POM**

```bash
git add flink-etl-connector/connector-*/pom.xml
git commit -m "feat: 创建其他 connector 的 POM 文件

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 8: 迁移其他 connector（保持包名）

**Files:**
- Move: connector-kafka (source + sink)
- Move: connector-localfile (source only)
- Move: connector-console (sink only)
- Move: connector-http (source only)
- Move: connector-mock (source only)

- [ ] **Step 1: 迁移 connector-kafka**

```bash
# 迁移 source
git mv flink-etl-source/flink-etl-source-kafka/src flink-etl-connector/connector-kafka/src-kafka-source

# 迁移 sink
git mv flink-etl-sink/flink-etl-sink-kafka/src flink-etl-connector/connector-kafka/src-kafka-sink

# 合并到标准 src 目录
mkdir -p flink-etl-connector/connector-kafka/src
git mv flink-etl-connector/connector-kafka/src-kafka-source/main flink-etl-connector/connector-kafka/src/main
git mv flink-etl-connector/connector-kafka/src-kafka-source/test flink-etl-connector/connector-kafka/src/test

# 合并 sink 的内容
cp -r flink-etl-connector/connector-kafka/src-kafka-sink/main/java/com/etl/sink/* flink-etl-connector/connector-kafka/src/main/java/com/etl/
cp -r flink-etl-connector/connector-kafka/src-kafka-sink/test/java/com/etl/sink/* flink-etl-connector/connector-kafka/src/test/java/com/etl/
git add flink-etl-connector/connector-kafka/src/main/java/com/etl/sink
git add flink-etl-connector/connector-kafka/src/test/java/com/etl/sink

# 删除临时目录
git rm -rf flink-etl-connector/connector-kafka/src-kafka-source
git rm -rf flink-etl-connector/connector-kafka/src-kafka-sink
```

- [ ] **Step 2: 迁移 connector-localfile（只有 source）**

```bash
git mv flink-etl-source/flink-etl-source-localfile/src flink-etl-connector/connector-localfile/src
```

- [ ] **Step 3: 迁移 connector-console（只有 sink）**

```bash
git mv flink-etl-sink/flink-etl-sink-console/src flink-etl-connector/connector-console/src
```

- [ ] **Step 4: 迁移 connector-http（只有 source）**

```bash
git mv flink-etl-source/flink-etl-source-http/src flink-etl-connector/connector-http/src
```

- [ ] **Step 5: 迁移 connector-mock（只有 source）**

```bash
git mv flink-etl-source/flink-etl-source-mock/src flink-etl-connector/connector-mock/src
```

- [ ] **Step 6: 提交其他 connector 迁移**

```bash
git add flink-etl-connector/connector-kafka/src
git add flink-etl-connector/connector-localfile/src
git add flink-etl-connector/connector-console/src
git add flink-etl-connector/connector-http/src
git add flink-etl-connector/connector-mock/src
git commit -m "feat: 迁移其他 connector（kafka、localfile、console、http、mock）

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 9: 更新顶级 pom.xml

**Files:**
- Modify: `pom.xml`

- [ ] **Step 1: 读取顶级 pom.xml**

使用 Read tool 读取文件

- [ ] **Step 2: 更新 modules 部分**

使用 Edit tool 替换：

```xml
old_string:
    <modules>
        <module>flink-etl-core</module>
        <module>flink-etl-client</module>
        <module>flink-etl-source</module>
        <module>flink-etl-sink</module>
        <module>flink-etl-transform</module>
    </modules>

new_string:
    <modules>
        <module>flink-etl-core</module>
        <module>flink-etl-client</module>
        <module>flink-etl-connector</module>
        <module>flink-etl-transform</module>
    </modules>
```

- [ ] **Step 3: 提交顶级 pom.xml 更新**

```bash
git add pom.xml
git commit -m "refactor: 更新顶级 pom.xml 的模块列表

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 10: 更新 flink-etl-client pom.xml

**Files:**
- Modify: `flink-etl-client/pom.xml`

- [ ] **Step 1: 读取 flink-etl-client/pom.xml**

使用 Read tool 读取文件

- [ ] **Step 2: 替换 dependencies 部分**

使用 Edit tool 替换：

```xml
old_string:
        <!-- Source 插件 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-source-jdbc</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-source-localfile</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-source-http</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-source-kafka</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-source-mock</artifactId>
            <version>${project.version}</version>
        </dependency>

        <!-- Sink 插件 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-sink-console</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-sink-jdbc</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-sink-kafka</artifactId>
            <version>${project.version}</version>
        </dependency>

new_string:
        <!-- 连接器插件 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>connector-jdbc</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>connector-kafka</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>connector-localfile</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>connector-console</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>connector-http</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>connector-mock</artifactId>
            <version>${project.version}</version>
        </dependency>
```

- [ ] **Step 3: 提交 flink-etl-client pom.xml 更新**

```bash
git add flink-etl-client/pom.xml
git commit -m "refactor: 更新 flink-etl-client 的依赖为 connector

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 11: 删除旧模块和 core 中的 dialect

**Files:**
- Delete: `flink-etl-source/` (整个目录)
- Delete: `flink-etl-sink/` (整个目录)
- Delete: `flink-etl-core/src/main/java/com/etl/core/dialect/` (整个目录)
- Delete: `flink-etl-core/src/main/java/com/etl/core/jdbc/TypeConverter.java`

- [ ] **Step 1: 删除 flink-etl-source 和 flink-etl-sink 目录**

```bash
git rm -rf flink-etl-source/
git rm -rf flink-etl-sink/
```

- [ ] **Step 2: 删除 flink-etl-core 中的 dialect 目录**

```bash
git rm -rf flink-etl-core/src/main/java/com/etl/core/dialect/
```

- [ ] **Step 3: 删除 flink-etl-core 中的 TypeConverter**

```bash
git rm flink-etl-core/src/main/java/com/etl/core/jdbc/TypeConverter.java
```

如果整个 jdbc 目录只剩下 TypeConverter，可以删除整个目录：

```bash
git rm -rf flink-etl-core/src/main/java/com/etl/core/jdbc/
```

- [ ] **Step 4: 提交删除旧模块**

```bash
git commit -m "refactor: 删除旧的 source/sink 模块和 core 中的 dialect

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 12: 编译验证

- [ ] **Step 1: 执行编译**

```bash
mvn clean compile
```

Expected: 编译成功，无错误

- [ ] **Step 2: 检查编译结果**

如果编译失败，检查错误信息：
- Import 语句错误：回到 Task 6 修复
- 包名错误：检查 dialect 和 converter 的包声明
- POM 依赖问题：检查 Task 9 和 Task 10

---

### Task 13: 运行测试

- [ ] **Step 1: 执行测试**

```bash
mvn test
```

Expected: 所有测试通过

- [ ] **Step 2: 检查测试结果**

如果测试失败，分析失败原因：
- 测试文件未迁移：检查 Task 3-8 的测试文件迁移
- Import 语句错误：回到 Task 6
- 包名问题：检查 dialect 和 converter 的包声明和 import

---

### Task 14: 更新文档

**Files:**
- Modify: `CLAUDE.md`
- Modify: `PLUGINS.md`

- [ ] **Step 1: 读取 CLAUDE.md**

使用 Read tool 读取文件

- [ ] **Step 2: 更新 CLAUDE.md 的架构概览部分**

找到"模块结构"部分，更新为：

```markdown
### 模块结构

```
flink-etl-tool/
├── flink-etl-core/               # 核心框架
├── flink-etl-client/             # 客户端启动器
├── flink-etl-connector/          # 连接器插件
│   ├── connector-jdbc/           # JDBC 连接器（Source + Sink + Dialect）
│   ├── connector-kafka/          # Kafka 连接器
│   ├── connector-localfile/      # 本地文件连接器
│   ├── connector-console/        # Console 连接器
│   ├── connector-http/           # HTTP 连接器
│   └── connector-mock/           # Mock 连接器
└── flink-etl-transform/          # Transform 插件
```
```

同时更新"扩展新插件"部分中提及的模块路径。

- [ ] **Step 3: 提交 CLAUDE.md 更新**

```bash
git add CLAUDE.md
git commit -m "docs: 更新 CLAUDE.md 的模块结构说明

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

- [ ] **Step 4: 读取 PLUGINS.md**

使用 Read tool 读取文件

- [ ] **Step 5: 更新 PLUGINS.md 的插件列表**

更新 Source 和 Sink 插件的模块路径说明。

- [ ] **Step 6: 提交 PLUGINS.md 更新**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md 的插件列表

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

### Task 15: 运行示例任务验证

- [ ] **Step 1: 打包项目**

```bash
mvn clean package
```

Expected: 打包成功，生成 JAR 文件

- [ ] **Step 2: 运行示例任务**

```bash
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/mysql-to-console.json
```

Expected: 任务运行成功，输出正常

---

## 自审检查清单

完成所有任务后，检查以下内容：

1. ✓ 所有文件迁移完成，旧模块已删除
2. ✓ dialect 和 converter 包名和物理路径已调整
3. ✓ 所有 import 语句已更新
4. ✓ POM 文件依赖已更新
5. ✓ 编译成功
6. ✓ 测试通过
7. ✓ 示例任务运行成功
8. ✓ 文档已更新
9. ✓ Git 历史保留（使用 git mv）
10. ✓ 所有提交包含 Co-Authored-By 信息