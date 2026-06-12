# Doris Sink 插件 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `connector-doris` 模块，提供 `type=doris` 的 Sink，通过 `flink-doris-connector-1.15:1.5.2` 的 Stream Load 把 `Row` 以 JSON 写入 Doris，序列化格式经 SPI ��扩展（本期仅 json）。

**Architecture:** 镜像 `KafkaSinkPlugin` 模式——`DorisSinkPlugin.createSink` 直接返回官方 `DorisSink.<Row>builder().build()`，塞自定义 `DorisRecordSerializer<Row>`。format 走独立 SPI（`DorisFormatPlugin` + `ServiceLoader`），与 Kafka source 的 `KafkaFormatPlugin` 同构。at-least-once（batch mode，非 2PC）。

**Tech Stack:** Java 1.8、Flink 1.15.2、flink-doris-connector-1.15:1.5.2、Google AutoService、Lombok、JUnit 5。

---

## 已核对的真实 API（1.5.2 反编译确认）

```java
// org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer<T> extends Serializable
DorisRecord serialize(T record) throws IOException;   // 唯一抽象方法；initial/flush/close 有 default

// org.apache.doris.flink.sink.writer.serializer.DorisRecord
static DorisRecord of(String database, String table, byte[] row);

// org.apache.doris.flink.cfg.DorisOptions.Builder
setFenodes(String) setTableIdentifier(String) setUsername(String) setPassword(String) build()

// org.apache.doris.flink.cfg.DorisExecutionOptions.Builder
setStreamLoadProp(Properties) setLabelPrefix(String) setBufferFlushMaxRows(int)
setBatchMode(Boolean) disable2PC() build()

// org.apache.doris.flink.cfg.DorisReadOptions.Builder  → builder().build()（Sink 用默认）

// org.apache.doris.flink.sink.DorisSink.Builder<IN>
setDorisOptions(DorisOptions) setDorisReadOptions(DorisReadOptions)
setDorisExecutionOptions(DorisExecutionOptions) setSerializer(DorisRecordSerializer<IN>) build()
```

**at-least-once 实现：** `setBatchMode(true)`（batch 模式天然非 2PC，1.5.x 推荐写法）。`batchSize` → `setBufferFlushMaxRows`。

## 依赖说明（无需在新模块 pom 声明）

根 `pom.xml` 的 `<dependencies>`（88-220 行）已含 `flink-doris-connector-1.15`（provided）、lombok、google-auto-service，全部被子模块继承。`flink-etl-connector/pom.xml` 还统一注入 `flink-etl-core`。故 `connector-doris/pom.xml` 只需声明 parent + artifactId，无额外 `<dependencies>`。

## 文件结构

```
flink-etl-connector/connector-doris/
├── pom.xml                                                         # 新建
└── src/
    ├── main/java/com/etl/connector/doris/sink/
    │   ├── DorisSinkPlugin.java                                    # 新建 @AutoService(SinkPlugin.class)
    │   ├── config/DorisSinkConfig.java                            # 新建 @Builder Serializable
    │   └── format/
    │       ├── DorisFormatPlugin.java                            # 新建 SPI 接口
    │       ├── DorisFormatLoader.java                            # 新建 ServiceLoader 加载
    │       ├── JsonFormatPlugin.java                             # 新建 @AutoService(DorisFormatPlugin.class)
    │       └── RowToDorisJsonSerializer.java                     # 新建 DorisRecordSerializer<Row>
    └── test/java/com/etl/connector/doris/sink/
        ├── config/DorisSinkConfigTest.java                       # 新建
        └── format/
            ├── DorisFormatLoaderTest.java                        # 新建
            └── RowToDorisJsonSerializerTest.java                # 新建
```

其余改动：
- `flink-etl-connector/pom.xml`：modules 加 `connector-doris`
- `flink-etl-client/pom.xml`：dependencies 加 `connector-doris`
- `docs/examples/batch-mock2doris.json`：示例
- `PLUGINS.md`：文档

---

### Task 1: 创建模块骨架与 pom

**Files:**
- Create: `flink-etl-connector/connector-doris/pom.xml`
- Modify: `flink-etl-connector/pom.xml`（modules 段）

- [ ] **Step 1: 创建 connector-doris/pom.xml**

`flink-etl-connector/connector-doris/pom.xml`：

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

    <artifactId>connector-doris</artifactId>

    <name>Flink ETL Connector - Doris</name>
    <description>Doris 连接器（Sink）</description>
</project>
```

- [ ] **Step 2: 注册模块到 connector 父 pom**

`flink-etl-connector/pom.xml` 的 `<modules>` 内，`connector-modbus` 后加一行：

```xml
        <module>connector-modbus</module>
        <module>connector-doris</module>
```

- [ ] **Step 3: 验证模块可解析**

Run: `mvn -q -pl flink-etl-connector/connector-doris validate`
Expected: BUILD SUCCESS（仅校验 pom 结构，无源码）

- [ ] **Step 4: Commit**

```bash
git add flink-etl-connector/connector-doris/pom.xml flink-etl-connector/pom.xml
git commit -m "feat: 新增 connector-doris 模块骨架"
```

---

### Task 2: DorisSinkConfig（配置 + 校验）

**Files:**
- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/config/DorisSinkConfig.java`
- Test: `flink-etl-connector/connector-doris/src/test/java/com/etl/connector/doris/sink/config/DorisSinkConfigTest.java`

参考 `KafkaSinkConfig.fromSinkConfig` 的校验风格（`config.get("key", String.class)` + 空值抛 `IllegalArgumentException`）。`SinkConfig.get` 已有重载 `get(key, type)` 和 `get(key, type, default)`。

- [ ] **Step 1: 写失败测试**

`DorisSinkConfigTest.java`：

```java
package com.etl.connector.doris.sink.config;

import com.etl.core.config.SinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DorisSinkConfigTest {

    private SinkConfig sinkConfig(Map<String, Object> props) {
        SinkConfig config = new SinkConfig();
        config.setType("doris");
        config.setProperties(props);
        return config;
    }

    private Map<String, Object> validProps() {
        Map<String, Object> p = new HashMap<>();
        p.put("fenodes", "127.0.0.1:8030");
        p.put("tableIdentifier", "test_db.test_tbl");
        p.put("username", "root");
        p.put("password", "");
        return p;
    }

    @Test
    void fromSinkConfig_allRequiredPresent_parsesAndDefaultsFormatJson() {
        DorisSinkConfig cfg = DorisSinkConfig.fromSinkConfig(sinkConfig(validProps()));
        assertEquals("127.0.0.1:8030", cfg.getFenodes());
        assertEquals("test_db.test_tbl", cfg.getTableIdentifier());
        assertEquals("root", cfg.getUsername());
        assertEquals("", cfg.getPassword());
        assertEquals("json", cfg.getFormat());
        assertNull(cfg.getLabelPrefix());
        assertNull(cfg.getBatchSize());
    }

    @Test
    void fromSinkConfig_optionalPresent_parsed() {
        Map<String, Object> p = validProps();
        p.put("labelPrefix", "etl-doris");
        p.put("batchSize", 5000);
        p.put("format", "json");
        DorisSinkConfig cfg = DorisSinkConfig.fromSinkConfig(sinkConfig(p));
        assertEquals("etl-doris", cfg.getLabelPrefix());
        assertEquals(5000, cfg.getBatchSize());
    }

    @Test
    void fromSinkConfig_missingFenodes_throws() {
        Map<String, Object> p = validProps();
        p.remove("fenodes");
        assertThrows(IllegalArgumentException.class, () -> DorisSinkConfig.fromSinkConfig(sinkConfig(p)));
    }

    @Test
    void fromSinkConfig_missingTableIdentifier_throws() {
        Map<String, Object> p = validProps();
        p.remove("tableIdentifier");
        assertThrows(IllegalArgumentException.class, () -> DorisSinkConfig.fromSinkConfig(sinkConfig(p)));
    }

    @Test
    void fromSinkConfig_tableIdentifierWithoutDot_throws() {
        Map<String, Object> p = validProps();
        p.put("tableIdentifier", "no_dot_table");
        assertThrows(IllegalArgumentException.class, () -> DorisSinkConfig.fromSinkConfig(sinkConfig(p)));
    }
}
```

> 注：`SinkConfig` 继承 `BaseConfig`，`get(...)` 与 `setProperties(Map)` 来自基类。若实际 setter 名不同，运行测试时按编译错误修正（参考 `KafkaSinkConfigTest`）。

- [ ] **Step 2: 运行测试，确认失败**

Run: `mvn -q -pl flink-etl-connector/connector-doris test -Dtest=DorisSinkConfigTest`
Expected: 编译失败（`DorisSinkConfig` 不存在）

- [ ] **Step 3: 写最小实现**

`DorisSinkConfig.java`：

```java
package com.etl.connector.doris.sink.config;

import com.etl.core.config.SinkConfig;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Doris Sink 配置
 */
@Getter
@Builder
public class DorisSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Doris FE 节点，host:port */
    private final String fenodes;
    /** 目标表标识，db.table */
    private final String tableIdentifier;
    /** 用户名 */
    private final String username;
    /** 密码 */
    private final String password;
    /** Stream Load label 前缀（可选） */
    private final String labelPrefix;
    /** 批量缓冲条数（可选） */
    private final Integer batchSize;
    /** 序列化格式，默认 json */
    private final String format;

    /**
     * 从 SinkConfig 解析并校验
     */
    public static DorisSinkConfig fromSinkConfig(SinkConfig config) {
        String fenodes = config.get("fenodes", String.class);
        Preconditions.checkArgument(fenodes != null && !fenodes.trim().isEmpty(), "fenodes 不能为空");

        String tableIdentifier = config.get("tableIdentifier", String.class);
        Preconditions.checkArgument(tableIdentifier != null && !tableIdentifier.trim().isEmpty(),
                "tableIdentifier 不能为空");
        Preconditions.checkArgument(tableIdentifier.contains("."),
                "tableIdentifier 必须为 db.table 格式: " + tableIdentifier);

        String username = config.get("username", String.class);
        Preconditions.checkArgument(username != null && !username.trim().isEmpty(), "username 不能为空");

        // password 允许空字符串，但不允许 null
        String password = config.get("password", String.class);
        Preconditions.checkArgument(password != null, "password 不能为 null");

        String labelPrefix = config.get("labelPrefix", String.class);
        Integer batchSize = config.get("batchSize", Integer.class);
        String format = config.get("format", String.class, "json");

        return DorisSinkConfig.builder()
                .fenodes(fenodes)
                .tableIdentifier(tableIdentifier)
                .username(username)
                .password(password)
                .labelPrefix(labelPrefix)
                .batchSize(batchSize)
                .format(format)
                .build();
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `mvn -q -pl flink-etl-connector/connector-doris test -Dtest=DorisSinkConfigTest`
Expected: PASS（5 个测试）

- [ ] **Step 5: Commit**

```bash
git add flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/config/DorisSinkConfig.java flink-etl-connector/connector-doris/src/test/java/com/etl/connector/doris/sink/config/DorisSinkConfigTest.java
git commit -m "feat: 新增 DorisSinkConfig 配置与校验"
```

---

### Task 3: DorisFormatPlugin SPI 接口

**Files:**
- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/format/DorisFormatPlugin.java`

无独立测试（纯接口），由 Task 4/5 覆盖。

- [ ] **Step 1: 写接口**

`DorisFormatPlugin.java`：

```java
package com.etl.connector.doris.sink.format;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Properties;

/**
 * Doris Sink 序列化格式 SPI 接口
 * 定义在 Doris Sink 模块内部，不污染 core 模块
 */
public interface DorisFormatPlugin extends Serializable {

    /**
     * Format 标识符，如 "json"、"debezium-json"
     */
    String identifier();

    /**
     * 创建 Row 序列化器
     */
    DorisRecordSerializer<Row> createSerializer(DorisSinkConfig config);

    /**
     * 该 format 对应的 Stream Load 属性
     * 如 json -> format=json, read_json_by_line=true
     */
    Properties streamLoadProperties();
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn -q -pl flink-etl-connector/connector-doris compile`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/format/DorisFormatPlugin.java
git commit -m "feat: 新增 DorisFormatPlugin SPI 接口"
```

---

### Task 4: RowToDorisJsonSerializer（Row → JSON bytes）

**Files:**
- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/format/RowToDorisJsonSerializer.java`
- Test: `flink-etl-connector/connector-doris/src/test/java/com/etl/connector/doris/sink/format/RowToDorisJsonSerializerTest.java`

复用 `com.etl.core.schema.RowToJsonConverter.convertRowToJsonNode(Row)` 和 `com.etl.core.utils.JsonUtils.writeValueAsString(Object)`。`DorisRecord.of(db, table, byte[])`。

- [ ] **Step 1: 写失败测试**

`RowToDorisJsonSerializerTest.java`：

```java
package com.etl.connector.doris.sink.format;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RowToDorisJsonSerializerTest {

    private DorisSinkConfig config() {
        return DorisSinkConfig.builder()
                .fenodes("127.0.0.1:8030")
                .tableIdentifier("test_db.test_tbl")
                .username("root")
                .password("")
                .format("json")
                .build();
    }

    @Test
    void serialize_namedRow_producesJsonWithDbAndTable() throws Exception {
        RowToDorisJsonSerializer ser = new RowToDorisJsonSerializer(config());

        Row row = Row.withNames();
        row.setField("id", 1);
        row.setField("name", "alice");

        DorisRecord record = ser.serialize(row);

        assertEquals("test_db", record.getDatabase());
        assertEquals("test_tbl", record.getTable());

        String json = new String(record.getRow(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"id\""), "json 应含字段 id: " + json);
        assertTrue(json.contains("\"name\""), "json 应含字段 name: " + json);
        assertTrue(json.contains("alice"), "json 应含值 alice: " + json);
    }
}
```

> 注：`RowToJsonConverter` 依赖具名 Row（field names）。`Row.withNames()` + `setField(name, value)` 产生具名 Row，与项目 Source 输出一致。

- [ ] **Step 2: 运行测试，确认失败**

Run: `mvn -q -pl flink-etl-connector/connector-doris test -Dtest=RowToDorisJsonSerializerTest`
Expected: 编译失败（`RowToDorisJsonSerializer` 不存在）

- [ ] **Step 3: 写实现**

`RowToDorisJsonSerializer.java`：

```java
package com.etl.connector.doris.sink.format;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.etl.core.schema.RowToJsonConverter;
import com.etl.core.utils.JsonUtils;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Row 到 Doris JSON 字节的序列化器
 * 复用 RowToJsonConverter，输出每行一个 JSON 对象（配合 read_json_by_line=true）
 */
public class RowToDorisJsonSerializer implements DorisRecordSerializer<Row> {

    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;

    public RowToDorisJsonSerializer(DorisSinkConfig config) {
        String[] parts = config.getTableIdentifier().split("\\.", 2);
        this.database = parts[0];
        this.table = parts[1];
    }

    @Override
    public DorisRecord serialize(Row row) throws IOException {
        JsonNode node = RowToJsonConverter.convertRowToJsonNode(row);
        String json = JsonUtils.writeValueAsString(node);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return DorisRecord.of(database, table, bytes);
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `mvn -q -pl flink-etl-connector/connector-doris test -Dtest=RowToDorisJsonSerializerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/format/RowToDorisJsonSerializer.java flink-etl-connector/connector-doris/src/test/java/com/etl/connector/doris/sink/format/RowToDorisJsonSerializerTest.java
git commit -m "feat: 新增 RowToDorisJsonSerializer 序列化器"
```

---

### Task 5: JsonFormatPlugin + DorisFormatLoader（SPI 加载）

**Files:**
- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/format/JsonFormatPlugin.java`
- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/format/DorisFormatLoader.java`
- Test: `flink-etl-connector/connector-doris/src/test/java/com/etl/connector/doris/sink/format/DorisFormatLoaderTest.java`

`DorisFormatLoader` 镜像 `com.etl.connector.kafka.source.format.KafkaFormatLoader`（静态 ServiceLoader 缓存）。

- [ ] **Step 1: 写失败测试**

`DorisFormatLoaderTest.java`：

```java
package com.etl.connector.doris.sink.format;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DorisFormatLoaderTest {

    @Test
    void getFormatPlugin_json_returnsPlugin() {
        DorisFormatPlugin plugin = DorisFormatLoader.getFormatPlugin("json");
        assertNotNull(plugin);
        assertEquals("json", plugin.identifier());
    }

    @Test
    void getFormatPlugin_unknown_returnsNull() {
        assertNull(DorisFormatLoader.getFormatPlugin("no-such-format"));
    }

    @Test
    void supportedFormats_containsJson() {
        assertTrue(DorisFormatLoader.supportedFormats().contains("json"));
    }

    @Test
    void jsonPlugin_streamLoadProperties_setsJsonProps() {
        DorisFormatPlugin plugin = DorisFormatLoader.getFormatPlugin("json");
        assertEquals("json", plugin.streamLoadProperties().getProperty("format"));
        assertEquals("true", plugin.streamLoadProperties().getProperty("read_json_by_line"));
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `mvn -q -pl flink-etl-connector/connector-doris test -Dtest=DorisFormatLoaderTest`
Expected: 编译失败（`JsonFormatPlugin`/`DorisFormatLoader` 不存在）

- [ ] **Step 3: 写 JsonFormatPlugin**

`JsonFormatPlugin.java`：

```java
package com.etl.connector.doris.sink.format;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.google.auto.service.AutoService;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * JSON 格式插件
 * Row 序列化为每行一个 JSON 对象，配合 Stream Load read_json_by_line=true
 */
@AutoService(DorisFormatPlugin.class)
public class JsonFormatPlugin implements DorisFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "json";
    }

    @Override
    public DorisRecordSerializer<Row> createSerializer(DorisSinkConfig config) {
        return new RowToDorisJsonSerializer(config);
    }

    @Override
    public Properties streamLoadProperties() {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        return props;
    }
}
```

- [ ] **Step 4: 写 DorisFormatLoader**

`DorisFormatLoader.java`：

```java
package com.etl.connector.doris.sink.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Doris Format Plugin 加载器
 * 使用 ServiceLoader 加载所有 @AutoService 注册的实现并按 identifier 缓存
 */
public class DorisFormatLoader {

    private static final Map<String, DorisFormatPlugin> formatPlugins;

    static {
        Map<String, DorisFormatPlugin> plugins = new HashMap<>();
        ServiceLoader<DorisFormatPlugin> loader = ServiceLoader.load(DorisFormatPlugin.class);
        for (DorisFormatPlugin plugin : loader) {
            plugins.put(plugin.identifier(), plugin);
        }
        formatPlugins = plugins;
    }

    /**
     * 根据格式名获取插件，未找到返回 null
     */
    public static DorisFormatPlugin getFormatPlugin(String format) {
        return formatPlugins.get(format);
    }

    /**
     * 列出所有支持的格式
     */
    public static List<String> supportedFormats() {
        return new ArrayList<>(formatPlugins.keySet());
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `mvn -q -pl flink-etl-connector/connector-doris test -Dtest=DorisFormatLoaderTest`
Expected: PASS（4 个测试，依赖 AutoService 生成的 SPI 文件 `META-INF/services/...DorisFormatPlugin`）

> 若失败且报找不到插件：确认 `mvn compile` 已触发 auto-service 注解处理器生成 SPI 文件（与 Kafka 模块同机制）。

- [ ] **Step 6: Commit**

```bash
git add flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/format/JsonFormatPlugin.java flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/format/DorisFormatLoader.java flink-etl-connector/connector-doris/src/test/java/com/etl/connector/doris/sink/format/DorisFormatLoaderTest.java
git commit -m "feat: 新增 JsonFormatPlugin 与 DorisFormatLoader"
```

---

### Task 6: DorisSinkPlugin（接线官方 DorisSink）

**Files:**
- Create: `flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/DorisSinkPlugin.java`

无单元测试（构建官方 Sink，集成性质；按 CLAUDE.md 不测整 job 流程）。由 Task 7 端到端编译 + Task 8 示例验证覆盖。

- [ ] **Step 1: 写实现**

`DorisSinkPlugin.java`：

```java
package com.etl.connector.doris.sink;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.etl.connector.doris.sink.format.DorisFormatLoader;
import com.etl.connector.doris.sink.format.DorisFormatPlugin;
import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

/**
 * Doris Sink 插件
 * 封装官方 flink-doris-connector，通过 Stream Load 写入，at-least-once（batch 模式）
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class DorisSinkPlugin implements SinkPlugin {

    @Override
    public String identifier() {
        return "doris";
    }

    @Override
    public Sink<Row> createSink(SinkConfig config) {
        DorisSinkConfig cfg = DorisSinkConfig.fromSinkConfig(config);

        // SPI 加载序列化格式
        DorisFormatPlugin fmt = DorisFormatLoader.getFormatPlugin(cfg.getFormat());
        if (fmt == null) {
            throw new IllegalArgumentException(
                    "不支持的 format: " + cfg.getFormat()
                            + "，支持: " + DorisFormatLoader.supportedFormats());
        }

        // Doris 连接配置
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(cfg.getFenodes())
                .setTableIdentifier(cfg.getTableIdentifier())
                .setUsername(cfg.getUsername())
                .setPassword(cfg.getPassword())
                .build();

        // 执行配置：batch 模式（at-least-once，非 2PC）+ format 的 Stream Load 属性
        DorisExecutionOptions.Builder execBuilder = DorisExecutionOptions.builder()
                .setBatchMode(true)
                .setStreamLoadProp(fmt.streamLoadProperties());
        if (cfg.getLabelPrefix() != null) {
            execBuilder.setLabelPrefix(cfg.getLabelPrefix());
        }
        if (cfg.getBatchSize() != null) {
            execBuilder.setBufferFlushMaxRows(cfg.getBatchSize());
        }
        DorisExecutionOptions execOptions = execBuilder.build();

        log.info("创建 Doris Sink: fenodes={}, table={}, format={}, labelPrefix={}, batchSize={}",
                cfg.getFenodes(), cfg.getTableIdentifier(), cfg.getFormat(),
                cfg.getLabelPrefix(), cfg.getBatchSize());

        return DorisSink.<Row>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(execOptions)
                .setSerializer(fmt.createSerializer(cfg))
                .build();
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn -q -pl flink-etl-connector/connector-doris compile`
Expected: BUILD SUCCESS

> 若 `DorisExecutionOptions.builder()` 静态方法名报错，改用 `new DorisExecutionOptions.Builder()`（反编译显示 Builder 有公有无参构造）。

- [ ] **Step 3: 跑模块全部测试**

Run: `mvn -q -pl flink-etl-connector/connector-doris test`
Expected: PASS（Config + Serializer + Loader 全绿）

- [ ] **Step 4: Commit**

```bash
git add flink-etl-connector/connector-doris/src/main/java/com/etl/connector/doris/sink/DorisSinkPlugin.java
git commit -m "feat: 新增 DorisSinkPlugin 接线官方 DorisSink"
```

---

### Task 7: 客户端接线

**Files:**
- Modify: `flink-etl-client/pom.xml`（dependencies）

- [ ] **Step 1: 加 connector-doris 依赖**

`flink-etl-client/pom.xml` 的 `<dependencies>` 内，`connector-kafka` 依赖块后加：

```xml
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>connector-doris</artifactId>
            <version>${project.version}</version>
        </dependency>
```

> 参考现有 `connector-jdbc`/`connector-kafka` 依赖块的精确格式（同一 groupId + version 写法）。

- [ ] **Step 2: 全量构建验证 SPI 装配**

Run: `mvn -q clean package -DskipTests`
Expected: BUILD SUCCESS，`connector-doris` 进入 reactor

- [ ] **Step 3: 跑全量测试**

Run: `mvn -q test`
Expected: PASS（含 doris 模块新增测试）

- [ ] **Step 4: Commit**

```bash
git add flink-etl-client/pom.xml
git commit -m "feat: 客户端接入 connector-doris"
```

---

### Task 8: 示例配置 + 文档

**Files:**
- Create: `docs/examples/batch-mock2doris.json`
- Modify: `PLUGINS.md`

- [ ] **Step 1: 写示例配置**

先读一个现有 mock 示例对齐字段结构：`docs/examples/batch-mock2console-fixed.json`。据其 sources 结构填充下方 `<对齐 mock 字段>`，sinks 用 doris：

`docs/examples/batch-mock2doris.json`：

```json
{
  "job": { "name": "batch-mock2doris", "mode": "batch" },
  "sources": [
    {
      "type": "mock",
      "outputTable": "t_src"
    }
  ],
  "sinks": [
    {
      "type": "doris",
      "inputTable": "t_src",
      "fenodes": "127.0.0.1:8030",
      "tableIdentifier": "test_db.test_tbl",
      "username": "root",
      "password": "",
      "format": "json",
      "labelPrefix": "etl-doris",
      "batchSize": 10000
    }
  ]
}
```

> Source 的 mock 字段（schema/rows 等）必须照搬 `batch-mock2console-fixed.json` 的 source 块，确保可运行。

- [ ] **Step 2: 更新 PLUGINS.md**

读 `PLUGINS.md` 的 Sink 章节，按现有 JDBC/Kafka Sink 条目格式新增 Doris Sink：类型 `doris`、配置项表（fenodes/tableIdentifier/username/password 必填，labelPrefix/batchSize/format 可选）、format SPI 说明（当前仅 json，可扩展）、at-least-once 语义说明。

- [ ] **Step 3: Commit**

```bash
git add docs/examples/batch-mock2doris.json PLUGINS.md
git commit -m "docs: 新增 Doris Sink 示例与插件文档"
```

---

## 自审清单（已执行）

**Spec 覆盖：**
- 集成模式（官方 DorisSink）→ Task 6 ✓
- JSON 格式（复用 RowToJsonConverter）→ Task 4 ✓
- at-least-once（batch 模式）→ Task 6 ✓
- 配置（4 必填 + labelPrefix/batchSize/format）→ Task 2 ✓
- format SPI（接口/loader/json 插件）→ Task 3/5 ✓
- 模块结构 → Task 1 ✓
- 客户端接线 → Task 7 ✓
- 示例 + PLUGINS.md → Task 8 ✓
- 单元测试（Config/Serializer/Loader）→ Task 2/4/5 ✓

**占位符扫描：** 无 TODO/TBD。两处「按实际修正」均给出具体回退方案（setter 名、Builder 构造）。

**类型一致性：** `DorisSinkConfig` 字段（fenodes/tableIdentifier/username/password/labelPrefix/batchSize/format）+ getter 跨 Task 2/4/6 一致；`DorisFormatPlugin` 三方法（identifier/createSerializer/streamLoadProperties）跨 Task 3/5/6 一致；`DorisRecord.of(db,table,bytes)`、`DorisRecordSerializer.serialize` 与反编译签名一致。

## 待实现期核对项（已降低风险，仍需注意）

1. `SinkConfig`/`BaseConfig` 的 `get(key, type)`、`get(key, type, default)`、`setProperties(Map)` 精确签名 —— 以 `KafkaSinkConfig`/`KafkaSinkConfigTest` 为准。
2. `DorisExecutionOptions.builder()` vs `new Builder()` —— 回退方案已给。
3. `Row.withNames()` API（Flink 1.15）—— 若不可用，测试改用 `RowToJsonConverter` 实际接受的 Row 构造方式（参考 `RowToJsonConverterTest`）。
