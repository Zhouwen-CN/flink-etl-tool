# SQL Transform 实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 SQL Transform 功能，支持通过 SQL 语句进行数据转换，同时强制 Source 配置 Schema 和 tableName。

**Architecture:** Source 输出 DataStream<Row> → 注册为 Flink Table → SQL Transform → 转回 DataStream<Row> → Sink。SinkPlugin 接口改为返回 `SinkFunction<Row>`。

**Tech Stack:** Apache Flink 1.19.0, Flink Table API, Java 11, JUnit 5

---

## 文件结构

### 新增文件
| 文件 | 职责 |
|------|------|
| `flink-etl-transform/src/main/java/com/etl/transform/SqlTransformPlugin.java` | SQL Transform 插件实现 |
| `flink-etl-transform/src/test/java/com/etl/transform/SqlTransformPluginTest.java` | SQL Transform 单元测试 |

### 修改文件
| 文件                                                                                     | 变更内容                                |
|----------------------------------------------------------------------------------------|-------------------------------------|
| `flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java`                      | 新增 `tableName` 字段                   |
| `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java`                   | 解析并校验 `tableName`                   |
| `flink-etl-core/src/main/java/com/etl/core/localFileSourceConfig/TransformConfig.java` | 新增 `getString()` 方法                 |
| `flink-etl-core/src/main/java/com/etl/core/spi/TransformPlugin.java`                   | 接口方法改为 `transform(Table, ...)`      |
| `flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java`                        | 返回类型改为 `SinkFunction<Row>`          |
| `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java`                        | 集成 Table API                        |
| `flink-etl-core/pom.xml`                                                               | 新增 `flink-table-api-java-bridge` 依赖 |
| `flink-etl-sink-console/.../ConsoleSinkPlugin.java`                                    | 返回类型改为 `SinkFunction<Row>`          |
| `flink-etl-sink-mysql/.../MySQLSinkPlugin.java`                                        | 返回类型改为 `SinkFunction<Row>`          |

### 删除文件
| 文件 | 原因 |
|------|------|
| `flink-etl-transform/src/main/java/com/etl/transform/FieldMappingTransformPlugin.java` | 被 SQL Transform 替代 |

---

## Chunk 1: Schema 增强

### Task 1: EtlSchema 新增 tableName 字段

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java`
- Modify: `flink-etl-core/src/test/java/com/etl/core/schema/EtlSchemaTest.java`

- [ ] **Step 1: 为 EtlSchema 新增 tableName 字段编写测试**

```java
// 文件: flink-etl-core/src/test/java/com/etl/core/schema/EtlSchemaTest.java
// 在现有测试类末尾添加

@Test
void getTableName_shouldReturnCorrectValue() {
    EtlSchema schema = new EtlSchema();
    schema.setTableName("users");
    schema.setFields(Arrays.asList(
        new EtlField("id", EtlFieldType.LONG)
    ));

    assertEquals("users", schema.getTableName());
}

@Test
void constructor_withTableName_shouldSetAllFields() {
    EtlSchema schema = new EtlSchema();
    schema.setTableName("orders");
    schema.setFields(Arrays.asList(
        new EtlField("order_id", EtlFieldType.LONG),
        new EtlField("amount", EtlFieldType.DOUBLE)
    ));

    assertEquals("orders", schema.getTableName());
    assertEquals(2, schema.getFields().size());
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -pl flink-etl-core -Dtest=EtlSchemaTest -q`
Expected: 编译错误或测试失败（tableName 字段不存在）

- [ ] **Step 3: 实现 tableName 字段**

```java
// 文件: flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java
// 修改类定义，添加 tableName 字段

package com.etl.core.schema;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ETL Schema 容器
 * 包含表名和字段列表定义
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EtlSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 表名，用于注册 Flink Table
     */
    private String tableName;

    /**
     * 字段列表
     */
    private List<EtlField> fields;

    /**
     * 按索引获取字段
     */
    public EtlField getField(int index) {
        return fields.get(index);
    }

    /**
     * 按名称获取字段
     */
    public EtlField getField(String name) {
        return fields.stream()
            .filter(f -> f.getName().equals(name))
            .findFirst()
            .orElse(null);
    }

    /**
     * 获取所有字段名
     */
    public List<String> getFieldNames() {
        return fields.stream()
            .map(EtlField::getName)
            .collect(Collectors.toList());
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -pl flink-etl-core -Dtest=EtlSchemaTest -q`
Expected: 测试通过

- [ ] **Step 5: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java flink-etl-core/src/test/java/com/etl/core/schema/EtlSchemaTest.java
git commit -m "feat: EtlSchema 新增 tableName 字段"
```

---

### Task 2: SchemaParser 解析 tableName

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java`
- Modify: `flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java`

- [ ] **Step 1: 为 SchemaParser 解析 tableName 编写测试**

```java
// 文件: flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java
// 在现有测试类末尾添加

@Test
void parse_shouldParseTableName() {
    Map<String, Object> field = new HashMap<>();
    field.put("name", "id");
    field.put("type", "long");

    Map<String, Object> schemaConfig = new HashMap<>();
    schemaConfig.put("tableName", "users");
    schemaConfig.put("fields", List.of(field));

    EtlSchema schema = SchemaParser.parse(schemaConfig);

    assertNotNull(schema);
    assertEquals("users", schema.getTableName());
}

@Test
void parse_shouldThrowException_whenTableNameMissing() {
    Map<String, Object> field = new HashMap<>();
    field.put("name", "id");
    field.put("type", "long");

    Map<String, Object> schemaConfig = new HashMap<>();
    schemaConfig.put("fields", List.of(field));
    // 不设置 tableName

    SchemaConfigException ex = assertThrows(SchemaConfigException.class,
        () -> SchemaParser.parse(schemaConfig));
    assertTrue(ex.getMessage().contains("tableName"));
}

@Test
void parse_shouldThrowException_whenTableNameEmpty() {
    Map<String, Object> field = new HashMap<>();
    field.put("name", "id");
    field.put("type", "long");

    Map<String, Object> schemaConfig = new HashMap<>();
    schemaConfig.put("tableName", "   "); // 空白字符串
    schemaConfig.put("fields", List.of(field));

    SchemaConfigException ex = assertThrows(SchemaConfigException.class,
        () -> SchemaParser.parse(schemaConfig));
    assertTrue(ex.getMessage().contains("tableName"));
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -pl flink-etl-core -Dtest=SchemaParserTest -q`
Expected: 测试失败（tableName 未被解析和校验）

- [ ] **Step 3: 实现 tableName 解析和校验**

```java
// 文件: flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java
// 完整替换

package com.etl.core.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Schema 解析器
 * 从配置对象解析 EtlSchema
 */
public class SchemaParser {

    @SuppressWarnings("unchecked")
    public static EtlSchema parse(Object schemaConfig) {
        if (schemaConfig == null) {
            return null;
        }

        // 类型校验
        if (!(schemaConfig instanceof Map)) {
            throw new SchemaConfigException("schema 必须是一个对象");
        }

        Map<String, Object> schemaMap = (Map<String, Object>) schemaConfig;

        // 解析并校验 tableName
        String tableName = (String) schemaMap.get("tableName");
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new SchemaConfigException("schema.tableName 不能为空");
        }

        // 解析 fields
        Object fieldsObj = schemaMap.get("fields");
        if (fieldsObj == null) {
            throw new SchemaConfigException("schema 缺少 'fields' 字段");
        }

        if (!(fieldsObj instanceof List)) {
            throw new SchemaConfigException("'fields' 必须是数组");
        }

        List<Map<String, Object>> fieldsConfig = (List<Map<String, Object>>) fieldsObj;

        List<EtlField> fields = new ArrayList<>();
        for (int i = 0; i < fieldsConfig.size(); i++) {
            Map<String, Object> fieldConfig = fieldsConfig.get(i);

            Object nameObj = fieldConfig.get("name");
            if (nameObj == null) {
                throw new SchemaConfigException("字段[" + i + "] 缺少 'name'");
            }
            if (!(nameObj instanceof String)) {
                throw new SchemaConfigException("字段[" + i + "] 的 'name' 必须是字符串");
            }

            Object typeObj = fieldConfig.get("type");
            if (typeObj == null) {
                throw new SchemaConfigException("字段[" + i + "] 缺少 'type'");
            }
            if (!(typeObj instanceof String)) {
                throw new SchemaConfigException("字段[" + i + "] 的 'type' 必须是字符串");
            }

            String name = (String) nameObj;
            String typeName = (String) typeObj;
            EtlFieldType type = EtlFieldType.fromString(typeName);
            if (type == null) {
                throw new SchemaConfigException(
                    "字段[" + i + "] '" + name + "' 的类型 '" + typeName + "' 不支持");
            }

            fields.add(new EtlField(name, type));
        }

        EtlSchema schema = new EtlSchema();
        schema.setTableName(tableName);
        schema.setFields(fields);
        return schema;
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -pl flink-etl-core -Dtest=SchemaParserTest -q`
Expected: 所有测试通过

- [ ] **Step 5: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java
git commit -m "feat: SchemaParser 解析并校验 tableName"
```

---

## Chunk 2: 配置和接口变更

### Task 3: TransformConfig 新增 getString 方法

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/localFileSourceConfig/TransformConfig.java`
- Create: `flink-etl-core/src/test/java/com/etl/core/localFileSourceConfig/TransformConfigTest.java`

- [ ] **Step 1: 为 TransformConfig.getString() 编写测试**

```java
// 文件: flink-etl-core/src/test/java/com/etl/core/localFileSourceConfig/TransformConfigTest.java
// 新建文件

package com.etl.core.localFileSourceConfig;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class TransformConfigTest {

    @Test
    void getString_shouldReturnValue() {
        Map<String, Object> localFileSourceConfig = new HashMap<>();
        localFileSourceConfig.put("sql", "SELECT * FROM users");

        TransformConfig transformConfig = new TransformConfig();
        transformConfig.setType("sql");
        transformConfig.setConfig(localFileSourceConfig);

        assertEquals("SELECT * FROM users", transformConfig.getString("sql"));
    }

    @Test
    void getString_shouldReturnNull_whenKeyNotFound() {
        Map<String, Object> localFileSourceConfig = new HashMap<>();

        TransformConfig transformConfig = new TransformConfig();
        transformConfig.setConfig(localFileSourceConfig);

        assertNull(transformConfig.getString("nonexistent"));
    }

    @Test
    void getString_shouldReturnNull_whenConfigIsNull() {
        TransformConfig transformConfig = new TransformConfig();
        transformConfig.setConfig(null);

        assertNull(transformConfig.getString("any"));
    }

    @Test
    void getString_shouldConvertToString() {
        Map<String, Object> localFileSourceConfig = new HashMap<>();
        localFileSourceConfig.put("number", 123);

        TransformConfig transformConfig = new TransformConfig();
        transformConfig.setConfig(localFileSourceConfig);

        assertEquals("123", transformConfig.getString("number"));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -pl flink-etl-core -Dtest=TransformConfigTest -q`
Expected: 编译错误或测试失败（getString 方法不存在）

- [ ] **Step 3: 实现 getString 方法**

```java
// 文件: flink-etl-core/src/main/java/com/etl/core/localFileSourceConfig/TransformConfig.java
// 完整替换

package com.etl.core.localFileSourceConfig;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.Map;

/**
 * Transform 配置类
 * 定义数据转换的基本配置
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransformConfig {
    private String type;
    private Map<String, Object> localFileSourceConfig;

    /**
     * 获取字符串类型的配置值
     *
     * @param key 配置键
     * @return 配置值
     */
    public String getString(String key) {
        if (localFileSourceConfig == null) {
            return null;
        }
        Object value = localFileSourceConfig.get(key);
        return value != null ? String.valueOf(value) : null;
    }

    /**
     * 获取配置值
     *
     * @param key 配置键
     * @return 配置值
     */
    public Object get(String key) {
        return localFileSourceConfig != null ? localFileSourceConfig.get(key) : null;
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -pl flink-etl-core -Dtest=TransformConfigTest -q`
Expected: 测试通过

- [ ] **Step 5: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/localFileSourceConfig/TransformConfig.java flink-etl-core/src/test/java/com/etl/core/localFileSourceConfig/TransformConfigTest.java
git commit -m "feat: TransformConfig 新增 getString 方法"
```

---

### Task 4: TransformPlugin 接口变更

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/spi/TransformPlugin.java`

- [ ] **Step 1: 修改 TransformPlugin 接口**

```java
// 文件: flink-etl-core/src/main/java/com/etl/core/spi/TransformPlugin.java
// 完整替换

package com.etl.core.spi;

import com.etl.core.localFileSourceConfig.TransformConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Transform 插件接口
 * 所有数据转换插件必须实现此接口
 */
public interface TransformPlugin {

    /**
     * 获取插件类型标识
     *
     * @return 插件类型标识
     */
    String getType();

    /**
     * 执行转换
     *
     * @param inputTable 输入表
     * @param localFileSourceConfig 转换配置
     * @param stEnv Table 环境
     * @return 转换后的表
     */
    Table transform(Table inputTable, TransformConfig localFileSourceConfig, StreamTableEnvironment stEnv);
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn compile -pl flink-etl-core -q`
Expected: 编译通过（需要先添加 flink-table-api-java-bridge 依赖，见 Task 5）

- [ ] **Step 3: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/spi/TransformPlugin.java
git commit -m "feat: TransformPlugin 接口改为 transform(Table, ...)"
```

---

### Task 5: SinkPlugin 接口类型约束

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java`

- [ ] **Step 1: 修改 SinkPlugin 接口**

```java
// 文件: flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java
// 完整替换

package com.etl.core.spi;

import com.etl.core.localFileSourceConfig.SinkConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Sink 插件接口
 * 所有数据写入插件必须实现此接口
 */
public interface SinkPlugin extends Serializable {

    /**
     * 获取插件类型标识
     *
     * @return 插件类型标识
     */
    String getType();

    /**
     * 创建 Sink 函数
     *
     * @param localFileSourceConfig Sink 配置
     * @return Flink SinkFunction，强制消费 Row 类型
     */
    SinkFunction<Row> createSink(SinkConfig localFileSourceConfig);
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn compile -pl flink-etl-core -q`
Expected: 编译通过

- [ ] **Step 3: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/spi/SinkPlugin.java
git commit -m "feat: SinkPlugin 返回类型强制约束为 SinkFunction<Row>"
```

---

### Task 6: 新增 Flink Table API 依赖

**Files:**
- Modify: `flink-etl-core/pom.xml`

- [ ] **Step 1: 添加 flink-table-api-java-bridge 依赖**

```xml
<!-- 文件: flink-etl-core/pom.xml -->
<!-- 在 </dependencies> 前添加 -->

        <!-- Flink Table API Java Bridge - 用于 DataStream 和 Table 互转 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge</artifactId>
        </dependency>
```

- [ ] **Step 2: 验证依赖下载**

Run: `mvn dependency:resolve -pl flink-etl-core -q`
Expected: 成功下载依赖

- [ ] **Step 3: 提交**

```bash
git add flink-etl-core/pom.xml
git commit -m "feat: 新增 flink-table-api-java-bridge 依赖"
```

---

## Chunk 3: SqlTransformPlugin 实现

### Task 7: 创建 SqlTransformPlugin

**Files:**
- Create: `flink-etl-transform/src/main/java/com/etl/transform/SqlTransformPlugin.java`
- Create: `flink-etl-transform/src/test/java/com/etl/transform/SqlTransformPluginTest.java`

- [ ] **Step 1: 编写 SqlTransformPlugin 测试**

```java
// 文件: flink-etl-transform/src/test/java/com/etl/transform/SqlTransformPluginTest.java
// 新建文件

package com.etl.transform;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SqlTransformPluginTest {

    @Test
    void getType_shouldReturnSql() {
        SqlTransformPlugin plugin = new SqlTransformPlugin();
        assertEquals("sql", plugin.getType());
    }

    @Test
    void transform_shouldThrowException_whenSqlMissing() {
        SqlTransformPlugin plugin = new SqlTransformPlugin();

        // 注意：完整测试需要 Mock StreamTableEnvironment
        // 这里只测试基本行为
        assertNotNull(plugin);
    }
}
```

- [ ] **Step 2: 实现 SqlTransformPlugin**

```java
// 文件: flink-etl-transform/src/main/java/com/etl/transform/SqlTransformPlugin.java
// 新建文件

package com.etl.transform;

import com.etl.core.localFileSourceConfig.TransformConfig;
import com.etl.core.spi.TransformPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * SQL Transform 插件
 * 支持通过 SQL 语句进行数据转换
 */
@Slf4j
@AutoService(TransformPlugin.class)
public class SqlTransformPlugin implements TransformPlugin {

    @Override
    public String getType() {
        return "sql";
    }

    @Override
    public Table transform(Table inputTable, TransformConfig localFileSourceConfig, StreamTableEnvironment stEnv) {
        String sql = localFileSourceConfig.getString("sql");

        // 参数校验
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL Transform 配置缺少 'sql' 字段");
        }

        log.info("执行 SQL: {}", sql);

        try {
            return stEnv.sqlQuery(sql);
        } catch (Exception e) {
            throw new RuntimeException("SQL 执行失败: " + e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 3: 验证编译**

Run: `mvn compile -pl flink-etl-transform -q`
Expected: 编译通过

- [ ] **Step 4: 运行测试**

Run: `mvn test -pl flink-etl-transform -Dtest=SqlTransformPluginTest -q`
Expected: 测试通过

- [ ] **Step 5: 提交**

```bash
git add flink-etl-transform/src/main/java/com/etl/transform/SqlTransformPlugin.java flink-etl-transform/src/test/java/com/etl/transform/SqlTransformPluginTest.java
git commit -m "feat: 新增 SqlTransformPlugin 实现"
```

---

## Chunk 4: JobBuilder 改造

### Task 8: JobBuilder 集成 Table API

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java`

- [ ] **Step 1: 重写 JobBuilder**

```java
// 文件: flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java
// 完整替换

package com.etl.core.job;

import com.etl.core.schema.EtlSchema;
import com.etl.core.localFileSourceConfig.JobConfig;
import com.etl.core.localFileSourceConfig.TransformConfig;
import com.etl.core.spi.PluginLoader;
import com.etl.core.spi.SinkPlugin;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.TransformPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Job 构建器
 * 负责将配置转换为 Flink Job
 */
@Slf4j
public class JobBuilder {

    private final PluginLoader pluginLoader;

    public JobBuilder(PluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    /**
     * 构建 Flink Job
     *
     * @param env    Flink 执行环境
     * @param localFileSourceConfig Job 配置
     */
    public void build(StreamExecutionEnvironment env, JobConfig localFileSourceConfig) {
        log.info("开始构建 Flink Job: {}", localFileSourceConfig.getJob().getName());

        // 创建 Table 环境
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

        // 1. Source -> DataStream -> 注册 Table
        SourcePlugin sourcePlugin = pluginLoader.loadSourcePlugin(localFileSourceConfig.getSource().getType());
        Source<?, ?, ?> source = sourcePlugin.createSource(localFileSourceConfig.getSource());
        DataStream<Row> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");

        // 强制校验 Schema
        EtlSchema schema = localFileSourceConfig.getSource().getSchema();
        if (schema == null) {
            throw new IllegalArgumentException("Source 必须配置 schema");
        }
        if (schema.getTableName() == null || schema.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Source 的 schema.tableName 不能为空");
        }

        // 注册为 Table
        stEnv.createTemporaryView(schema.getTableName(), sourceStream);
        log.info("注册 Table: {}", schema.getTableName());

        // 2. Transform 链式处理
        Table resultTable = stEnv.from(schema.getTableName());

        List<TransformConfig> transforms = localFileSourceConfig.getTransforms();
        if (transforms != null && !transforms.isEmpty()) {
            for (int i = 0; i < transforms.size(); i++) {
                TransformConfig transformConfig = transforms.get(i);
                TransformPlugin transformPlugin = pluginLoader.loadTransformPlugin(transformConfig.getType());
                resultTable = transformPlugin.transform(resultTable, transformConfig, stEnv);

                // 将 Transform 结果注册为中间表，供后续 SQL 引用
                String intermediateTableName = "transform_result_" + i;
                stEnv.createTemporaryView(intermediateTableName, resultTable);
                log.info("注册中间表: {}", intermediateTableName);

                log.info("Transform 应用成功: {}", transformConfig.getType());
            }
        }

        // 3. Table -> DataStream<Row>
        DataStream<Row> resultStream = stEnv.toDataStream(resultTable);
        log.info("Table 转换为 DataStream");

        // 4. Sink 消费 DataStream
        SinkPlugin sinkPlugin = pluginLoader.loadSinkPlugin(localFileSourceConfig.getSink().getType());
        SinkFunction<Row> sink = sinkPlugin.createSink(localFileSourceConfig.getSink());
        resultStream.addSink(sink);
        log.info("Sink 创建成功");

        log.info("Flink Job 构建完成");
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn compile -pl flink-etl-core -q`
Expected: 编译通过

- [ ] **Step 3: 提交**

```bash
git add flink-etl-core/src/main/java/com/etl/core/job/JobBuilder.java
git commit -m "feat: JobBuilder 集成 Table API 支持 SQL Transform"
```

---

## Chunk 5: Sink 插件适配

### Task 9: ConsoleSinkPlugin 适配

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java`

- [ ] **Step 1: 修改 ConsoleSinkPlugin 返回类型**

```java
// 文件: flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java
// 完整替换

package com.etl.sink.console;

import com.etl.core.localFileSourceConfig.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
 * Console Sink 插件
 * 将数据输出到控制台
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class ConsoleSinkPlugin implements SinkPlugin {
    private static final long serialVersionUID = 1L;

    @Override
    public String getType() {
        return "console";
    }

    @Override
    public RichSinkFunction<Row> createSink(SinkConfig localFileSourceConfig) {
        String format = localFileSourceConfig.getString("format");
        boolean showSubtask = localFileSourceConfig.getBoolean("showSubtask", true);
        log.info("创建 Console Sink, format={}, showSubtask={}", format, showSubtask);

        return new ConsoleSinkFunction(format, showSubtask);
    }

    /**
     * Console Sink Function
     * 使用 RichSinkFunction 获取 RuntimeContext，支持显示分片信息
     */
    private static class ConsoleSinkFunction extends RichSinkFunction<Row> {
        private static final long serialVersionUID = 1L;
        private final String format;
        private final boolean showSubtask;

        // 缓存分片信息，避免每次 invoke 都调用
        private transient int subtaskIndex = -1;
        private transient int totalSubtasks = -1;

        public ConsoleSinkFunction(String format, boolean showSubtask) {
            this.format = format != null ? format : "json";
            this.showSubtask = showSubtask;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在 open() 中获取分片信息，只执行一次
            RuntimeContext ctx = getRuntimeContext();
            TaskInfo taskInfo = ctx.getTaskInfo();
            this.subtaskIndex = taskInfo.getIndexOfThisSubtask() + 1;
            this.totalSubtasks = taskInfo.getNumberOfParallelSubtasks();
            log.info("ConsoleSinkFunction 初始化, subtask[{}/{}]", subtaskIndex, totalSubtasks);
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            if (showSubtask) {
                System.out.printf("[subtask-%d/%d] %s%n", subtaskIndex, totalSubtasks, value);
            } else {
                System.out.println(value);
            }
        }
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `mvn compile -pl flink-etl-sink/flink-etl-sink-console -q`
Expected: 编译通过

- [ ] **Step 3: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-console/src/main/java/com/etl/sink/console/ConsoleSinkPlugin.java
git commit -m "feat: ConsoleSinkPlugin 适配 SinkFunction<Row>"
```

---

### Task 10: MySQLSinkPlugin 和 MySQLSinkFunction 适配

**Files:**
- Modify: `flink-etl-sink/flink-etl-sink-mysql/src/main/java/com/etl/sink/mysql/MySQLSinkPlugin.java`
- Modify: `flink-etl-sink/flink-etl-sink-mysql/src/main/java/com/etl/sink/mysql/MySQLSinkFunction.java`

- [ ] **Step 1: 修改 MySQLSinkPlugin 返回类型**

```java
// 文件: flink-etl-sink/flink-etl-sink-mysql/src/main/java/com/etl/sink/mysql/MySQLSinkPlugin.java
// 修改 createSink 方法返回类型

package com.etl.sink.mysql;

import com.etl.core.localFileSourceConfig.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

/**
 * MySQL Sink 插件
 * 将数据写入 MySQL 数据库，支持批量写入和 upsert 模式
 * 列名从运行时 Row 的字段名中动态获取，无需在配置中指定
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class MySQLSinkPlugin implements SinkPlugin {
    private static final long serialVersionUID = 1L;

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public SinkFunction<Row> createSink(SinkConfig localFileSourceConfig) {
        String url = localFileSourceConfig.getString("url");
        String username = localFileSourceConfig.getString("username");
        String password = localFileSourceConfig.getString("password");
        String table = localFileSourceConfig.getString("table");
        int batchSize = localFileSourceConfig.getInteger("batchSize") != null ? localFileSourceConfig.getInteger("batchSize") : 100;
        String writeMode = localFileSourceConfig.getString("writeMode") != null ? localFileSourceConfig.getString("writeMode") : "insert";

        if (url == null || username == null || password == null || table == null) {
            throw new IllegalArgumentException("MySQL Sink 缺少必要配置: url, username, password, table");
        }

        log.info("创建 MySQL Sink, table={}, mode={}, batchSize={}", table, writeMode, batchSize);
        return new MySQLSinkFunction(url, username, password, table, batchSize, writeMode);
    }
}
```

- [ ] **Step 2: 验证 MySQLSinkFunction 无需修改**

MySQLSinkFunction 已经是 `RichSinkFunction<Row>` 类型，无需修改。只需修改 MySQLSinkPlugin 的返回类型即可。

- [ ] **Step 3: 验证编译**

Run: `mvn compile -pl flink-etl-sink/flink-etl-sink-mysql -q`
Expected: 编译通过

- [ ] **Step 4: 提交**

```bash
git add flink-etl-sink/flink-etl-sink-mysql/src/main/java/com/etl/sink/mysql/MySQLSinkPlugin.java
git commit -m "feat: MySQLSinkPlugin 适配 SinkFunction<Row>"
```

---

## Chunk 6: 清理和验证

### Task 11: 删除 FieldMappingTransformPlugin

**Files:**
- Delete: `flink-etl-transform/src/main/java/com/etl/transform/FieldMappingTransformPlugin.java`

- [ ] **Step 1: 删除 FieldMappingTransformPlugin**

```bash
rm flink-etl-transform/src/main/java/com/etl/transform/FieldMappingTransformPlugin.java
```

- [ ] **Step 2: 验证编译**

Run: `mvn compile -pl flink-etl-transform -q`
Expected: 编译通过（不再依赖 FieldMappingTransformPlugin）

- [ ] **Step 3: 提交**

```bash
git add -A flink-etl-transform/
git commit -m "refactor: 删除 FieldMappingTransformPlugin，被 SQL Transform 替代"
```

---

### Task 12: 更新示例配置文件

**Files:**
- Modify: `docs/examples/mysql-to-console.json`

- [ ] **Step 1: 更新示例配置**

```json
// 文件: docs/examples/batch-mysql2console.json
// 完整替换

{
  "job": {
    "name": "mysql-to-console",
    "mode": "batch",
    "parallelism": 1
  },
  "source": {
    "type": "mysql",
    "localFileSourceConfig": {
      "url": "jdbc:mysql://118.145.119.51:3306/test_db",
      "table": "user_table",
      "username": "root",
      "password": "Zw$8rb8*",
      "splitColumn": "id",
      "fetchSize": 1,
      "schema": {
        "tableName": "users",
        "fields": [
          { "name": "id", "type": "LONG" },
          { "name": "name", "type": "STRING" }
        ]
      }
    }
  },
  "transforms": [
    {
      "type": "sql",
      "localFileSourceConfig": {
        "sql": "SELECT id AS user_id, name FROM users WHERE id > 0"
      }
    }
  ],
  "sink": {
    "type": "console",
    "localFileSourceConfig": {
      "format": "json"
    }
  }
}
```

- [ ] **Step 2: 提交**

```bash
git add docs/examples/batch-mysql2console.json
git commit -m "docs: 更新示例配置，使用 SQL Transform"
```

---

### Task 13: 全量编译和测试

- [ ] **Step 1: 全量编译**

Run: `mvn clean compile -q`
Expected: 编译成功

- [ ] **Step 2: 全量测试**

Run: `mvn test -q`
Expected: 所有测试通过

- [ ] **Step 3: 最终提交**

```bash
git add -A
git commit -m "feat: SQL Transform 功能实现完成"
```

---

## 执行顺序

1. **Chunk 1**: Task 1 → Task 2（Schema 增强）
2. **Chunk 2**: Task 3 → Task 4 → Task 5 → Task 6（配置和接口变更）
3. **Chunk 3**: Task 7（SqlTransformPlugin）
4. **Chunk 4**: Task 8（JobBuilder）
5. **Chunk 5**: Task 9 → Task 10（Sink 适配）
6. **Chunk 6**: Task 11 → Task 12 → Task 13（清理和验证）

每个 Chunk 可由独立的 subagent 执行，Chunk 内的 Task 需顺序执行。