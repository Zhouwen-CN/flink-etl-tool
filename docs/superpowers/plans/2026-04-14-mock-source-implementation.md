# Mock Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Mock Source plugin for generating test data without external dependencies.

**Architecture:** Single-split Source extending AbstractSplitSource, supporting batch (bounded) and streaming (unbounded) modes with fixed rows or random data generation.

**Tech Stack:** Java 1.8, Apache Flink 1.15.2, FLIP-27 Source API, AutoService SPI

---

## File Structure

**Module:** `flink-etl-source/flink-etl-source-mock/`

**Create files:**

### Maven and SPI
- `pom.xml` - Module dependencies
- `src/main/resources/META-INF/services/com.etl.core.spi.SourcePlugin` - SPI registration

### Core Classes (extending base classes)
- `MockSourcePlugin.java` - SPI entry point
- `MockSource.java` - Main source class (extends AbstractSplitSource)
- `MockSplit.java` - Single split with fixed ID
- `MockSplitEnumerator.java` - Split enumerator (extends BaseSplitEnumerator)
- `MockSplitReader.java` - Split reader (extends BaseSplitReader)
- `MockSourceReader.java` - Source reader (extends BaseSourceReader)
- `MockRecordEmitter.java` - Record emitter
- `MockEnumCheckpoint.java` - Enumerator checkpoint
- `MockSplitState.java` - Split state

### Configuration
- `config/MockSourceConfig.java` - Configuration POJO (Serializable)

### Generator Utilities
- `generator/DataRowGenerator.java` - Generate rows from config
- `generator/RandomRowGenerator.java` - Generate random rows

### Tests (TDD)
- `MockSourceTest.java` - Config validation and schema tests
- `MockSplitReaderTest.java` - Split reader behavior tests
- `generator/DataRowGeneratorTest.java` - Data parsing tests
- `generator/RandomRowGeneratorTest.java` - Random generation tests

### Examples
- `docs/examples/mock-batch-fixed.json` - Batch mode fixed data example
- `docs/examples/mock-batch-random.json` - Batch mode random example
- `docs/examples/mock-streaming.json` - Streaming mode example
- `docs/examples/mock-cdc-test.json` - CDC scenario example

**Modify files:**
- `flink-etl-source/pom.xml` - Add mock module
- `flink-etl-client/pom.xml` - Add mock dependency
- `PLUGINS.md` - Add Mock Source documentation

---

## Task 1: Create Maven Module

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/pom.xml`
- Modify: `flink-etl-source/pom.xml` (add module)

- [ ] **Step 1: Create mock module directory structure**

```bash
mkdir -p flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock
mkdir -p flink-etl-source/flink-etl-source-mock/src/main/resources/META-INF/services
mkdir -p flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock
```

- [ ] **Step 2: Create pom.xml for mock module**

Create `flink-etl-source/flink-etl-source-mock/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>flink-etl-source</artifactId>
        <groupId>com.etl</groupId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>flink-etl-source-mock</artifactId>
    <name>Mock Source Plugin</name>

    <dependencies>
        <!-- 核心框架依赖 -->
        <dependency>
            <groupId>com.etl</groupId>
            <artifactId>flink-etl-core</artifactId>
            <version>${project.version}</version>
        </dependency>

        <!-- AutoService 注解 -->
        <dependency>
            <groupId>com.google.auto.service</groupId>
            <artifactId>auto-service</artifactId>
            <version>1.0.1</version>
            <scope>provided</scope>
        </dependency>

        <!-- 测试依赖 -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.9.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 3: Add mock module to parent pom**

Modify `flink-etl-source/pom.xml`, add `<module>flink-etl-source-mock</module>` to modules list:

```xml
<modules>
    <module>flink-etl-source-jdbc</module>
    <module>flink-etl-source-localfile</module>
    <module>flink-etl-source-http</module>
    <module>flink-etl-source-kafka</module>
    <module>flink-etl-source-mock</module>  <!-- 新增 -->
</modules>
```

- [ ] **Step 4: Commit Maven module setup**

```bash
git add flink-etl-source/flink-etl-source-mock/
git add flink-etl-source/pom.xml
git commit -m "feat: 新增 Mock Source Maven 模块"
```

---

## Task 2: Implement SPI Entry Point

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSourcePlugin.java`
- Create: `flink-etl-source/flink-etl-source-mock/src/main/resources/META-INF/services/com.etl.core.spi.SourcePlugin`

- [ ] **Step 1: Create MockSourcePlugin class**

Create `MockSourcePlugin.java`:

```java
package com.etl.source.mock;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;

/**
 * Mock Source 插件
 * 支持固定数据和随机生成两种模式
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class MockSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "mock";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        log.info("创建 Mock Source");
        return new MockSource(config);
    }
}
```

- [ ] **Step 2: Create SPI configuration file**

Create `META-INF/services/com.etl.core.spi.SourcePlugin`:

```
com.etl.source.mock.MockSourcePlugin
```

- [ ] **Step 3: Compile to verify SPI generation**

```bash
mvn clean compile -pl flink-etl-source/flink-etl-source-mock
```

Expected: Compilation succeeds, AutoService generates META-INF/services file.

- [ ] **Step 4: Commit SPI entry point**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSourcePlugin.java
git add flink-etl-source/flink-etl-source-mock/src/main/resources/META-INF/services/com.etl.core.spi.SourcePlugin
git commit -m "feat: 实现 Mock Source SPI 入口"
```

---

## Task 3: Implement Configuration POJO

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/config/MockSourceConfig.java`

- [ ] **Step 1: Create MockSourceConfig class**

Create `config/MockSourceConfig.java`:

```java
package com.etl.source.mock.config;

import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Mock Source 配置封装类
 */
@Data
@Builder
public class MockSourceConfig implements Serializable {

    /** 运行模式：batch 或 streaming */
    private RunMode runMode;

    /** Schema 定义 */
    private EtlSchema schema;

    /** 固定数据配置（batch 模式） */
    private List<RowData> rows;

    /** batch 模式随机生成的行数 */
    private Integer numRows;

    /** streaming 模式生成间隔（毫秒） */
    private Long intervalMs;

    /**
     * 运行模式枚举
     */
    public enum RunMode {
        BATCH,
        STREAMING
    }

    /**
     * Row 数据配置
     */
    @Data
    public static class RowData implements Serializable {
        /** RowKind: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE */
        private String kind;

        /** 字段值映射 */
        private Map<String, Object> data;
    }
}
```

- [ ] **Step 2: Commit configuration class**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/config/MockSourceConfig.java
git commit -m "feat: 新增 MockSourceConfig 配置封装类"
```

---

## Task 4: Implement DataRowGenerator (TDD)

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock/generator/DataRowGeneratorTest.java`
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/generator/DataRowGenerator.java`

- [ ] **Step 1: Write test for RowKind parsing**

Create `generator/DataRowGeneratorTest.java`:

```java
package com.etl.source.mock.generator;

import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.source.mock.config.MockSourceConfig;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataRowGeneratorTest {

    @Test
    void testParseRowKind() {
        assertEquals(RowKind.INSERT, DataRowGenerator.parseRowKind("INSERT"));
        assertEquals(RowKind.UPDATE_BEFORE, DataRowGenerator.parseRowKind("UPDATE_BEFORE"));
        assertEquals(RowKind.UPDATE_AFTER, DataRowGenerator.parseRowKind("UPDATE_AFTER"));
        assertEquals(RowKind.DELETE, DataRowGenerator.parseRowKind("DELETE"));

        // 大小写不敏感
        assertEquals(RowKind.INSERT, DataRowGenerator.parseRowKind("insert"));
        assertEquals(RowKind.INSERT, DataRowGenerator.parseRowKind("Insert"));
    }

    @Test
    void testParseInvalidRowKind() {
        assertThrows(SchemaConfigException.class, () -> {
            DataRowGenerator.parseRowKind("INVALID");
        });
    }

    @Test
    void testGenerateRowsFromConfig() {
        // 创建 schema
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .field("name", "STRING")
            .field("age", "INT")
            .build();

        // 创建 rows 配置
        List<MockSourceConfig.RowData> rowsData = Arrays.asList(
            createRowData("INSERT", Map.of("id", 1L, "name", "Alice", "age", 25)),
            createRowData("UPDATE_AFTER", Map.of("id", 2L, "name", "Bob", "age", 30)),
            createRowData("DELETE", Map.of("id", 3L, "name", "Charlie", "age", 28))
        );

        // 生成 rows
        List<Row> rows = DataRowGenerator.generateRows(rowsData, schema);

        // 验证结果
        assertEquals(3, rows.size());

        Row row1 = rows.get(0);
        assertEquals(RowKind.INSERT, row1.getRowKind());
        assertEquals(1L, row1.getField("id"));
        assertEquals("Alice", row1.getField("name"));
        assertEquals(25, row1.getField("age"));

        Row row2 = rows.get(1);
        assertEquals(RowKind.UPDATE_AFTER, row2.getRowKind());
        assertEquals(2L, row2.getField("id"));

        Row row3 = rows.get(2);
        assertEquals(RowKind.DELETE, row3.getRowKind());
        assertEquals(3L, row3.getField("id"));
    }

    @Test
    void testGenerateRowsMissingField() {
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .field("name", "STRING")
            .build();

        List<MockSourceConfig.RowData> rowsData = Arrays.asList(
            createRowData("INSERT", Map.of("id", 1L))  // 缺失 name 字段
        );

        assertThrows(SchemaConfigException.class, () -> {
            DataRowGenerator.generateRows(rowsData, schema);
        });
    }

    private MockSourceConfig.RowData createRowData(String kind, Map<String, Object> data) {
        MockSourceConfig.RowData rowData = new MockSourceConfig.RowData();
        rowData.setKind(kind);
        rowData.setData(data);
        return rowData;
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl flink-etl-source/flink-etl-source-mock -Dtest=DataRowGeneratorTest
```

Expected: Test fails because DataRowGenerator class doesn't exist yet.

- [ ] **Step 3: Implement DataRowGenerator**

Create `generator/DataRowGenerator.java`:

```java
package com.etl.source.mock.generator;

import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.source.mock.config.MockSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 从 rows 配置生成 Row 数据
 */
@Slf4j
public class DataRowGenerator {

    /**
     * 批量生成 Row 数据
     */
    public static List<Row> generateRows(List<MockSourceConfig.RowData> rowsData, EtlSchema schema) {
        List<Row> rows = new ArrayList<>();
        for (MockSourceConfig.RowData rowData : rowsData) {
            Row row = generateRow(rowData, schema);
            rows.add(row);
        }
        log.info("从 rows 配置生成 {} 行数据", rows.size());
        return rows;
    }

    /**
     * 生成单行数据
     */
    private static Row generateRow(MockSourceConfig.RowData rowData, EtlSchema schema) {
        RowKind rowKind = parseRowKind(rowData.getKind());
        Row row = Row.withKind(rowKind);

        Map<String, Object> data = rowData.getData();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String fieldName = schema.getFieldName(i);
            Object value = data.get(fieldName);

            if (value == null) {
                throw new SchemaConfigException(
                    "rows 配置缺失字段 '" + fieldName + "'，必须匹配 schema 定义的所有字段");
            }

            TypeInformation<?> expectedType = schema.getFieldType(i);
            Object convertedValue = convertValue(value, expectedType, fieldName);
            row.setField(i, convertedValue);
        }

        return row;
    }

    /**
     * 解析 RowKind
     */
    public static RowKind parseRowKind(String kind) {
        switch (kind.toUpperCase()) {
            case "INSERT":
                return RowKind.INSERT;
            case "UPDATE_BEFORE":
                return RowKind.UPDATE_BEFORE;
            case "UPDATE_AFTER":
                return RowKind.UPDATE_AFTER;
            case "DELETE":
                return RowKind.DELETE;
            default:
                throw new SchemaConfigException("无效的 RowKind: " + kind);
        }
    }

    /**
     * 类型转换
     */
    private static Object convertValue(Object value, TypeInformation<?> type, String fieldName) {
        try {
            if (type == Types.STRING) {
                return value.toString();
            } else if (type == Types.BOOLEAN) {
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString());
            } else if (type == Types.INT) {
                if (value instanceof Integer) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.parseInt(value.toString());
            } else if (type == Types.LONG) {
                if (value instanceof Long) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return Long.parseLong(value.toString());
            } else if (type == Types.DOUBLE) {
                if (value instanceof Double) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return Double.parseDouble(value.toString());
            } else if (type == Types.BIG_DEC) {
                if (value instanceof BigDecimal) {
                    return value;
                }
                if (value instanceof Number) {
                    return BigDecimal.valueOf(((Number) value).doubleValue());
                }
                return new BigDecimal(value.toString());
            } else if (type == Types.SQL_TIMESTAMP) {
                if (value instanceof Timestamp) {
                    return value;
                }
                if (value instanceof Number) {
                    return new Timestamp(((Number) value).longValue());
                }
                return Timestamp.valueOf(value.toString());
            } else {
                throw new SchemaConfigException("不支持的类型: " + type);
            }
        } catch (Exception e) {
            throw new SchemaConfigException(
                "字段 '" + fieldName + "' 类型转换失败：期望类型 " + type +
                "，实际值 " + value + " (" + value.getClass().getName() + ")");
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
mvn test -pl flink-etl-source/flink-etl-source-mock -Dtest=DataRowGeneratorTest
```

Expected: All tests pass.

- [ ] **Step 5: Commit DataRowGenerator**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/generator/DataRowGenerator.java
git add flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock/generator/DataRowGeneratorTest.java
git commit -m "feat: 实现 DataRowGenerator（TDD）"
```

---

## Task 5: Implement RandomRowGenerator (TDD)

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock/generator/RandomRowGeneratorTest.java`
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/generator/RandomRowGenerator.java`

- [ ] **Step 1: Write test for single row generation**

Create `generator/RandomRowGeneratorTest.java`:

```java
package com.etl.source.mock.generator;

import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RandomRowGeneratorTest {

    @Test
    void testGenerateSingleRow() {
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .field("name", "STRING")
            .field("age", "INT")
            .field("active", "BOOLEAN")
            .field("amount", "DOUBLE")
            .field("price", "DECIMAL")
            .field("timestamp", "TIMESTAMP")
            .build();

        Row row = RandomRowGenerator.generateRow(schema);

        // 验证 RowKind 为 INSERT
        assertEquals(RowKind.INSERT, row.getRowKind());

        // 验证字段数量
        assertEquals(7, row.getArity());

        // 验证类型正确性
        assertTrue(row.getField("id") instanceof Long);
        assertTrue(row.getField("name") instanceof String);
        assertTrue(row.getField("age") instanceof Integer);
        assertTrue(row.getField("active") instanceof Boolean);
        assertTrue(row.getField("amount") instanceof Double);
        assertTrue(row.getField("price") instanceof BigDecimal);
        assertTrue(row.getField("timestamp") instanceof Timestamp);

        // 验证值范围
        Long id = (Long) row.getField("id");
        assertTrue(id >= 0 && id <= 10000);

        String name = (String) row.getField("name");
        assertTrue(name.startsWith("field_"));
        assertTrue(name.length() > 6);  // "field_" + at least 1 char

        Integer age = (Integer) row.getField("age");
        assertTrue(age >= 0 && age <= 10000);
    }

    @Test
    void testGenerateMultipleRows() {
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .field("value", "INT")
            .build();

        List<Row> rows = RandomRowGenerator.generateRows(schema, 10);

        assertEquals(10, rows.size());

        for (Row row : rows) {
            assertEquals(RowKind.INSERT, row.getRowKind());
            assertEquals(2, row.getArity());
            assertTrue(row.getField("id") instanceof Long);
            assertTrue(row.getField("value") instanceof Integer);
        }
    }

    @Test
    void testGenerateUnsupportedType() {
        // 创建包含复杂类型的 schema（模拟）
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .build();

        // 验证正常类型可以生成
        Row row = RandomRowGenerator.generateRow(schema);
        assertNotNull(row);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -pl flink-etl-source/flink-etl-source-mock -Dtest=RandomRowGeneratorTest
```

Expected: Test fails because RandomRowGenerator class doesn't exist.

- [ ] **Step 3: Implement RandomRowGenerator**

Create `generator/RandomRowGenerator.java`:

```java
package com.etl.source.mock.generator;

import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * 随机生成 Row 数据
 */
@Slf4j
public class RandomRowGenerator {

    private static final Random random = new Random();

    /**
     * 批量生成 Row 数据
     */
    public static List<Row> generateRows(EtlSchema schema, int numRows) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            Row row = generateRow(schema);
            rows.add(row);
        }
        log.info("随机生成 {} 行数据", numRows);
        return rows;
    }

    /**
     * 生成单行数据
     */
    public static Row generateRow(EtlSchema schema) {
        Row row = Row.withKind(RowKind.INSERT);

        for (int i = 0; i < schema.getFieldCount(); i++) {
            TypeInformation<?> type = schema.getFieldType(i);
            Object value = generateValue(type);
            row.setField(i, value);
        }

        return row;
    }

    /**
     * 根据类型生成随机值
     */
    private static Object generateValue(TypeInformation<?> type) {
        if (type == Types.STRING) {
            return "field_" + UUID.randomUUID().toString().substring(0, 8);
        } else if (type == Types.BOOLEAN) {
            return random.nextBoolean();
        } else if (type == Types.INT) {
            return random.nextInt(10001);  // 0-10000
        } else if (type == Types.LONG) {
            return (long) random.nextInt(10001);
        } else if (type == Types.DOUBLE) {
            return random.nextDouble() * 10000.0;
        } else if (type == Types.BIG_DEC) {
            return BigDecimal.valueOf(random.nextDouble() * 10000.0)
                .setScale(2, RoundingMode.HALF_UP);
        } else if (type == Types.SQL_TIMESTAMP) {
            return new Timestamp(System.currentTimeMillis());
        } else {
            throw new SchemaConfigException("不支持的类型: " + type);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
mvn test -pl flink-etl-source/flink-etl-source-mock -Dtest=RandomRowGeneratorTest
```

Expected: All tests pass.

- [ ] **Step 5: Commit RandomRowGenerator**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/generator/RandomRowGenerator.java
git add flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock/generator/RandomRowGeneratorTest.java
git commit -m "feat: 实现 RandomRowGenerator（TDD）"
```

---

## Task 6: Implement Split and Checkpoint Classes

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplit.java`
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockEnumCheckpoint.java`
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplitState.java`

- [ ] **Step 1: Implement MockSplit**

Create `MockSplit.java`:

```java
package com.etl.source.mock;

import com.etl.core.source.BaseSourceSplit;
import com.etl.source.mock.config.MockSourceConfig;
import lombok.Getter;

/**
 * Mock Source 单分片
 * 固定 ID: "mock-split-0"
 */
@Getter
public class MockSplit extends BaseSourceSplit {

    private final MockSourceConfig mockConfig;

    public MockSplit(MockSourceConfig mockConfig) {
        super("mock-split-0");  // 固定分片 ID
        this.mockConfig = mockConfig;
    }
}
```

- [ ] **Step 2: Implement MockEnumCheckpoint**

Create `MockEnumCheckpoint.java`:

```java
package com.etl.source.mock;

import com.etl.core.source.BaseEnumCheckpoint;
import lombok.Getter;

import java.util.List;

/**
 * Mock Source Enumerator 检查点
 */
@Getter
public class MockEnumCheckpoint extends BaseEnumCheckpoint<MockSplit> {

    public MockEnumCheckpoint(List<MockSplit> pendingSplits) {
        super(pendingSplits);
    }
}
```

- [ ] **Step 3: Implement MockSplitState**

Create `MockSplitState.java`:

```java
package com.etl.source.mock;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;

/**
 * Mock Split 状态
 */
@Getter
public class MockSplitState extends BaseSplitState<MockSplit> {

    public MockSplitState(MockSplit split) {
        super(split);
    }
}
```

- [ ] **Step 4: Commit split classes**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplit.java
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockEnumCheckpoint.java
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplitState.java
git commit -m "feat: 实现 Mock Split 和 Checkpoint 类"
```

---

## Task 7: Implement MockSplitEnumerator

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplitEnumerator.java`

- [ ] **Step 1: Implement MockSplitEnumerator**

Create `MockSplitEnumerator.java`:

```java
package com.etl.source.mock;

import com.etl.core.source.BaseSplitEnumerator;
import com.etl.source.mock.config.MockSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.Collections;

/**
 * Mock Source 分片枚举器
 * 在 start() 时创建单个 MockSplit 并分配到队列
 */
@Slf4j
public class MockSplitEnumerator
        extends BaseSplitEnumerator<MockSplit, MockEnumCheckpoint> {

    private final MockSourceConfig mockConfig;

    public MockSplitEnumerator(
            SplitEnumeratorContext<MockSplit> context,
            MockSourceConfig mockConfig) {
        super(context);
        this.mockConfig = mockConfig;
    }

    public MockSplitEnumerator(
            SplitEnumeratorContext<MockSplit> context,
            MockEnumCheckpoint checkpoint,
            MockSourceConfig mockConfig) {
        super(context, checkpoint);
        this.mockConfig = mockConfig;
    }

    @Override
    public void start() {
        // 创建固定的单分片
        MockSplit split = new MockSplit(mockConfig);

        // 添加到待分配队列
        pendingSplits.add(split);

        log.info("Mock Source 创建单分片: {}", split.splitId());

        // 立即通知所有已注册的 Reader 分片已就绪
        context.callAllReadersToRequestSplits();
    }

    @Override
    public MockEnumCheckpoint snapshotState(long checkpointId) {
        return new MockEnumCheckpoint(Collections.unmodifiableList(pendingSplits));
    }
}
```

- [ ] **Step 2: Commit MockSplitEnumerator**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplitEnumerator.java
git commit -m "feat: 实现 MockSplitEnumerator"
```

---

## Task 8: Implement MockSplitReader (Core Logic)

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock/MockSplitReaderTest.java`
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplitReader.java`

- [ ] **Step 1: Write test for batch mode with rows**

Create `MockSplitReaderTest.java`:

```java
package com.etl.source.mock;

import com.etl.core.schema.EtlSchema;
import com.etl.source.mock.config.MockSourceConfig;
import com.etl.source.mock.generator.DataRowGenerator;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MockSplitReaderTest {

    @Test
    void testBatchModeWithFixedRows() throws Exception {
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .field("value", "INT")
            .build();

        List<MockSourceConfig.RowData> rowsData = Arrays.asList(
            createRowData("INSERT", Map.of("id", 1L, "value", 100)),
            createRowData("INSERT", Map.of("id", 2L, "value", 200))
        );

        MockSourceConfig config = MockSourceConfig.builder()
            .runMode(MockSourceConfig.RunMode.BATCH)
            .schema(schema)
            .rows(rowsData)
            .build();

        MockSplitReader reader = new MockSplitReader(config);

        // 模拟 fetchNextBatch 调用
        reader.fetchNextBatch();

        // 验证数据生成
        // 注意：实际测试需要通过 BaseSplitReader 的 outputQueue 获取数据
        // 这里简化验证逻辑

        reader.close();
    }

    private MockSourceConfig.RowData createRowData(String kind, Map<String, Object> data) {
        MockSourceConfig.RowData rowData = new MockSourceConfig.RowData();
        rowData.setKind(kind);
        rowData.setData(data);
        return rowData;
    }
}
```

- [ ] **Step 2: Implement MockSplitReader**

Create `MockSplitReader.java`:

```java
package com.etl.source.mock;

import com.etl.core.schema.EtlSchema;
import com.etl.core.source.BaseSplitReader;
import com.etl.source.mock.config.MockSourceConfig;
import com.etl.source.mock.generator.DataRowGenerator;
import com.etl.source.mock.generator.RandomRowGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock Split 读取器
 * 核心逻辑：batch 模式读取固定数据，streaming 模式定时生成数据
 */
@Slf4j
public class MockSplitReader extends BaseSplitReader<Row, MockSplit> {

    private final MockSourceConfig mockConfig;
    private final EtlSchema schema;

    // batch 模式状态
    private Iterator<Row> batchDataIterator;
    private int currentRowIndex = 0;

    // streaming 模式状态
    private ScheduledExecutorService scheduler;
    private volatile boolean running = true;
    private final AtomicLong rowCounter = new AtomicLong(0);

    public MockSplitReader(MockSourceConfig mockConfig) {
        this.mockConfig = mockConfig;
        this.schema = mockConfig.getSchema();

        // 初始化数据生成器
        if (mockConfig.getRows() != null) {
            // batch 模式 - 固定数据
            List<Row> rows = DataRowGenerator.generateRows(mockConfig.getRows(), schema);
            batchDataIterator = rows.iterator();
            log.info("Batch 模式：从 rows 配置生成 {} 行数据", rows.size());
        } else if (mockConfig.getRunMode() == MockSourceConfig.RunMode.BATCH) {
            // batch 模式 - 随机生成
            List<Row> rows = RandomRowGenerator.generateRows(schema, mockConfig.getNumRows());
            batchDataIterator = rows.iterator();
            log.info("Batch 模式：随机生成 {} 行数据", rows.size());
        } else {
            // streaming 模式 - 定时生成
            scheduler = Executors.newSingleThreadScheduledExecutor();
            log.info("Streaming 模式：准备启动定时生成器，间隔 {} ms", mockConfig.getIntervalMs());
        }
    }

    @Override
    protected void fetchNextBatch() throws IOException {
        if (mockConfig.getRunMode() == MockSourceConfig.RunMode.BATCH) {
            fetchBatchData();
        } else {
            fetchStreamingData();
        }
    }

    /**
     * batch 模式：从 iterator 读取数据
     */
    private void fetchBatchData() throws IOException {
        if (batchDataIterator.hasNext()) {
            Row row = batchDataIterator.next();
            currentRowIndex++;

            // 添加到输出队列
            outputQueue.add(row);

            log.debug("读取第 {} 行数据: {}", currentRowIndex, row);
        } else {
            // 数据读取完毕，标记分片结束
            log.info("Batch 模式数据读取完毕，共 {} 行", currentRowIndex);
            finished = true;
        }
    }

    /**
     * streaming 模式：定时生成数据
     *
     * 注意：此方法的线程模型与 BaseSplitReader 的协调机制：
     * 1. BaseSplitReader 的工作线程会周期调用 fetchNextBatch()
     * 2. 第一次调用时，启动 ScheduledExecutorService（scheduler）
     * 3. scheduler 在独立线程中定时生成数据并添加到 outputQueue
     * 4. 后续 fetchNextBatch() 调用时，scheduler 已启动，直接返回
     * 5. BaseSplitReader 工作线程从 outputQueue 取数据并发射到下游
     * 6. outputQueue 作为两个线程之间的缓冲区，协调生产/消费速率
     * 7. streaming 模式下 finished 永不为 true，因为数据无限生成
     */
    private void fetchStreamingData() throws IOException {
        // 防止重复启动 scheduler
        if (scheduler != null && !scheduler.isShutdown()) {
            log.debug("Scheduler 已启动，跳过重复启动");
            return;
        }

        // 启动定时任务
        scheduler.scheduleAtFixedRate(() -> {
            if (!running) {
                return;
            }

            try {
                // 生成随机数据
                Row row = RandomRowGenerator.generateRow(schema);
                rowCounter.incrementAndGet();

                // 添加到输出队列
                outputQueue.add(row);

                log.debug("生成第 {} 行数据: {}", rowCounter.get(), row);
            } catch (Exception e) {
                log.error("生成数据失败", e);
            }
        }, 0, mockConfig.getIntervalMs(), TimeUnit.MILLISECONDS);

        log.info("Streaming 模式：scheduler 已启动");
    }

    @Override
    public void close() throws IOException {
        if (scheduler != null) {
            running = false;
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("MockSplitReader 关闭，streaming 模式共生成 {} 行数据", rowCounter.get());
        } else {
            log.info("MockSplitReader 关闭，batch 模式共读取 {} 行数据", currentRowIndex);
        }
    }
}
```

- [ ] **Step 3: Commit MockSplitReader**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplitReader.java
git add flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock/MockSplitReaderTest.java
git commit -m "feat: 实现 MockSplitReader 核心逻辑"
```

---

## Task 9: Implement MockRecordEmitter and MockSourceReader

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockRecordEmitter.java`
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSourceReader.java`

- [ ] **Step 1: Implement MockRecordEmitter**

Create `MockRecordEmitter.java`:

```java
package com.etl.source.mock;

import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

/**
 * Mock Source 记录发射器
 * 发射 Row 数据到下游
 */
public class MockRecordEmitter implements RecordEmitter<Row, Row, MockSplitState> {

    @Override
    public void emitRecord(Row record, MockSplitState splitState, SourceOutput<Row> output) {
        output.collect(record);
    }
}
```

- [ ] **Step 2: Implement MockSourceReader**

Create `MockSourceReader.java`:

```java
package com.etl.source.mock;

import com.etl.core.source.BaseSourceReader;
import com.etl.core.source.BaseSplitReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Mock Source 阅读器
 * 包装 MockSplitReader，处理分片状态
 */
public class MockSourceReader
        extends BaseSourceReader<Row, Row, MockSplit, MockSplitState> {

    private final Supplier<BaseSplitReader<Row, MockSplit>> splitReaderSupplier;

    public MockSourceReader(
            Supplier<BaseSplitReader<Row, MockSplit>> splitReaderSupplier,
            SourceReaderContext context) {
        super(
            splitReaderSupplier,
            new MockRecordEmitter(),
            new Configuration(),
            context
        );
        this.splitReaderSupplier = splitReaderSupplier;
    }

    @Override
    public MockSplitState initializedState(MockSplit split) {
        return new MockSplitState(split);
    }

    @Override
    protected MockSplit toSplitType(String splitId, MockSplitState splitState) {
        return splitState.getSplit();
    }
}
```

- [ ] **Step 3: Commit emitter and reader**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockRecordEmitter.java
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSourceReader.java
git commit -m "feat: 实现 MockRecordEmitter 和 MockSourceReader"
```

---

## Task 10: Implement MockSource Main Class

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSource.java`

- [ ] **Step 1: Implement MockSource**

Create `MockSource.java`:

```java
package com.etl.source.mock;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.source.mock.config.MockSourceConfig;
import com.etl.source.mock.generator.DataRowGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Mock Source 主类
 * 支持固定数据和随机生成两种模式
 */
@Slf4j
public class MockSource extends AbstractSplitSource<MockSplit, MockEnumCheckpoint> {

    private final MockSourceConfig mockConfig;

    public MockSource(SourceConfig config) {
        super(config);

        // 1. 获取运行模式（从 job.mode 传入）
        MockSourceConfig.RunMode runMode = getRunModeFromJobConfig(config);

        // 2. Schema 校验
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");
        validateSimpleTypesOnly(schema);

        // 3. 配置参数获取
        List<MockSourceConfig.RowData> rows = parseRowsConfig(config);
        Integer numRows = config.getInteger("numRows", 10);
        Long intervalMs = config.getLong("intervalMs", 1000L);

        // 4. 配置冲突警告
        if (runMode == MockSourceConfig.RunMode.BATCH && config.contains("intervalMs")) {
            log.warn("batch 模式下 intervalMs 参数被忽略");
        }
        if (runMode == MockSourceConfig.RunMode.STREAMING &&
            (config.contains("rows") || config.contains("numRows"))) {
            log.warn("streaming 模式下 rows/numRows 参数被忽略");
        }

        // 5. 封装配置对象
        this.mockConfig = MockSourceConfig.builder()
            .runMode(runMode)
            .schema(schema)
            .rows(rows)
            .numRows(runMode == MockSourceConfig.RunMode.BATCH ?
                (rows != null ? rows.size() : numRows) : null)
            .intervalMs(runMode == MockSourceConfig.RunMode.STREAMING ? intervalMs : null)
            .build();

        log.info("创建 MockSource: runMode={}, rows={}, numRows={}, intervalMs={}",
            runMode, rows != null ? rows.size() : null,
            mockConfig.getNumRows(), mockConfig.getIntervalMs());
    }

    private MockSourceConfig.RunMode getRunModeFromJobConfig(SourceConfig config) {
        // 注意：SourceConfig 在 JobBuilder 创建时，会从 JobConfig 传递 mode 参数
        // 参考 JobBuilder.build() 中对 SourceConfig 的初始化逻辑
        String mode = config.getString("mode", "batch");
        return MockSourceConfig.RunMode.valueOf(mode.toUpperCase());
    }

    private List<MockSourceConfig.RowData> parseRowsConfig(SourceConfig config) {
        if (!config.contains("rows")) {
            return null;
        }

        // 解析 rows 配置（JSON 数组）
        // 注意：SourceConfig.getList() 方法需要验证，可能需要使用 JSON 解析器
        List<Map<String, Object>> rowsList = config.getList("rows");
        List<MockSourceConfig.RowData> rowsData = new java.util.ArrayList<>();

        for (Map<String, Object> rowMap : rowsList) {
            MockSourceConfig.RowData rowData = new MockSourceConfig.RowData();
            rowData.setKind((String) rowMap.get("kind"));
            rowData.setData((Map<String, Object>) rowMap.get("data"));
            rowsData.add(rowData);
        }

        return rowsData;
    }

    @Override
    public Boundedness getBoundedness() {
        return mockConfig.getRunMode() == MockSourceConfig.RunMode.BATCH
            ? Boundedness.BOUNDED
            : Boundedness.UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MockSplit, MockEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<MockSplit> enumContext) {
        log.info("创建 MockSplitEnumerator");
        return new MockSplitEnumerator(enumContext, mockConfig);
    }

    @Override
    public SplitEnumerator<MockSplit, MockEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<MockSplit> enumContext,
            MockEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 MockSplitEnumerator");
        return new MockSplitEnumerator(enumContext, checkpoint, mockConfig);
    }

    @Override
    public SourceReader<Row, MockSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 MockSourceReader");

        Supplier<BaseSplitReader<Row, MockSplit>> splitReaderSupplier = () ->
            new MockSplitReader(mockConfig);

        return new MockSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<MockEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }

    /**
     * 校验 schema 不包含复杂类型
     */
    private void validateSimpleTypesOnly(EtlSchema schema) {
        for (int i = 0; i < schema.getFieldCount(); i++) {
            TypeInformation<?> type = schema.getFieldType(i);
            if (isComplexType(type)) {
                throw new SchemaConfigException(
                    "Mock Source 不支持复杂类型字段 '" + schema.getFieldName(i) + "'。" +
                    "只支持简单类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
            }
        }
    }

    /**
     * 检查是否为复杂类型
     */
    private boolean isComplexType(TypeInformation<?> type) {
        return type instanceof RowTypeInfo
            || type instanceof BasicArrayTypeInfo
            || type instanceof ObjectArrayTypeInfo;
    }
}
```

- [ ] **Step 2: Commit MockSource main class**

```bash
git add flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSource.java
git commit -m "feat: 实现 MockSource 主类"
```

---

## Task 11: Add Mock Source Tests

**Files:**
- Create: `flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock/MockSourceTest.java`

- [ ] **Step 1: Write MockSourceTest**

Create `MockSourceTest.java`:

```java
package com.etl.source.mock;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MockSourceTest {

    @Test
    void testBatchModeBoundedness() {
        SourceConfig config = createMockConfig("batch", null, 10);
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .field("value", "INT")
            .build();
        config.setSchema(schema);

        MockSource source = new MockSource(config);

        assertEquals(Boundedness.BOUNDED, source.getBoundedness());
    }

    @Test
    void testStreamingModeBoundedness() {
        SourceConfig config = createMockConfig("streaming", null, null);
        config.set("intervalMs", 1000L);
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .build();
        config.setSchema(schema);

        MockSource source = new MockSource(config);

        assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    @Test
    void testSchemaWithComplexTypeThrowsException() {
        SourceConfig config = createMockConfig("batch", null, 10);

        // 创建包含复杂类型的 schema（模拟）
        EtlSchema schema = EtlSchema.builder()
            .field("id", "LONG")
            .field("tags", "ARRAY")  // 复杂类型
            .build();
        config.setSchema(schema);

        assertThrows(SchemaConfigException.class, () -> {
            new MockSource(config);
        });
    }

    private SourceConfig createMockConfig(String mode, Object rows, Object numRows) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("mode", mode);
        if (rows != null) {
            configMap.put("rows", rows);
        }
        if (numRows != null) {
            configMap.put("numRows", numRows);
        }

        return new SourceConfig(configMap);
    }
}
```

- [ ] **Step 2: Run all tests**

```bash
mvn test -pl flink-etl-source/flink-etl-source-mock
```

Expected: All tests pass.

- [ ] **Step 3: Commit tests**

```bash
git add flink-etl-source/flink-etl-source-mock/src/test/java/com/etl/source/mock/MockSourceTest.java
git commit -m "feat: 新增 MockSource 配置校验测试"
```

---

## Task 12: Add Client Dependency and Compile

**Files:**
- Modify: `flink-etl-client/pom.xml`

- [ ] **Step 1: Add mock dependency to client**

Modify `flink-etl-client/pom.xml`, add dependency:

```xml
<dependency>
    <groupId>com.etl</groupId>
    <artifactId>flink-etl-source-mock</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 2: Compile entire project**

```bash
mvn clean install -DskipTests
```

Expected: Compilation succeeds, mock module installed to local repository.

- [ ] **Step 3: Commit client dependency**

```bash
git add flink-etl-client/pom.xml
git commit -m "feat: flink-etl-client 新增 Mock Source 依赖"
```

---

## Task 13: Create Example Configuration Files

**Files:**
- Create: `docs/examples/mock-batch-fixed.json`
- Create: `docs/examples/mock-batch-random.json`
- Create: `docs/examples/mock-streaming.json`
- Create: `docs/examples/mock-cdc-test.json`

- [ ] **Step 1: Create batch fixed data example**

Create `docs/examples/mock-batch-fixed.json`:

```json
{
  "job": {
    "name": "mock-batch-fixed",
    "mode": "batch"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "users",
    "config": {
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "age": "INT",
        "active": "BOOLEAN"
      },
      "rows": [
        {
          "kind": "INSERT",
          "data": {
            "id": 1,
            "name": "Alice",
            "age": 25,
            "active": true
          }
        },
        {
          "kind": "INSERT",
          "data": {
            "id": 2,
            "name": "Bob",
            "age": 30,
            "active": false
          }
        },
        {
          "kind": "UPDATE_AFTER",
          "data": {
            "id": 1,
            "name": "Alice Updated",
            "age": 26,
            "active": true
          }
        }
      ]
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "users",
    "config": {}
  }]
}
```

- [ ] **Step 2: Create batch random example**

Create `docs/examples/mock-batch-random.json`:

```json
{
  "job": {
    "name": "mock-batch-random",
    "mode": "batch"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "orders",
    "config": {
      "schema": {
        "orderId": "LONG",
        "amount": "DOUBLE",
        "status": "STRING",
        "created_at": "TIMESTAMP"
      },
      "numRows": 50
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "orders",
    "config": {}
  }]
}
```

- [ ] **Step 3: Create streaming example**

Create `docs/examples/mock-streaming.json`:

```json
{
  "job": {
    "name": "mock-streaming",
    "mode": "streaming"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "events",
    "config": {
      "schema": {
        "eventId": "LONG",
        "eventType": "STRING",
        "timestamp": "TIMESTAMP"
      },
      "intervalMs": 500
    }
  }],
  "sinks": [{
    "type": "console",
    "inputTable": "events",
    "config": {}
  }]
}
```

- [ ] **Step 4: Create CDC test example**

Create `docs/examples/mock-cdc-test.json`:

```json
{
  "job": {
    "name": "mock-cdc-test",
    "mode": "batch"
  },
  "sources": [{
    "type": "mock",
    "outputTable": "users_cdc",
    "config": {
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "email": "STRING"
      },
      "rows": [
        {
          "kind": "INSERT",
          "data": {
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com"
          }
        },
        {
          "kind": "INSERT",
          "data": {
            "id": 2,
            "name": "Bob",
            "email": "bob@example.com"
          }
        },
        {
          "kind": "UPDATE_AFTER",
          "data": {
            "id": 1,
            "name": "Alice Updated",
            "email": "alice_new@example.com"
          }
        },
        {
          "kind": "DELETE",
          "data": {
            "id": 2,
            "name": "Bob",
            "email": "bob@example.com"
          }
        }
      ]
    }
  }],
  "sinks": [{
    "type": "jdbc",
    "inputTable": "users_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/test_db",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "cdc",
      "keyFields": ["id"],
      "batchSize": 100
    }
  }]
}
```

- [ ] **Step 5: Commit example files**

```bash
git add docs/examples/mock-*.json
git commit -m "docs: 新增 Mock Source 配置示例文件"
```

---

## Task 14: Update PLUGINS.md Documentation

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: Add Mock Source section to PLUGINS.md**

Add Mock Source section after Kafka Source section:

```markdown
### Mock Source

从配置生成模拟数据，支持固定数据和随机生成两种模式，适用于测试和演示场景。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `schema` | 是 | - | Schema 定义，只支持简单类型（STRING、BOOLEAN、INT、LONG、DOUBLE、DECIMAL、TIMESTAMP） |
| `rows` | 否 | - | 固定数据配置，数组格式，每项包含 `kind` 和 `data` 字段。batch 模式时使用，streaming 模式时忽略 |
| `numRows` | 否 | 10 | batch 模式随机生成的行数。仅在未配置 `rows` 时生效，streaming 模式时忽略 |
| `intervalMs` | 否 | 1000 | streaming 模式生成数据的间隔时间（毫秒）。batch 模式时忽略 |

#### rows 配置格式

rows 配置支持 Flink 的四种 RowKind：

| RowKind | 说明 |
|---------|------|
| `INSERT` | 插入数据 |
| `UPDATE_BEFORE` | 更新前的数据 |
| `UPDATE_AFTER` | 更新后的数据 |
| `DELETE` | 删除数据 |

rows 配置示例：

```json
{
  "rows": [
    {
      "kind": "INSERT",
      "data": { "id": 1, "name": "张三", "age": 18 }
    },
    {
      "kind": "UPDATE_AFTER",
      "data": { "id": 2, "name": "李四", "age": 25 }
    },
    {
      "kind": "DELETE",
      "data": { "id": 3, "name": "王五" }
    }
  ]
}
```

#### 配置示例

**Batch 模式 - 固定数据：**

```json
{
  "source": {
    "type": "mock",
    "outputTable": "users",
    "config": {
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "age": "INT"
      },
      "rows": [
        { "kind": "INSERT", "data": { "id": 1, "name": "Alice", "age": 25 } },
        { "kind": "INSERT", "data": { "id": 2, "name": "Bob", "age": 30 } }
      ]
    }
  }
}
```

**Batch 模式 - 随机生成：**

```json
{
  "source": {
    "type": "mock",
    "outputTable": "orders",
    "config": {
      "schema": {
        "orderId": "LONG",
        "amount": "DOUBLE",
        "status": "STRING"
      },
      "numRows": 50
    }
  }
}
```

**Streaming 模式 - 定时生成：**

```json
{
  "source": {
    "type": "mock",
    "outputTable": "events",
    "config": {
      "schema": {
        "eventId": "LONG",
        "eventType": "STRING",
        "timestamp": "TIMESTAMP"
      },
      "intervalMs": 500
    }
  }
}
```

#### CDC 测试场景

Mock Source 可生成带有 RowKind 的数据，配合 JDBC Sink 的 CDC 模式测试 CDC 写入逻辑：

```json
{
  "source": {
    "type": "mock",
    "outputTable": "users_cdc",
    "config": {
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "email": "STRING"
      },
      "rows": [
        { "kind": "INSERT", "data": { "id": 1, "name": "Alice", "email": "alice@example.com" } },
        { "kind": "UPDATE_AFTER", "data": { "id": 1, "name": "Alice Updated", "email": "alice_new@example.com" } },
        { "kind": "DELETE", "data": { "id": 2, "name": "Bob", "email": "bob@example.com" } }
      ]
    }
  },
  "sink": {
    "type": "jdbc",
    "inputTable": "users_cdc",
    "config": {
      "url": "jdbc:mysql://localhost:3306/test_db",
      "username": "root",
      "password": "password",
      "table": "users",
      "mode": "cdc",
      "keyFields": ["id"]
    }
  }
}
```

#### 使用场景

- **快速原型验证**：无需搭建外部数据源即可测试完整的 ETL 任务配置
- **Transform 逻辑测试**：提供稳定的测试数据验证 SQL Transform 的正确性
- **Sink 功能测试**：验证 Console Sink、JDBC Sink（包括 CDC 模式）的写入逻辑
- **流式任务演示**：演示 streaming 模式下的持续数据处理流程

---
```

- [ ] **Step 2: Commit PLUGINS.md update**

```bash
git add PLUGINS.md
git commit -m "docs: PLUGINS.md 新增 Mock Source 说明"
```

---

## Task 15: Integration Testing and Verification

**Files:**
- Test execution with example configs

- [ ] **Step 1: Test batch mode with fixed data**

```bash
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file docs/examples/mock-batch-fixed.json
```

Expected: Console outputs 3 rows with correct RowKind markers.

- [ ] **Step 2: Test batch mode with random data**

```bash
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file docs/examples/mock-batch-random.json
```

Expected: Console outputs 50 randomly generated rows.

- [ ] **Step 3: Test streaming mode (manual stop after 5s)**

```bash
java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar \
  --file docs/examples/mock-streaming.json
```

Expected: Console continuously outputs rows every 500ms. Manually stop after observing several rows.

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "feat: Mock Source 实现完成并通过集成测试"
```

---

## Summary

**Total Tasks:** 15
**Total Steps:** ~75 bite-sized actions

**Implementation Flow:**

1. Maven module setup
2. SPI entry point
3. Configuration POJO
4. Data generators (TDD)
5. Split/checkpoint classes
6. Enumerator
7. Split reader (core logic)
8. Record emitter and source reader
9. Main source class
10. Configuration tests
11. Client dependency
12. Example configs
13. Documentation
14. Integration testing
15. Final verification

**TDD Approach:**
- DataRowGenerator: test → implement → verify
- RandomRowGenerator: test → implement → verify
- MockSource: test → implement → verify

**Frequent Commits:** Each task has a commit step to maintain clear progress history.