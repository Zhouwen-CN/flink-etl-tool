# Schema 类型系统实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 ETL 框架实现 Schema 类型系统，支持 CSV 强制配置 schema 进行类型转换，JDBC 可选 schema 自动推断。

**Architecture:** 在 flink-etl-core 中创建 schema 包，定义类型系统和转换器。修改 SourceConfig 支持 schema 解析，修改 AbstractSplitSource 实现 ResultTypeQueryable。适配 CsvFormatPlugin 和 JDBC Source。

**Tech Stack:** Java 11, Flink 1.19, Lombok, JUnit 5

---

## 文件结构

**新建文件：**
```
flink-etl-core/src/main/java/com/etl/core/schema/
├── EtlFieldType.java        # 类型枚举
├── EtlField.java            # 字段定义
├── EtlSchema.java           # Schema 容器
├── SchemaParser.java        # Schema 解析器
├── SchemaConfigException.java  # 配置异常
├── TypeConverter.java       # 类型转换器
├── TypeConversionException.java # 转换异常
└── FlinkTypeConverter.java  # Flink 类型转换

flink-etl-core/src/test/java/com/etl/core/schema/
├── EtlFieldTypeTest.java
├── SchemaParserTest.java
├── TypeConverterTest.java
└── FlinkTypeConverterTest.java
```

**修改文件：**
```
flink-etl-core/src/main/java/com/etl/core/config/SourceConfig.java
flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java
flink-etl-source/flink-etl-source-localfile/.../CsvFormatPlugin.java
flink-etl-source/flink-etl-source-jdbc/.../JdbcDialect.java
flink-etl-source/flink-etl-source-jdbc/.../dialect/MySQLDialect.java
flink-etl-source/flink-etl-source-jdbc/.../JdbcSource.java
```

---

## Chunk 1: 核心类型定义

### Task 1: EtlFieldType 枚举

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/schema/EtlFieldType.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/schema/EtlFieldTypeTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.etl.core.schema;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class EtlFieldTypeTest {

    @Test
    void fromString_shouldReturnCorrectType_forValidTypes() {
        assertEquals(EtlFieldType.STRING, EtlFieldType.fromString("string"));
        assertEquals(EtlFieldType.STRING, EtlFieldType.fromString("STRING"));
        assertEquals(EtlFieldType.STRING, EtlFieldType.fromString("String"));
        assertEquals(EtlFieldType.BOOLEAN, EtlFieldType.fromString("boolean"));
        assertEquals(EtlFieldType.INT, EtlFieldType.fromString("int"));
        assertEquals(EtlFieldType.LONG, EtlFieldType.fromString("long"));
        assertEquals(EtlFieldType.DOUBLE, EtlFieldType.fromString("double"));
        assertEquals(EtlFieldType.DECIMAL, EtlFieldType.fromString("decimal"));
        assertEquals(EtlFieldType.TIMESTAMP, EtlFieldType.fromString("timestamp"));
        assertEquals(EtlFieldType.BYTES, EtlFieldType.fromString("bytes"));
    }

    @Test
    void fromString_shouldReturnNull_forInvalidTypes() {
        assertNull(EtlFieldType.fromString("invalid"));
        assertNull(EtlFieldType.fromString(""));
        assertNull(EtlFieldType.fromString(null));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl flink-etl-core -Dtest=EtlFieldTypeTest`
Expected: FAIL (class not found)

- [ ] **Step 3: Write minimal implementation**

```java
package com.etl.core.schema;

/**
 * ETL 字段类型枚举
 * 定义支持的 8 种基础类型
 */
public enum EtlFieldType {
    STRING,
    BOOLEAN,
    INT,
    LONG,
    DOUBLE,
    DECIMAL,
    TIMESTAMP,
    BYTES;

    /**
     * 从字符串解析类型（大小写不敏感）
     *
     * @param typeName 类型名称
     * @return 对应的枚举值，无效时返回 null
     */
    public static EtlFieldType fromString(String typeName) {
        if (typeName == null || typeName.isEmpty()) {
            return null;
        }
        try {
            return EtlFieldType.valueOf(typeName.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl flink-etl-core -Dtest=EtlFieldTypeTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/EtlFieldType.java
git add flink-etl-core/src/test/java/com/etl/core/schema/EtlFieldTypeTest.java
git commit -m "feat: 添加 EtlFieldType 枚举类型"
```

---

### Task 2: EtlField 字段定义

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/schema/EtlField.java`

- [ ] **Step 1: Write the implementation**

```java
package com.etl.core.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * ETL 字段定义
 * 包含字段名和类型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EtlField implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 字段名
     */
    private String name;

    /**
     * 字段类型
     */
    private EtlFieldType type;
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/EtlField.java
git commit -m "feat: 添加 EtlField 字段定义类"
```

---

### Task 3: EtlSchema 容器

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/schema/EtlSchemaTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.etl.core.schema;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class EtlSchemaTest {

    @Test
    void getField_byIndex_shouldReturnCorrectField() {
        EtlSchema schema = new EtlSchema(Arrays.asList(
            new EtlField("id", EtlFieldType.LONG),
            new EtlField("name", EtlFieldType.STRING)
        ));

        assertEquals("id", schema.getField(0).getName());
        assertEquals("name", schema.getField(1).getName());
    }

    @Test
    void getField_byName_shouldReturnCorrectField() {
        EtlSchema schema = new EtlSchema(Arrays.asList(
            new EtlField("id", EtlFieldType.LONG),
            new EtlField("name", EtlFieldType.STRING)
        ));

        assertEquals(EtlFieldType.LONG, schema.getField("id").getType());
        assertEquals(EtlFieldType.STRING, schema.getField("name").getType());
    }

    @Test
    void getField_byName_shouldReturnNull_whenNotFound() {
        EtlSchema schema = new EtlSchema(Arrays.asList(
            new EtlField("id", EtlFieldType.LONG)
        ));

        assertNull(schema.getField("nonexistent"));
    }

    @Test
    void getFieldNames_shouldReturnAllNames() {
        EtlSchema schema = new EtlSchema(Arrays.asList(
            new EtlField("id", EtlFieldType.LONG),
            new EtlField("name", EtlFieldType.STRING)
        ));

        List<String> names = schema.getFieldNames();
        assertEquals(2, names.size());
        assertEquals("id", names.get(0));
        assertEquals("name", names.get(1));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl flink-etl-core -Dtest=EtlSchemaTest`
Expected: FAIL (class not found)

- [ ] **Step 3: Write minimal implementation**

```java
package com.etl.core.schema;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ETL Schema 容器
 * 包含字段列表定义
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EtlSchema implements Serializable {

    private static final long serialVersionUID = 1L;

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

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl flink-etl-core -Dtest=EtlSchemaTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java
git add flink-etl-core/src/test/java/com/etl/core/schema/EtlSchemaTest.java
git commit -m "feat: 添加 EtlSchema 容器类"
```

---

### Task 4: SchemaConfigException 异常

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/schema/SchemaConfigException.java`

- [ ] **Step 1: Write the implementation**

```java
package com.etl.core.schema;

/**
 * Schema 配置异常
 * 当 Schema 配置格式错误时抛出
 */
public class SchemaConfigException extends RuntimeException {

    public SchemaConfigException(String message) {
        super("Schema 配置错误: " + message);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/SchemaConfigException.java
git commit -m "feat: 添加 SchemaConfigException 异常类"
```

---

### Task 5: SchemaParser 解析器

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.etl.core.schema;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class SchemaParserTest {

    @Test
    void parse_shouldReturnSchema_forValidConfig() {
        Map<String, Object> field1 = new HashMap<>();
        field1.put("name", "id");
        field1.put("type", "long");

        Map<String, Object> field2 = new HashMap<>();
        field2.put("name", "name");
        field2.put("type", "string");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("fields", List.of(field1, field2));

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertNotNull(schema);
        assertEquals(2, schema.getFields().size());
        assertEquals("id", schema.getField(0).getName());
        assertEquals(EtlFieldType.LONG, schema.getField(0).getType());
        assertEquals("name", schema.getField(1).getName());
        assertEquals(EtlFieldType.STRING, schema.getField(1).getType());
    }

    @Test
    void parse_shouldReturnNull_forNullInput() {
        assertNull(SchemaParser.parse(null));
    }

    @Test
    void parse_shouldThrowException_forNonMapInput() {
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse("not a map"));
    }

    @Test
    void parse_shouldThrowException_whenFieldsMissing() {
        Map<String, Object> schemaConfig = new HashMap<>();
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
    }

    @Test
    void parse_shouldThrowException_whenFieldsNotList() {
        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("fields", "not a list");
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
    }

    @Test
    void parse_shouldThrowException_whenFieldNameMissing() {
        Map<String, Object> field = new HashMap<>();
        field.put("type", "string");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("fields", List.of(field));

        SchemaConfigException ex = assertThrows(SchemaConfigException.class,
            () -> SchemaParser.parse(schemaConfig));
        assertTrue(ex.getMessage().contains("缺少 'name'"));
    }

    @Test
    void parse_shouldThrowException_whenFieldTypeMissing() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("fields", List.of(field));

        SchemaConfigException ex = assertThrows(SchemaConfigException.class,
            () -> SchemaParser.parse(schemaConfig));
        assertTrue(ex.getMessage().contains("缺少 'type'"));
    }

    @Test
    void parse_shouldThrowException_forUnsupportedType() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");
        field.put("type", "unsupported");

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("fields", List.of(field));

        SchemaConfigException ex = assertThrows(SchemaConfigException.class,
            () -> SchemaParser.parse(schemaConfig));
        assertTrue(ex.getMessage().contains("不支持"));
    }

    @Test
    void parse_shouldHandleCaseInsensitiveType() {
        Map<String, Object> field = new HashMap<>();
        field.put("name", "id");
        field.put("type", "LONG"); // 大写

        Map<String, Object> schemaConfig = new HashMap<>();
        schemaConfig.put("fields", List.of(field));

        EtlSchema schema = SchemaParser.parse(schemaConfig);
        assertEquals(EtlFieldType.LONG, schema.getField(0).getType());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl flink-etl-core -Dtest=SchemaParserTest`
Expected: FAIL (class not found)

- [ ] **Step 3: Write minimal implementation**

```java
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

        return new EtlSchema(fields);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl flink-etl-core -Dtest=SchemaParserTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java
git add flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java
git commit -m "feat: 添加 SchemaParser 解析器"
```

---

## Chunk 2: 类型转换器

### Task 6: TypeConversionException 异常

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/schema/TypeConversionException.java`

- [ ] **Step 1: Write the implementation**

```java
package com.etl.core.schema;

import lombok.Getter;

/**
 * 类型转换异常
 * 当值无法转换为目标类型时抛出
 */
@Getter
public class TypeConversionException extends RuntimeException {

    private final String fieldName;
    private final String rawValue;
    private final EtlFieldType targetType;

    public TypeConversionException(String fieldName, String rawValue,
                                   EtlFieldType targetType, Throwable cause) {
        super(String.format("字段 '%s' 类型转换失败: 值 '%s' 无法转换为 %s",
              fieldName, rawValue, targetType), cause);
        this.fieldName = fieldName;
        this.rawValue = rawValue;
        this.targetType = targetType;
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/TypeConversionException.java
git commit -m "feat: 添加 TypeConversionException 异常类"
```

---

### Task 7: TypeConverter 类型转换器

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/schema/TypeConverterTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.etl.core.schema;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import static org.junit.jupiter.api.Assertions.*;

class TypeConverterTest {

    // === null 和空值处理 ===

    @Test
    void convert_shouldReturnNull_forNullValue() {
        assertNull(TypeConverter.convert(null, "field", EtlFieldType.STRING));
    }

    @Test
    void convert_shouldReturnNull_forEmptyString() {
        assertNull(TypeConverter.convert("", "field", EtlFieldType.STRING));
        assertNull(TypeConverter.convert("  ", "field", EtlFieldType.STRING));
    }

    // === STRING 类型 ===

    @Test
    void convert_shouldReturnString_forStringType() {
        assertEquals("hello", TypeConverter.convert("hello", "field", EtlFieldType.STRING));
        assertEquals("123", TypeConverter.convert("123", "field", EtlFieldType.STRING));
    }

    // === BOOLEAN 类型 ===

    @Test
    void convert_shouldReturnBoolean_forBooleanStrings() {
        assertTrue((Boolean) TypeConverter.convert("true", "field", EtlFieldType.BOOLEAN));
        assertTrue((Boolean) TypeConverter.convert("TRUE", "field", EtlFieldType.BOOLEAN));
        assertTrue((Boolean) TypeConverter.convert("1", "field", EtlFieldType.BOOLEAN));
        assertTrue((Boolean) TypeConverter.convert("yes", "field", EtlFieldType.BOOLEAN));

        assertFalse((Boolean) TypeConverter.convert("false", "field", EtlFieldType.BOOLEAN));
        assertFalse((Boolean) TypeConverter.convert("FALSE", "field", EtlFieldType.BOOLEAN));
        assertFalse((Boolean) TypeConverter.convert("0", "field", EtlFieldType.BOOLEAN));
        assertFalse((Boolean) TypeConverter.convert("no", "field", EtlFieldType.BOOLEAN));
    }

    @Test
    void convert_shouldThrowException_forInvalidBoolean() {
        assertThrows(TypeConversionException.class,
            () -> TypeConverter.convert("invalid", "field", EtlFieldType.BOOLEAN));
    }

    // === INT 类型 ===

    @Test
    void convert_shouldReturnInteger_forIntStrings() {
        assertEquals(123, TypeConverter.convert("123", "field", EtlFieldType.INT));
        assertEquals(-456, TypeConverter.convert("-456", "field", EtlFieldType.INT));
        assertEquals(0, TypeConverter.convert("0", "field", EtlFieldType.INT));
    }

    @Test
    void convert_shouldThrowException_forInvalidInt() {
        assertThrows(TypeConversionException.class,
            () -> TypeConverter.convert("abc", "field", EtlFieldType.INT));
    }

    @Test
    void convert_shouldReturnSameType_forAlreadyCorrectType() {
        assertEquals(123, TypeConverter.convert(123, "field", EtlFieldType.INT));
    }

    // === LONG 类型 ===

    @Test
    void convert_shouldReturnLong_forLongStrings() {
        assertEquals(123456789L, TypeConverter.convert("123456789", "field", EtlFieldType.LONG));
        assertEquals(-987654321L, TypeConverter.convert("-987654321", "field", EtlFieldType.LONG));
    }

    // === DOUBLE 类型 ===

    @Test
    void convert_shouldReturnDouble_forDoubleStrings() {
        assertEquals(3.14, (Double) TypeConverter.convert("3.14", "field", EtlFieldType.DOUBLE), 0.001);
        assertEquals(-2.5, (Double) TypeConverter.convert("-2.5", "field", EtlFieldType.DOUBLE), 0.001);
    }

    // === DECIMAL 类型 ===

    @Test
    void convert_shouldReturnBigDecimal_forDecimalStrings() {
        BigDecimal result = (BigDecimal) TypeConverter.convert("123.456", "field", EtlFieldType.DECIMAL);
        assertEquals(new BigDecimal("123.456"), result);
    }

    // === TIMESTAMP 类型 ===

    @Test
    void convert_shouldReturnLocalDateTime_forTimestampStrings() {
        LocalDateTime result = (LocalDateTime) TypeConverter.convert(
            "2024-01-15 10:30:00", "field", EtlFieldType.TIMESTAMP);
        assertEquals(2024, result.getYear());
        assertEquals(1, result.getMonthValue());
        assertEquals(15, result.getDayOfMonth());
        assertEquals(10, result.getHour());
        assertEquals(30, result.getMinute());
    }

    @Test
    void convert_shouldThrowException_forInvalidTimestamp() {
        assertThrows(TypeConversionException.class,
            () -> TypeConverter.convert("invalid-date", "field", EtlFieldType.TIMESTAMP));
    }

    // === BYTES 类型 ===

    @Test
    void convert_shouldReturnBytes_forStringInput() {
        byte[] result = (byte[]) TypeConverter.convert("hello", "field", EtlFieldType.BYTES);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
    }

    @Test
    void convert_shouldReturnSameBytes_forBytesInput() {
        byte[] input = "test".getBytes(StandardCharsets.UTF_8);
        byte[] result = (byte[]) TypeConverter.convert(input, "field", EtlFieldType.BYTES);
        assertSame(input, result);
    }

    // === 兼容类型检查 ===

    @Test
    void convert_shouldReturnSameValue_forCompatibleType() {
        // Integer -> INT
        assertEquals(42, TypeConverter.convert(42, "field", EtlFieldType.INT));

        // Long -> LONG
        assertEquals(100L, TypeConverter.convert(100L, "field", EtlFieldType.LONG));

        // Boolean -> BOOLEAN
        assertTrue((Boolean) TypeConverter.convert(true, "field", EtlFieldType.BOOLEAN));

        // String -> STRING
        assertEquals("test", TypeConverter.convert("test", "field", EtlFieldType.STRING));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl flink-etl-core -Dtest=TypeConverterTest`
Expected: FAIL (class not found)

- [ ] **Step 3: Write minimal implementation**

```java
package com.etl.core.schema;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * 类型转换器
 * 将原始值转换为目标类型
 */
public class TypeConverter {

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将原始值转换为目标类型
     *
     * @param value 原始值（通常是 String）
     * @param fieldName 字段名（用于错误信息）
     * @param targetType 目标类型
     * @return 转换后的值
     * @throws TypeConversionException 转换失败时抛出
     */
    public static Object convert(Object value, String fieldName, EtlFieldType targetType) {
        if (value == null) {
            return null;
        }

        // 如果已经是目标类型或兼容类型，直接返回
        if (isCompatibleType(value, targetType)) {
            return value;
        }

        String strValue = String.valueOf(value).trim();
        if (strValue.isEmpty()) {
            return null;
        }

        try {
            switch (targetType) {
                case STRING:
                    return strValue;
                case BOOLEAN:
                    return parseBoolean(strValue);
                case INT:
                    return Integer.parseInt(strValue);
                case LONG:
                    return Long.parseLong(strValue);
                case DOUBLE:
                    return Double.parseDouble(strValue);
                case DECIMAL:
                    return new BigDecimal(strValue);
                case TIMESTAMP:
                    return LocalDateTime.parse(strValue, DEFAULT_TIMESTAMP_FORMAT);
                case BYTES:
                    return parseBytes(value, strValue);
                default:
                    throw new IllegalArgumentException("不支持的类型: " + targetType);
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new TypeConversionException(fieldName, strValue, targetType, e);
        }
    }

    private static boolean isCompatibleType(Object value, EtlFieldType targetType) {
        switch (targetType) {
            case STRING:
                return value instanceof String;
            case BOOLEAN:
                return value instanceof Boolean;
            case INT:
                return value instanceof Integer;
            case LONG:
                return value instanceof Long;
            case DOUBLE:
                return value instanceof Double;
            case DECIMAL:
                return value instanceof BigDecimal;
            case TIMESTAMP:
                return value instanceof LocalDateTime || value instanceof java.sql.Timestamp;
            case BYTES:
                return value instanceof byte[];
            default:
                return false;
        }
    }

    private static Boolean parseBoolean(String value) {
        // 支持多种布尔值表示
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)) {
            return false;
        }
        throw new NumberFormatException("无法解析为布尔值: " + value);
    }

    private static byte[] parseBytes(Object value, String strValue) {
        // 如果已经是字节数组，直接返回
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        // 字符串转字节数组
        return strValue.getBytes(StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl flink-etl-core -Dtest=TypeConverterTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java
git add flink-etl-core/src/test/java/com/etl/core/schema/TypeConverterTest.java
git commit -m "feat: 添加 TypeConverter 类型转换器"
```

---

### Task 8: FlinkTypeConverter Flink 类型转换

**Files:**
- Create: `flink-etl-core/src/main/java/com/etl/core/schema/FlinkTypeConverter.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/schema/FlinkTypeConverterTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.etl.core.schema;

import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class FlinkTypeConverterTest {

    @Test
    void toRowType_shouldReturnCorrectRowType() {
        EtlSchema schema = new EtlSchema(Arrays.asList(
            new EtlField("id", EtlFieldType.LONG),
            new EtlField("name", EtlFieldType.STRING),
            new EtlField("age", EtlFieldType.INT),
            new EtlField("price", EtlFieldType.DOUBLE),
            new EtlField("active", EtlFieldType.BOOLEAN),
            new EtlField("amount", EtlFieldType.DECIMAL),
            new EtlField("created_at", EtlFieldType.TIMESTAMP),
            new EtlField("data", EtlFieldType.BYTES)
        ));

        RowType rowType = FlinkTypeConverter.toRowType(schema);

        assertEquals(8, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertEquals("name", rowType.getFieldNames().get(1));

        // 验证类型映射
        assertTrue(rowType.getTypeAt(0) instanceof BigIntType);
        assertTrue(rowType.getTypeAt(1) instanceof VarCharType);
        assertTrue(rowType.getTypeAt(2) instanceof IntType);
        assertTrue(rowType.getTypeAt(3) instanceof DoubleType);
        assertTrue(rowType.getTypeAt(4) instanceof BooleanType);
        assertTrue(rowType.getTypeAt(5) instanceof DecimalType);
        assertTrue(rowType.getTypeAt(6) instanceof TimestampType);
        assertTrue(rowType.getTypeAt(7) instanceof VarBinaryType);
    }

    @Test
    void toRowType_shouldHandleSingleField() {
        EtlSchema schema = new EtlSchema(Arrays.asList(
            new EtlField("id", EtlFieldType.INT)
        ));

        RowType rowType = FlinkTypeConverter.toRowType(schema);

        assertEquals(1, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldNames().get(0));
        assertTrue(rowType.getTypeAt(0) instanceof IntType);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl flink-etl-core -Dtest=FlinkTypeConverterTest`
Expected: FAIL (class not found)

- [ ] **Step 3: Write minimal implementation**

```java
package com.etl.core.schema;

import org.apache.flink.table.types.logical.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Flink 类型转换器
 * 将 EtlSchema 转换为 Flink RowType
 */
public class FlinkTypeConverter {

    /**
     * 将 EtlSchema 转换为 Flink RowType
     */
    public static RowType toRowType(EtlSchema schema) {
        List<RowType.RowField> fields = schema.getFields().stream()
            .map(f -> new RowType.RowField(f.getName(), toLogicalType(f.getType())))
            .collect(Collectors.toList());
        return new RowType(fields);
    }

    /**
     * 将 EtlFieldType 转换为 Flink LogicalType
     */
    private static LogicalType toLogicalType(EtlFieldType type) {
        switch (type) {
            case STRING:
                return new VarCharType(VarCharType.MAX_LENGTH);
            case BOOLEAN:
                return new BooleanType();
            case INT:
                return new IntType();
            case LONG:
                return new BigIntType();
            case DOUBLE:
                return new DoubleType();
            case DECIMAL:
                return new DecimalType(38, 18); // 默认精度
            case TIMESTAMP:
                return new TimestampType(9); // 纳秒精度
            case BYTES:
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            default:
                throw new IllegalArgumentException("不支持的类型: " + type);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl flink-etl-core -Dtest=FlinkTypeConverterTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/FlinkTypeConverter.java
git add flink-etl-core/src/test/java/com/etl/core/schema/FlinkTypeConverterTest.java
git commit -m "feat: 添加 FlinkTypeConverter Flink 类型转换器"
```

---

## Chunk 3: 抽象层集成

### Task 9: SourceConfig 扩展

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/config/SourceConfig.java`

- [ ] **Step 1: 读取现有文件**

Read: `flink-etl-core/src/main/java/com/etl/core/config/SourceConfig.java`

- [ ] **Step 2: 添加 getSchema 方法**

在 SourceConfig 类中添加以下方法：

```java
/**
 * 获取 Schema 配置
 *
 * @return EtlSchema 对象，如果未配置则返回 null
 */
public EtlSchema getSchema() {
    if (config == null) {
        return null;
    }
    return SchemaParser.parse(config.get("schema"));
}
```

需要添加 import:
```java
import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.SchemaParser;
```

- [ ] **Step 3: 编译验证**

Run: `mvn compile -pl flink-etl-core`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/config/SourceConfig.java
git commit -m "feat: SourceConfig 添加 getSchema 方法"
```

---

### Task 10: AbstractSplitSource 扩展

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java`

- [ ] **Step 1: 读取现有文件**

Read: `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java`

- [ ] **Step 2: 实现 ResultTypeQueryable 接口**

修改类定义和添加方法：

```java
package com.etl.core.source;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.FlinkTypeConverter;
import org.apache.flink.api.common.typeinfo.ResultTypeQueryable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * 支持分片的 Source 抽象基类
 * 简化 Flink FLIP-27 Source API 的实现
 *
 * @param <T> 输出记录类型
 * @param <SplitT> 分片类型
 * @param <CheckpointT> 检查点类型
 */
public abstract class AbstractSplitSource<T, SplitT extends SourceSplit, CheckpointT>
        implements Source<T, SplitT, CheckpointT>, ResultTypeQueryable<T> {

    protected EtlSchema schema;

    @Override
    public abstract SplitEnumerator<SplitT, CheckpointT>
    createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, CheckpointT>
    restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext, CheckpointT checkpoint);

    @Override
    public abstract SourceReader<T, SplitT> createReader(SourceReaderContext readerContext);

    @Override
    public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

    @Override
    public abstract SimpleVersionedSerializer<CheckpointT> getEnumeratorCheckpointSerializer();

    @Override
    public TypeInformation<T> getProducedType() {
        if (schema != null) {
            return (TypeInformation<T>) TypeConversions.fromDataTypeToLegacyInfo(
                TypeConversions.fromLogicalToDataType(FlinkTypeConverter.toRowType(schema)));
        }
        return null;
    }
}
```

- [ ] **Step 3: 编译验证**

Run: `mvn compile -pl flink-etl-core`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java
git commit -m "feat: AbstractSplitSource 实现 ResultTypeQueryable 接口"
```

---

## Chunk 4: CsvFormatPlugin 适配

### Task 11: CsvFormatPlugin 改造

**Files:**
- Modify: `flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java`
- Modify: `flink-etl-source/flink-etl-source-localfile/src/test/java/com/etl/source/localfile/format/CsvFormatPluginTest.java`

- [ ] **Step 1: 读取现有文件**

Read: `flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java`
Read: `flink-etl-source/flink-etl-source-localfile/src/test/java/com/etl/source/localfile/format/CsvFormatPluginTest.java`

- [ ] **Step 2: 修改 CsvFormatPlugin**

完整替换为：

```java
package com.etl.source.localfile.format;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.*;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * CSV 格式解析插件
 * 要求配置 schema 定义字段名和类型
 */
@Slf4j
@AutoService(FileFormatPlugin.class)
public class CsvFormatPlugin implements FileFormatPlugin {

    @Override
    public String getType() {
        return "csv";
    }

    @Override
    public List<String> resolveFields(SourceConfig config, InputStream firstFile) {
        // 字段名从 schema 获取
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            throw new SchemaConfigException("CSV Source 必须配置 schema");
        }
        return schema.getFieldNames();
    }

    @Override
    public Iterable<Row> parse(SourceConfig config, InputStream inputStream, List<String> fields) {
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            throw new SchemaConfigException("CSV Source 必须配置 schema");
        }

        String encoding = config.getString("encoding");
        Charset charset = encoding != null ? Charset.forName(encoding) : StandardCharsets.UTF_8;

        String delimiter = config.getString("delimiter");
        char delim = delimiter != null ? delimiter.charAt(0) : ',';

        // skipHeader: 是否跳过 CSV 第一行（默认 true）
        boolean skipHeader = config.getBoolean("skipHeader", true);

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setDelimiter(delim)
                .build();

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charset));
            CSVParser parser = csvFormat.parse(reader);

            return new CsvRowIterable(parser, schema, reader, inputStream, skipHeader);

        } catch (IOException e) {
            throw new RuntimeException("解析 CSV 文件失败: " + e.getMessage(), e);
        }
    }

    /**
     * CSV Row 迭代器封装
     * 确保在迭代完成后关闭输入流
     */
    private static class CsvRowIterable implements Iterable<Row> {

        private final CSVParser parser;
        private final EtlSchema schema;
        private final BufferedReader reader;
        private final InputStream inputStream;
        private final boolean skipHeader;
        private volatile boolean closed = false;

        CsvRowIterable(CSVParser parser, EtlSchema schema, BufferedReader reader,
                       InputStream inputStream, boolean skipHeader) {
            this.parser = parser;
            this.schema = schema;
            this.reader = reader;
            this.inputStream = inputStream;
            this.skipHeader = skipHeader;
        }

        @Override
        public Iterator<Row> iterator() {
            return new Iterator<Row>() {
                private final Iterator<CSVRecord> csvIterator = parser.iterator();
                private boolean headerSkipped = false;

                @Override
                public boolean hasNext() {
                    if (closed) {
                        return false;
                    }
                    // 跳过头部行（如果配置了 skipHeader=true）
                    if (skipHeader && !headerSkipped && csvIterator.hasNext()) {
                        csvIterator.next(); // 跳过头部
                        headerSkipped = true;
                    }
                    boolean hasNext = csvIterator.hasNext();
                    if (!hasNext) {
                        closeQuietly();
                    }
                    return hasNext;
                }

                @Override
                public Row next() {
                    CSVRecord record = csvIterator.next();
                    int schemaSize = schema.getFields().size();
                    int recordSize = record.size();
                    Row row = new Row(schemaSize);

                    for (int i = 0; i < schemaSize; i++) {
                        Object value;
                        if (i < recordSize) {
                            value = record.get(i);
                        } else {
                            log.warn("CSV 行缺少字段 '{}', 已设为 null", schema.getField(i).getName());
                            value = null;
                        }

                        EtlField field = schema.getField(i);
                        Object converted = TypeConverter.convert(value, field.getName(), field.getType());
                        row.setField(i, converted);
                    }

                    // 检查是否有多余列
                    if (recordSize > schemaSize) {
                        log.warn("CSV 行有 {} 个多余列被忽略", recordSize - schemaSize);
                    }

                    return row;
                }

                private void closeQuietly() {
                    if (closed) {
                        return;
                    }
                    closed = true;
                    try {
                        parser.close();
                    } catch (Exception e) {
                        log.warn("关闭 CSV 解析器失败", e);
                    }
                    try {
                        reader.close();
                    } catch (Exception e) {
                        log.warn("关闭 BufferedReader 失败", e);
                    }
                    try {
                        inputStream.close();
                    } catch (Exception e) {
                        log.warn("关闭输入流失败", e);
                    }
                }
            };
        }
    }
}
```

- [ ] **Step 3: 编译验证**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-localfile`
Expected: BUILD SUCCESS

- [ ] **Step 4: 更新测试文件 CsvFormatPluginTest**

更新测试以验证新功能：

```java
package com.etl.source.localfile.format;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.SchemaConfigException;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

class CsvFormatPluginTest {

    private CsvFormatPlugin plugin = new CsvFormatPlugin();

    @Test
    void resolveFields_shouldThrowException_whenSchemaMissing() {
        SourceConfig config = new SourceConfig();
        config.setConfig(new java.util.HashMap<>());

        ByteArrayInputStream input = new ByteArrayInputStream("id,name".getBytes());

        assertThrows(SchemaConfigException.class,
            () -> plugin.resolveFields(config, input));
    }

    @Test
    void parse_shouldConvertTypes_correctly() throws Exception {
        // 构造带 schema 的配置
        SourceConfig config = new SourceConfig();
        java.util.Map<String, Object> configMap = new java.util.HashMap<>();
        java.util.Map<String, Object> schemaMap = new java.util.HashMap<>();

        schemaMap.put("fields", java.util.List.of(
            java.util.Map.of("name", "id", "type", "long"),
            java.util.Map.of("name", "name", "type", "string"),
            java.util.Map.of("name", "age", "type", "int")
        ));
        configMap.put("schema", schemaMap);
        configMap.put("skipHeader", false); // 测试数据无头行
        config.setConfig(configMap);

        // CSV 数据（无头行）
        String csvData = "1,Alice,25\n2,Bob,30";
        ByteArrayInputStream input = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        Iterable<org.apache.flink.types.Row> rows = plugin.parse(config, input,
            java.util.List.of("id", "name", "age"));

        var iterator = rows.iterator();

        // 验证第一行
        assertTrue(iterator.hasNext());
        org.apache.flink.types.Row row1 = iterator.next();
        assertEquals(1L, row1.getField(0)); // Long 类型
        assertEquals("Alice", row1.getField(1)); // String 类型
        assertEquals(25, row1.getField(2)); // Integer 类型

        // 验证第二行
        assertTrue(iterator.hasNext());
        org.apache.flink.types.Row row2 = iterator.next();
        assertEquals(2L, row2.getField(0));
        assertEquals("Bob", row2.getField(1));
        assertEquals(30, row2.getField(2));
    }

    @Test
    void parse_shouldSkipHeader_whenSkipHeaderTrue() throws Exception {
        SourceConfig config = new SourceConfig();
        java.util.Map<String, Object> configMap = new java.util.HashMap<>();
        java.util.Map<String, Object> schemaMap = new java.util.HashMap<>();

        schemaMap.put("fields", java.util.List.of(
            java.util.Map.of("name", "id", "type", "string")
        ));
        configMap.put("schema", schemaMap);
        configMap.put("skipHeader", true); // 跳过头行
        config.setConfig(configMap);

        String csvData = "header\nvalue1\nvalue2";
        ByteArrayInputStream input = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));

        Iterable<org.apache.flink.types.Row> rows = plugin.parse(config, input,
            java.util.List.of("id"));

        var iterator = rows.iterator();
        iterator.forEachRemaining(row -> {
            // 不应该包含 "header"
            assertNotEquals("header", row.getField(0));
        });
    }
}
```

- [ ] **Step 5: Commit**

```bash
git add flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java
git commit -m "feat: CsvFormatPlugin 支持 schema 类型转换

- 强制要求配置 schema
- header 参数改为 skipHeader
- 删除 columns 参数，字段名从 schema 获取
- 使用 TypeConverter 进行类型转换"
```

---

## Chunk 5: JDBC Source 适配

### Task 12: JdbcDialect 接口扩展

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcDialect.java`

- [ ] **Step 1: 读取现有文件**

Read: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcDialect.java`

- [ ] **Step 2: 添加 inferSchema 方法**

在接口中添加：

```java
/**
 * 从 ResultSetMetaData 推断 Schema
 *
 * @param metaData ResultSet 元数据
 * @return 推断的 EtlSchema
 * @throws SQLException SQL 异常
 */
EtlSchema inferSchema(ResultSetMetaData metaData) throws SQLException;

/**
 * 构建示例查询（用于推断 Schema）
 *
 * @param table 表名（可能为 null）
 * @param sql 自定义 SQL（可能为 null）
 * @return 示例查询 SQL，返回空结果集
 */
String buildSampleQuery(String table, String sql);
```

需要添加 import:
```java
import com.etl.core.schema.EtlSchema;
import java.sql.ResultSetMetaData;
```

- [ ] **Step 3: 编译验证**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcDialect.java
git commit -m "feat: JdbcDialect 添加 inferSchema 方法"
```

---

### Task 13: MySQLDialect 实现

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/MySQLDialect.java`

- [ ] **Step 1: 读取现有文件**

Read: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/MySQLDialect.java`

- [ ] **Step 2: 实现新方法**

在 MySQLDialect 类中添加：

```java
@Override
public EtlSchema inferSchema(ResultSetMetaData metaData) throws SQLException {
    List<EtlField> fields = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
        String name = metaData.getColumnLabel(i);
        EtlFieldType type = inferFieldType(metaData.getColumnType(i));
        fields.add(new EtlField(name, type));
    }
    return new EtlSchema(fields);
}

@Override
public String buildSampleQuery(String table, String sql) {
    if (table != null) {
        return "SELECT * FROM " + quoteIdentifier(table) + " WHERE 1=0";
    } else {
        return "SELECT * FROM (" + sql + ") AS t WHERE 1=0";
    }
}

private EtlFieldType inferFieldType(int sqlType) {
    switch (sqlType) {
        case Types.BIT:
        case Types.BOOLEAN:
            return EtlFieldType.BOOLEAN;
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
            return EtlFieldType.INT;
        case Types.BIGINT:
            return EtlFieldType.LONG;
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
            return EtlFieldType.DOUBLE;
        case Types.DECIMAL:
        case Types.NUMERIC:
            return EtlFieldType.DECIMAL;
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
            return EtlFieldType.TIMESTAMP;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return EtlFieldType.BYTES;
        default:
            return EtlFieldType.STRING;
    }
}
```

需要添加 import:
```java
import com.etl.core.schema.EtlField;
import com.etl.core.schema.EtlFieldType;
import com.etl.core.schema.EtlSchema;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
```

- [ ] **Step 3: 修改 createRow 方法使用位置访问**

将 createRow 方法修改为：

```java
@Override
public Row createRow(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    // 使用位置访问方式
    Row row = new Row(columnCount);
    for (int i = 1; i <= columnCount; i++) {
        row.setField(i - 1, rs.getObject(i));
    }
    return row;
}
```

- [ ] **Step 4: 编译验证**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/MySQLDialect.java
git commit -m "feat: MySQLDialect 实现 inferSchema 方法"
```

---

### Task 14: JdbcSource 适配

**Files:**
- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 1: 读取现有文件**

Read: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java`

- [ ] **Step 2: 添加 schema 解析和推断逻辑**

修改构造函数和 createEnumerator：

在构造函数中添加：
```java
// 解析 schema（可选）
this.schema = config.getSchema();
```

在 createEnumerator 方法开头添加 schema 推断逻辑：
```java
@Override
public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
    log.info("创建 SplitEnumerator");

    // 如果没有配置 schema，尝试从数据库推断
    if (schema == null) {
        schema = inferSchemaFromDatabase();
    }

    Range<Long> range = getSplitColumnRange();
    int parallelism = enumContext.currentParallelism();
    List<RangeSplit> splits = calculateSplits(range, parallelism);
    return new JdbcSplitEnumerator(splits, enumContext);
}

private EtlSchema inferSchemaFromDatabase() {
    String sampleQuery = dialect.buildSampleQuery(table, sql);
    log.info("推断 Schema: {}", sampleQuery);

    try (Connection conn = DriverManager.getConnection(url, username, password);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sampleQuery)) {
        return dialect.inferSchema(rs.getMetaData());
    } catch (SQLException e) {
        throw new RuntimeException("从数据库推断 Schema 失败: " + e.getMessage(), e);
    }
}
```

需要添加 import:
```java
import com.etl.core.schema.EtlSchema;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
```

- [ ] **Step 3: 编译验证**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/JdbcSource.java
git commit -m "feat: JdbcSource 支持可选 schema 配置"
```

---

## Chunk 6: 集成测试和文档更新

### Task 15: 更新示例配置

**Files:**
- Modify: `docs/examples/csv-to-console.json`
- Modify: `docs/plugins.md`

- [ ] **Step 1: 更新 csv-to-console.json**

```json
{
  "job": {
    "name": "csv-to-console",
    "mode": "batch",
    "parallelism": 2
  },
  "source": {
    "type": "localfile",
    "config": {
      "path": "docs/examples/data/*.csv",
      "format": "csv",
      "skipHeader": true,
      "schema": {
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "age", "type": "int"}
        ]
      }
    }
  },
  "sink": {
    "type": "console",
    "config": {}
  }
}
```

- [ ] **Step 2: 更新 plugins.md 文档**

在 `docs/plugins.md` 的 LocalFile Source 部分更新参数说明：

```markdown
### LocalFile Source

本地文件数据源，支持读取 CSV 等格式文件。

**参数配置：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `path` | string | 是 | - | 文件路径，支持通配符 |
| `format` | string | 是 | - | 文件格式，支持 `csv` |
| `schema` | object | **是** | - | 字段定义，包含字段名和类型 |
| `skipHeader` | boolean | 否 | `true` | 是否跳过 CSV 第一行（头行） |
| `delimiter` | string | 否 | `,` | CSV 分隔符 |
| `encoding` | string | 否 | `UTF-8` | 文件编码 |
| `recursive` | boolean | 否 | `false` | 是否递归读取子目录 |

**schema 配置：**

```json
"schema": {
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
```

**支持的字段类型：**

| 类型 | Java 类型 | 说明 |
|------|-----------|------|
| `string` | String | 字符串 |
| `boolean` | Boolean | 布尔值（支持 true/false/1/0/yes/no） |
| `int` | Integer | 整数 |
| `long` | Long | 长整数 |
| `double` | Double | 浮点数 |
| `decimal` | BigDecimal | 高精度十进制数 |
| `timestamp` | LocalDateTime | 时间戳（格式：yyyy-MM-dd HH:mm:ss） |
| `bytes` | byte[] | 字节数组 |

**配置示例：**

```json
{
  "source": {
    "type": "localfile",
    "config": {
      "path": "/data/users.csv",
      "format": "csv",
      "skipHeader": true,
      "schema": {
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "age", "type": "int"}
        ]
      }
    }
  }
}
```

**注意：**
- CSV 文件必须配置 `schema`，不再支持 `columns` 参数
- `skipHeader` 参数替代了旧的 `header` 参数，语义更清晰
```

- [ ] **Step 3: Commit**

```bash
git add docs/examples/csv-to-console.json docs/plugins.md
git commit -m "docs: 更新示例配置和插件文档"
```

---

### Task 16: 运行完整测试

- [ ] **Step 1: 运行所有单元测试**

Run: `mvn test`
Expected: All tests pass

- [ ] **Step 2: 运行集成测试（手动）**

创建测试数据文件 `docs/examples/data/users.csv`：

```csv
id,name,age,price,created_at,is_active
1,Alice,25,100.50,2024-01-15 10:30:00,true
2,Bob,30,200.75,2024-01-16 14:20:00,false
3,Charlie,35,300.00,2024-01-17 09:00:00,true
```

更新配置文件 `docs/examples/csv-to-console.json`：

```json
{
  "job": {
    "name": "csv-to-console",
    "mode": "batch",
    "parallelism": 1
  },
  "source": {
    "type": "localfile",
    "config": {
      "path": "docs/examples/data/users.csv",
      "format": "csv",
      "skipHeader": true,
      "schema": {
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "age", "type": "int"},
          {"name": "price", "type": "double"},
          {"name": "created_at", "type": "timestamp"},
          {"name": "is_active", "type": "boolean"}
        ]
      }
    }
  },
  "sink": {
    "type": "console",
    "config": {
      "format": "json"
    }
  }
}
```

运行命令：

```bash
java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/csv-to-console.json
```

验证输出：
1. 检查 Console 输出包含 3 条记录
2. 确认 `id` 字段是 Long 类型（输出中应显示为数字）
3. 确认 `price` 字段是 Double 类型（输出中应显示小数）
4. 确认 `created_at` 字段是 Timestamp 类型
5. 确认 `is_active` 字段是 Boolean 类型（输出中应显示 true/false）

预期输出示例：

```json
{"id":1,"name":"Alice","age":25,"price":100.5,"created_at":"2024-01-15T10:30:00","is_active":true}
{"id":2,"name":"Bob","age":30,"price":200.75,"created_at":"2024-01-16T14:20:00","is_active":false}
{"id":3,"name":"Charlie","age":35,"price":300.0,"created_at":"2024-01-17T09:00:00","is_active":true}
```

- [ ] **Step 3: 最终提交**

```bash
git add -A
git commit -m "feat: 完成 Schema 类型系统实现

- 添加 EtlFieldType、EtlField、EtlSchema 核心类
- 添加 SchemaParser 解析配置
- 添加 TypeConverter 类型转换器
- 添加 FlinkTypeConverter Flink 类型映射
- CSV Source 强制配置 schema
- JDBC Source 可选 schema，支持自动推断
- header 参数改为 skipHeader
- 删除 columns 参数"
```