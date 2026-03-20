# Schema 配置格式优化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 简化 Schema 架构，将 `EtlSchema` 改为直接存储 `String[] fieldNames` 和 `TypeInformation<?>[] fieldTypes`
，消除冗余的类型转换层。配置格式统一为对象格式 `{fieldName: fieldType}`。

**Architecture:**

- 移除中间抽象层：`EtlFieldType` 枚举、`EtlField` 类、`FlinkTypeConverter` 类
- `EtlSchema` 直接存储 Flink 原生类型信息，配置解析时一次性完成类型转换
- `TypeConverter` 改为基于 `TypeInformation` 进行类型判断和转换
- Schema 配置格式统一为对象格式，不再兼容旧数组格式

**Tech Stack:** Java 11, Flink 1.19.0, JUnit 5

---

## 文件结构

**删除文件：**

- `flink-etl-core/src/main/java/com/etl/core/schema/EtlFieldType.java`
- `flink-etl-core/src/main/java/com/etl/core/schema/EtlField.java`
- `flink-etl-core/src/main/java/com/etl/core/schema/FlinkTypeConverter.java`

**修改文件：**

- `flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java` - 简化为存储 fieldNames 和 fieldTypes
- `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java` - 直接解析为 Flink 类型
- `flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java` - 基于 TypeInformation 转换，迁移 fromSqlType 方法
- `flink-etl-core/src/main/java/com/etl/core/exception/TypeConversionException.java` - 字段类型改为 TypeInformation
- `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java` - 简化 getProducedType()
- `flink-etl-core/src/main/java/com/etl/core/config/SourceConfig.java` - 无需修改
- `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/JdbcDialect.java` - 使用
  TypeConverter.fromSqlType()
- `flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java` - 适配新
  API
- `docs/schema/job-config.schema.json` - 更新 schema 格式定义
- `docs/examples/csv-to-console.json` - 示例配置文件

**新增文件：**

- `flink-etl-core/src/test/java/com/etl/core/schema/EtlSchemaTest.java`
- `flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java`
- `flink-etl-core/src/test/java/com/etl/core/schema/TypeConverterTest.java`

---

### Task 1: 重构 EtlSchema

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java`

- [ ] **Step 1: 编写 EtlSchema 测试**

```java
package com.etl.core.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EtlSchemaTest {

    @Test
    void testCreateSchema() {
        String[] fieldNames = {"id", "name", "age"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING, Types.INT};

        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        assertEquals(3, schema.getFieldCount());
        assertArrayEquals(fieldNames, schema.getFieldNames());
        assertEquals(Types.LONG, schema.getFieldType(0));
        assertEquals(Types.STRING, schema.getFieldType(1));
        assertEquals(Types.INT, schema.getFieldType(2));
    }

    @Test
    void testGetFieldNamesAsList() {
        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};

        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        assertEquals(2, schema.getFieldNamesAsList().size());
        assertEquals("id", schema.getFieldNamesAsList().get(0));
        assertEquals("name", schema.getFieldNamesAsList().get(1));
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flink-etl-core -Dtest=EtlSchemaTest -q`
Expected: 测试失败

- [ ] **Step 3: 重写 EtlSchema**

```java
package com.etl.core.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * ETL Schema 定义
 * 直接存储字段名和 Flink TypeInformation，消除中间转换层
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EtlSchema implements Serializable {

    /**
     * 字段名数组
     */
    private String[] fieldNames;

    /**
     * 字段类型数组（Flink TypeInformation）
     */
    private TypeInformation<?>[] fieldTypes;

    /**
     * 获取字段数量
     */
    public int getFieldCount() {
        return fieldNames != null ? fieldNames.length : 0;
    }

    /**
     * 按索引获取字段名
     */
    public String getFieldName(int index) {
        return fieldNames[index];
    }

    /**
     * 按索引获取字段类型
     */
    public TypeInformation<?> getFieldType(int index) {
        return fieldTypes[index];
    }

    /**
     * 获取所有字段名（List 形式，便于使用）
     */
    public List<String> getFieldNamesAsList() {
        return Arrays.asList(fieldNames);
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn test -pl flink-etl-core -Dtest=EtlSchemaTest -q`
Expected: 测试通过

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/EtlSchema.java flink-etl-core/src/test/java/com/etl/core/schema/EtlSchemaTest.java
git commit -m "refactor: 简化 EtlSchema 直接存储 fieldNames 和 fieldTypes"
```

---

### Task 2: 重构 TypeConverter

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java`

- [ ] **Step 1: 编写 TypeConverter 测试**

```java
package com.etl.core.schema;

import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class TypeConverterTest {

    @Test
    void testConvertString() {
        assertEquals("hello", TypeConverter.convert("hello", "field", Types.STRING));
        assertNull(TypeConverter.convert("", "field", Types.STRING));
        assertNull(TypeConverter.convert(null, "field", Types.STRING));
    }

    @Test
    void testConvertInt() {
        assertEquals(123, TypeConverter.convert("123", "field", Types.INT));
        assertEquals(456, TypeConverter.convert(456, "field", Types.INT));
    }

    @Test
    void testConvertLong() {
        assertEquals(123L, TypeConverter.convert("123", "field", Types.LONG));
        assertEquals(456L, TypeConverter.convert(456L, "field", Types.LONG));
    }

    @Test
    void testConvertDouble() {
        assertEquals(3.14, TypeConverter.convert("3.14", "field", Types.DOUBLE));
        assertEquals(2.71, TypeConverter.convert(2.71, "field", Types.DOUBLE));
    }

    @Test
    void testConvertBoolean() {
        assertEquals(true, TypeConverter.convert("true", "field", Types.BOOLEAN));
        assertEquals(true, TypeConverter.convert("1", "field", Types.BOOLEAN));
        assertEquals(false, TypeConverter.convert("false", "field", Types.BOOLEAN));
        assertEquals(false, TypeConverter.convert("0", "field", Types.BOOLEAN));
    }

    @Test
    void testConvertBigDecimal() {
        Object result = TypeConverter.convert("123.456", "field", Types.BIG_DEC);
        assertTrue(result instanceof BigDecimal);
        assertEquals(new BigDecimal("123.456"), result);
    }

    @Test
    void testConvertLocalDateTime() {
        Object result = TypeConverter.convert("2024-01-15 10:30:00", "field", Types.LOCAL_DATE_TIME);
        assertTrue(result instanceof LocalDateTime);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flink-etl-core -Dtest=TypeConverterTest -q`
Expected: 测试失败（方法签名变更）

- [ ] **Step 3: 重写 TypeConverter**

说明：

- 方法签名从 `convert(value, fieldName, EtlFieldType)` 改为 `convert(value, fieldName, TypeInformation)`
- 新增 `fromSqlType(int sqlType)` 方法，从 `FlinkTypeConverter` 迁移过来

```java
package com.etl.core.schema;

import com.etl.core.exception.TypeConversionException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * 类型转换器
 * 将原始值转换为目标类型（基于 Flink TypeInformation）
 */
public class TypeConverter {

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将原始值转换为目标类型
     *
     * @param value 原始值（通常是 String）
     * @param fieldName 字段名（用于错误信息）
     * @param targetType 目标类型（Flink TypeInformation）
     * @return 转换后的值
     * @throws TypeConversionException 转换失败时抛出
     */
    public static Object convert(Object value, String fieldName, TypeInformation<?> targetType) {
        if (value == null) {
            return null;
        }

        // 处理字符串类型：检查空字符串
        if (value instanceof String) {
            String strValue = (String) value;
            if (strValue.isEmpty()) {
                return null;
            }
        }

        // 如果已经是目标类型或兼容类型，直接返回
        if (isCompatibleType(value, targetType)) {
            return value;
        }

        String strValue = String.valueOf(value);
        if (strValue.isEmpty()) {
            return null;
        }

        try {
            // 根据 TypeInformation 判断目标类型
            if (targetType == Types.STRING) {
                return strValue;
            } else if (targetType == Types.BOOLEAN) {
                return parseBoolean(strValue);
            } else if (targetType == Types.INT) {
                return Integer.parseInt(strValue);
            } else if (targetType == Types.LONG) {
                return Long.parseLong(strValue);
            } else if (targetType == Types.DOUBLE) {
                return Double.parseDouble(strValue);
            } else if (targetType == Types.BIG_DEC) {
                return new BigDecimal(strValue);
            } else if (targetType == Types.LOCAL_DATE_TIME) {
                return LocalDateTime.parse(strValue, DEFAULT_TIMESTAMP_FORMAT);
            } else {
                // 未知类型，返回原值
                return value;
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new TypeConversionException(fieldName, strValue, targetType, e);
        }
    }

    /**
     * 根据 JDBC java.sql.Types 转换为 Flink TypeInformation
     * 从 FlinkTypeConverter 迁移
     *
     * @param sqlType JDBC SQL 类型常量（来自 java.sql.Types）
     * @return 对应的 Flink TypeInformation
     */
    public static TypeInformation<?> fromSqlType(int sqlType) {
        // 注意：使用完全限定名避免与 Flink Types 冲突
        switch (sqlType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB:
            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NCLOB:
                return Types.STRING;

            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return Types.BOOLEAN;

            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
                return Types.INT;

            case java.sql.Types.BIGINT:
                return Types.LONG;

            case java.sql.Types.REAL:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
                return Types.DOUBLE;

            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                return Types.BIG_DEC;

            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                return Types.LOCAL_DATE_TIME;

            default:
                return Types.STRING;
        }
    }

    /**
     * 检查值是否已经是目标类型
     */
    private static boolean isCompatibleType(Object value, TypeInformation<?> targetType) {
        if (targetType == Types.STRING) {
            return value instanceof String;
        } else if (targetType == Types.BOOLEAN) {
            return value instanceof Boolean;
        } else if (targetType == Types.INT) {
            return value instanceof Integer;
        } else if (targetType == Types.LONG) {
            return value instanceof Long;
        } else if (targetType == Types.DOUBLE) {
            return value instanceof Double;
        } else if (targetType == Types.BIG_DEC) {
            return value instanceof BigDecimal;
        } else if (targetType == Types.LOCAL_DATE_TIME) {
            return value instanceof LocalDateTime || value instanceof java.sql.Timestamp;
        }
        return false;
    }

    /**
     * 解析布尔值（支持多种格式）
     */
    private static Boolean parseBoolean(String value) {
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)) {
            return false;
        }
        throw new NumberFormatException("无法解析为布尔值: " + value);
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn test -pl flink-etl-core -Dtest=TypeConverterTest -q`
Expected: 测试通过

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/TypeConverter.java flink-etl-core/src/test/java/com/etl/core/schema/TypeConverterTest.java
git commit -m "refactor: TypeConverter 改为基于 TypeInformation 进行类型转换"
```

---

### Task 2.5: 更新 TypeConversionException

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/exception/TypeConversionException.java`

- [ ] **Step 1: 修改字段类型为 TypeInformation**

```java
package com.etl.core.exception;

import lombok.Getter;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * 类型转换异常
 * 当值无法转换为目标类型时抛出
 */
@Getter
public class TypeConversionException extends RuntimeException {

    private final String fieldName;
    private final String rawValue;
    private final TypeInformation<?> targetType;

    public TypeConversionException(String fieldName, String rawValue,
                                   TypeInformation<?> targetType, Throwable cause) {
        super(String.format("字段 '%s' 类型转换失败: 值 '%s' 无法转换为 %s",
                fieldName, rawValue, targetType), cause);
        this.fieldName = fieldName;
        this.rawValue = rawValue;
        this.targetType = targetType;
    }
}
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-core -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/exception/TypeConversionException.java
git commit -m "refactor: TypeConversionException 字段类型改为 TypeInformation"
```

---

### Task 3: 重构 SchemaParser

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java`

- [ ] **Step 1: 编写 SchemaParser 测试**

```java
package com.etl.core.schema;

import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaParserTest {

    @Test
    void testParseObjectFormat() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("id", "LONG");
        schemaConfig.put("name", "STRING");
        schemaConfig.put("age", "INT");

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertNotNull(schema);
        assertEquals(3, schema.getFieldCount());
        assertEquals("id", schema.getFieldName(0));
        assertEquals(Types.LONG, schema.getFieldType(0));
        assertEquals("name", schema.getFieldName(1));
        assertEquals(Types.STRING, schema.getFieldType(1));
        assertEquals("age", schema.getFieldName(2));
        assertEquals(Types.INT, schema.getFieldType(2));
    }

    @Test
    void testParseNullReturnsNull() {
        assertNull(SchemaParser.parse(null));
    }

    @Test
    void testParseInvalidFormatThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SchemaParser.parse("invalid"));
    }

    @Test
    void testParseInvalidTypeThrowsException() {
        Map<String, Object> schemaConfig = Map.of("id", "INVALID_TYPE");
        assertThrows(IllegalArgumentException.class, () -> SchemaParser.parse(schemaConfig));
    }

    @Test
    void testAllSupportedTypes() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("f1", "STRING");
        schemaConfig.put("f2", "BOOLEAN");
        schemaConfig.put("f3", "INT");
        schemaConfig.put("f4", "LONG");
        schemaConfig.put("f5", "DOUBLE");
        schemaConfig.put("f6", "DECIMAL");
        schemaConfig.put("f7", "TIMESTAMP");

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertEquals(Types.STRING, schema.getFieldType(0));
        assertEquals(Types.BOOLEAN, schema.getFieldType(1));
        assertEquals(Types.INT, schema.getFieldType(2));
        assertEquals(Types.LONG, schema.getFieldType(3));
        assertEquals(Types.DOUBLE, schema.getFieldType(4));
        assertEquals(Types.BIG_DEC, schema.getFieldType(5));
        assertEquals(Types.LOCAL_DATE_TIME, schema.getFieldType(6));
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -pl flink-etl-core -Dtest=SchemaParserTest -q`
Expected: 测试失败

- [ ] **Step 3: 重写 SchemaParser**

```java
package com.etl.core.schema;

import com.etl.core.exception.SchemaConfigException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema 解析器
 * 从配置对象解析 EtlSchema
 *
 * <p>配置格式：{ "fieldName": "TYPE", ... }
 * <p>支持类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP
 */
public class SchemaParser {

    @SuppressWarnings("unchecked")
    public static EtlSchema parse(Object schemaConfig) {
        if (schemaConfig == null) {
            return null;
        }

        if (!(schemaConfig instanceof Map)) {
            throw new SchemaConfigException("schema 必须是对象格式 {fieldName: fieldType}");
        }

        return parseObjectFormat((Map<String, Object>) schemaConfig);
    }

    /**
     * 解析对象格式：{ "fieldName": "TYPE" }
     */
    private static EtlSchema parseObjectFormat(Map<String, Object> schemaConfig) {
        List<String> names = new ArrayList<>();
        List<TypeInformation<?>> types = new ArrayList<>();

        // 保持字段顺序
        Map<String, Object> orderedConfig = schemaConfig instanceof LinkedHashMap
                ? schemaConfig
                : new LinkedHashMap<>(schemaConfig);

        for (Map.Entry<String, Object> entry : orderedConfig.entrySet()) {
            String fieldName = entry.getKey();
            Object typeObj = entry.getValue();

            if (!(typeObj instanceof String)) {
                throw new SchemaConfigException(
                        "字段 '" + fieldName + "' 的类型必须是字符串");
            }

            TypeInformation<?> type = parseType((String) typeObj, fieldName);
            names.add(fieldName);
            types.add(type);
        }

        return new EtlSchema(
                names.toArray(new String[0]),
                types.toArray(new TypeInformation<?>[0])
        );
    }

    /**
     * 解析类型字符串为 Flink TypeInformation
     */
    private static TypeInformation<?> parseType(String typeName, String fieldName) {
        switch (typeName.toUpperCase()) {
            case "STRING":
                return Types.STRING;
            case "BOOLEAN":
                return Types.BOOLEAN;
            case "INT":
                return Types.INT;
            case "LONG":
                return Types.LONG;
            case "DOUBLE":
                return Types.DOUBLE;
            case "DECIMAL":
                return Types.BIG_DEC;
            case "TIMESTAMP":
                return Types.LOCAL_DATE_TIME;
            default:
                throw new SchemaConfigException(
                        "字段 '" + fieldName + "' 的类型 '" + typeName + "' 不支持，" +
                                "支持的类型: STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
        }
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn test -pl flink-etl-core -Dtest=SchemaParserTest -q`
Expected: 测试通过

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java
git commit -m "refactor: SchemaParser 直接解析为 Flink TypeInformation"
```

---

### Task 4: 简化 AbstractSplitSource

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java`

- [ ] **Step 1: 简化 getProducedType 方法**

找到旧代码（第 68-85 行）：

```java

@Override
public TypeInformation<Row> getProducedType() {
    EtlSchema schema = config.getSchema();
    if (schema == null) {
        return null;
    }

    // 使用 schema 字段名重建 Row，确保 Flink Table API 能识别列名
    List<String> fieldNames = schema.getFieldNames();
    List<EtlField> fields = schema.getFields();

    // 构建 RowTypeInfo 用于 Flink Table API
    TypeInformation<?>[] typeInfos = fields.stream()
            .map(f -> FlinkTypeConverter.fromEtlType(f.getType()))
            .toArray(TypeInformation<?>[]::new);
    String[] names = fieldNames.toArray(new String[0]);
    return Types.ROW_NAMED(names, typeInfos);
}
```

替换为：

```java

@Override
public TypeInformation<Row> getProducedType() {
    EtlSchema schema = config.getSchema();
    if (schema == null) {
        return null;
    }

    // 直接使用 EtlSchema 中的字段名和类型
    return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
}
```

- [ ] **Step 2: 更新 import 语句**

删除不再使用的 import：

```java
import com.etl.core.schema.EtlField;
import com.etl.core.schema.FlinkTypeConverter;
```

- [ ] **Step 3: 验证编译通过**

Run: `mvn compile -pl flink-etl-core -q`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/source/AbstractSplitSource.java
git commit -m "refactor: 简化 AbstractSplitSource.getProducedType"
```

---

### Task 5: 更新 CsvFormatPlugin

**Files:**

- Modify:
  `flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java`

- [ ] **Step 1: 适配新 EtlSchema API**

找到第 117-120 行的旧代码：

```java
                        EtlField field = schema.getField(i);
Object converted;
                        try{
converted =TypeConverter.

convert(value, field.getName(),field.

getType());
        }catch(
Exception e){
        throw new

RuntimeException("CSV 字段类型转换失败: 字段名="+field.getName()
                                    +", 字段类型="+field.

getType() +", 原始值="+value,e);
        }
```

替换为：

```java
                        String fieldName = schema.getFieldName(i);
Object converted;
                        try{
converted =TypeConverter.

convert(value, fieldName, schema.getFieldType(i));
        }catch(
Exception e){
        throw new

RuntimeException("CSV 字段类型转换失败: 字段名="+fieldName
                                    +", 字段类型="+schema.getFieldType(i) +", 原始值="+value,e);
        }
```

- [ ] **Step 2: 更新 import 语句**

删除不再使用的 import：

```java
import com.etl.core.schema.EtlField;
```

- [ ] **Step 3: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-localfile -q`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java
git commit -m "refactor: CsvFormatPlugin 适配新 EtlSchema API"
```

---

### Task 5.5: 更新 JdbcDialect

**Files:**

- Modify: `flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/JdbcDialect.java`

- [ ] **Step 1: 更新 import 和方法调用**

找到旧代码（第 3 行和第 74 行）：

```java
import com.etl.core.schema.FlinkTypeConverter;
...
types[index]=FlinkTypeConverter.

fromSqlType(metaData.getColumnType(i));
```

替换为：

```java
import com.etl.core.schema.TypeConverter;
...
types[index]=TypeConverter.

fromSqlType(metaData.getColumnType(i));
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -pl flink-etl-source/flink-etl-source-jdbc -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-jdbc/src/main/java/com/etl/source/jdbc/dialect/JdbcDialect.java
git commit -m "refactor: JdbcDialect 使用 TypeConverter.fromSqlType"
```

---

### Task 6: 删除废弃类

**Files:**

- Delete: `flink-etl-core/src/main/java/com/etl/core/schema/EtlFieldType.java`
- Delete: `flink-etl-core/src/main/java/com/etl/core/schema/EtlField.java`
- Delete: `flink-etl-core/src/main/java/com/etl/core/schema/FlinkTypeConverter.java`

- [ ] **Step 1: 删除文件**

```bash
rm flink-etl-core/src/main/java/com/etl/core/schema/EtlFieldType.java
rm flink-etl-core/src/main/java/com/etl/core/schema/EtlField.java
rm flink-etl-core/src/main/java/com/etl/core/schema/FlinkTypeConverter.java
```

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: 删除废弃的 EtlFieldType、EtlField、FlinkTypeConverter"
```

---

### Task 7: 更新 JSON Schema 和示例配置

**Files:**

- Modify: `docs/schema/job-config.schema.json`
- Modify: `docs/examples/csv-to-console.json`

- [ ] **Step 1: 更新 mysqlSourceConfig 中的 schema 定义（第 151-157 行）**

找到旧代码：

```json
        "schema": {
"type": "array",
"description": "字段定义（可选）",
"items": {
"$ref": "#/definitions/schemaField"
}
}
```

替换为：

```json
        "schema": {
"type": "object",
"description": "字段定义（可选）",
"additionalProperties": {
"type": "string",
"enum": ["STRING", "BOOLEAN", "INT", "LONG", "DOUBLE", "DECIMAL", "TIMESTAMP"]
}
}
```

- [ ] **Step 2: 更新 localfileSourceConfig 中的 schema 定义（第 195-201 行）**

找到旧代码：

```json
        "schema": {
"type": "array",
"description": "字段定义（必填）",
"items": {
"$ref": "#/definitions/schemaField"
}
}
```

替换为：

```json
        "schema": {
"type": "object",
"description": "字段定义（必填）",
"additionalProperties": {
"type": "string",
"enum": ["STRING", "BOOLEAN", "INT", "LONG", "DOUBLE", "DECIMAL", "TIMESTAMP"]
}
}
```

- [ ] **Step 3: 删除 schemaField 定义（第 232-247 行）**

删除不再使用的 `schemaField` 定义：

```json
    "schemaField": {
"type": "object",
"description": "字段定义",
"required": ["name", "type"],
"properties": {
"name": {
"type": "string",
"description": "字段名"
},
"type": {
"type": "string",
"description": "字段类型",
"enum": ["INT", "LONG", "STRING", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"]
}
}
}
```

- [ ] **Step 4: 更新示例配置**

找到 `docs/examples/csv-to-console.json` 第 18-23 行：

```json
        "schema": [
{"name": "id", "type": "LONG"},
{"name": "name", "type": "STRING"},
{"name": "age", "type": "INT"},
{"name": "email", "type": "STRING"}
]
```

替换为：

```json
        "schema": {
"id": "LONG",
"name": "STRING",
"age": "INT",
"email": "STRING"
}
```

- [ ] **Step 5: 验证 JSON 语法**

Run: `cat docs/examples/csv-to-console.json | python3 -m json.tool > /dev/null && echo "JSON valid"`
Expected: JSON valid

- [ ] **Step 6: Commit**

```bash
git add docs/schema/job-config.schema.json docs/examples/csv-to-console.json
git commit -m "docs: 更新 JSON Schema 和示例配置使用对象格式 schema"
```

---

### Task 8: 运行完整测试验证

**Files:**

- 无新增文件

- [ ] **Step 1: 运行全部测试**

Run: `mvn test -q`
Expected: BUILD SUCCESS，所有测试通过

- [ ] **Step 2: 编译打包验证**

Run: `mvn clean package -DskipTests -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 集成测试**

Run: `java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/csv-to-console.json`
Expected: 正常启动并读取 CSV 文件，无解析错误

- [ ] **Step 4: 最终检查**

```bash
git status
git log --oneline -10
```

---

## 变更摘要

| 文件                             | 变更类型 | 说明                                     |
|--------------------------------|------|----------------------------------------|
| `EtlSchema.java`               | 重写   | 简化为 `fieldNames[]` + `fieldTypes[]`    |
| `SchemaParser.java`            | 重写   | 直接解析为 Flink TypeInformation            |
| `TypeConverter.java`           | 重写   | 基于 TypeInformation 转换，新增 fromSqlType() |
| `TypeConversionException.java` | 修改   | 字段类型改为 TypeInformation                 |
| `AbstractSplitSource.java`     | 简化   | 直接使用 schema 的类型信息                      |
| `CsvFormatPlugin.java`         | 适配   | 使用新 API                                |
| `JdbcDialect.java`             | 适配   | 使用 TypeConverter.fromSqlType()         |
| `EtlFieldType.java`            | 删除   | 不再需要                                   |
| `EtlField.java`                | 删除   | 不再需要                                   |
| `FlinkTypeConverter.java`      | 删除   | 不再需要                                   |
| `job-config.schema.json`       | 更新   | 支持对象格式                                 |
| `csv-to-console.json`          | 更新   | 使用新格式                                  |

## 架构对比

**重构前：**

```
JSON → SchemaParser → EtlField(EtlFieldType)
                           ↓
                FlinkTypeConverter → TypeInformation
                           ↓
                TypeConverter(value, EtlFieldType)
```

**重构后：**

```
JSON → SchemaParser → EtlSchema(names[], types[])
                           ↓
                    直接用于 getProducedType()
                           ↓
                TypeConverter(value, TypeInformation)
```

## 配置格式变更

```json
// 旧格式（已废弃）
"schema": [
{"name": "id", "type": "LONG"},
{"name": "name", "type": "STRING"}
]

// 新格式
"schema": {
"id": "LONG",
"name": "STRING"
}
```