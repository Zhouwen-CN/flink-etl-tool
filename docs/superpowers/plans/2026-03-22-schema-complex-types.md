# Schema 复杂类型支持实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 schema 添加 ARRAY 和 OBJECT 类型支持，复用 Flink 已有的 TypeInformation 类型系统。

**Architecture:** 参考 `RowTypeParser` 的设计思路，`SchemaField` 只包含 `name` 和 `TypeInformation<?> type`，复杂类型直接用
`RowTypeInfo` 和 `ObjectArrayTypeInfo` 表示。

**Tech Stack:** Java 11, Flink TypeInformation (RowTypeInfo, ObjectArrayTypeInfo, BasicArrayTypeInfo)

---

## 设计思路

参考 `RowTypeParser.java` 的实现：

- `Field` 只有 `name` 和 `TypeInformation<?> type` 两个字段
- OBJECT 类型 → `RowTypeInfo`，可通过 `getFieldNames()` 和 `getFieldTypes()` 获取字段信息
- ARRAY<简单类型> → `BasicArrayTypeInfo`
- ARRAY<OBJECT> → `ObjectArrayTypeInfo<RowTypeInfo>`

---

## 配置格式

### 简单类型（兼容现有格式）

```json
{
  "schema": {
    "id": "LONG",
    "name": "STRING",
    "age": "INT"
  }
}
```

### ARRAY<简单类型>

```json
{
  "schema": {
    "tags": "ARRAY<STRING>",
    "scores": "ARRAY<INT>"
  }
}
```

### OBJECT 类型

```json
{
  "schema": {
    "address": {
      "city": "STRING",
      "street": "STRING"
    }
  }
}
```

### OBJECT 嵌套 ARRAY

```json
{
  "schema": {
    "address": {
      "city": "STRING",
      "zipcodes": "ARRAY<INT>"
    }
  }
}
```

### ARRAY<OBJECT>

```json
{
  "schema": {
    "friends": [
      {"name": "STRING", "age": "INT"}
    ]
  }
}
```

### 完整示例

```json
{
  "schema": {
    "id": "LONG",
    "name": "STRING",
    "hobby": "ARRAY<STRING>",
    "address": {
      "city": "STRING",
      "zipcodes": "ARRAY<INT>"
    },
    "friends": [
      {
        "name": "STRING",
        "age": "INT",
        "tags": "ARRAY<STRING>"
      }
    ]
  }
}
```

---

## 文件结构

**修改文件：**

- `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java` - 扩展解析逻辑
- `flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java` - 添加复杂类型测试
- `flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java` -
  添加复杂类型校验

**无需修改：**

- `EtlSchema.java` - 已有 `fieldNames` 和 `fieldTypes` 数组足够，`fieldTypes` 中可存储 `RowTypeInfo` 和
  `ObjectArrayTypeInfo`

---

## Task 1: 扩展 SchemaParser 支持复杂类型

**Files:**

- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java`
- Test: `flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java`

- [ ] **Step 1: Write the failing tests for complex types**

在 `SchemaParserTest.java` 中添加：

```java
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.LinkedHashMap;
import java.util.List;

// 新增测试方法

@Test
void testParseArraySimpleType() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    schemaConfig.put("tags", "ARRAY<STRING>");
    schemaConfig.put("scores", "ARRAY<INT>");

    EtlSchema schema = SchemaParser.parse(schemaConfig);

    assertEquals(2, schema.getFieldCount());

    // tags: ARRAY<STRING> → BasicArrayTypeInfo
    TypeInformation<?> tagsType = schema.getFieldType(0);
    assertTrue(tagsType instanceof BasicArrayTypeInfo);
    BasicArrayTypeInfo<?, ?> arrayInfo = (BasicArrayTypeInfo<?, ?>) tagsType;
    assertEquals(Types.STRING, arrayInfo.getComponentInfo());

    // scores: ARRAY<INT>
    TypeInformation<?> scoresType = schema.getFieldType(1);
    assertTrue(scoresType instanceof BasicArrayTypeInfo);
}

@Test
void testParseObjectType() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    Map<String, Object> addressDef = new LinkedHashMap<>();
    addressDef.put("city", "STRING");
    addressDef.put("street", "STRING");
    schemaConfig.put("address", addressDef);

    EtlSchema schema = SchemaParser.parse(schemaConfig);

    // address: OBJECT → RowTypeInfo
    TypeInformation<?> addressType = schema.getFieldType(0);
    assertTrue(addressType instanceof RowTypeInfo);

    RowTypeInfo rowTypeInfo = (RowTypeInfo) addressType;
    assertArrayEquals(new String[]{"city", "street"}, rowTypeInfo.getFieldNames());
    assertEquals(Types.STRING, rowTypeInfo.getFieldTypes()[0]);
    assertEquals(Types.STRING, rowTypeInfo.getFieldTypes()[1]);
}

@Test
void testParseObjectWithNestedArray() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    Map<String, Object> addressDef = new LinkedHashMap<>();
    addressDef.put("city", "STRING");
    addressDef.put("zipcodes", "ARRAY<INT>");
    schemaConfig.put("address", addressDef);

    EtlSchema schema = SchemaParser.parse(schemaConfig);

    RowTypeInfo addressType = (RowTypeInfo) schema.getFieldType(0);

    // zipcodes 字段是 ARRAY<INT>
    TypeInformation<?> zipcodesType = addressType.getFieldTypes()[1];
    assertTrue(zipcodesType instanceof BasicArrayTypeInfo);
}

@Test
void testParseArrayObjectType() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();

    // friends: [{name: STRING, age: INT}]
    List<Map<String, Object>> friendsDef = List.of(
        new LinkedHashMap<String, Object>() {{
            put("name", "STRING");
            put("age", "INT");
        }}
    );
    schemaConfig.put("friends", friendsDef);

    EtlSchema schema = SchemaParser.parse(schemaConfig);

    // friends: ARRAY<OBJECT> → ObjectArrayTypeInfo<RowTypeInfo>
    TypeInformation<?> friendsType = schema.getFieldType(0);
    assertTrue(friendsType instanceof ObjectArrayTypeInfo);

    ObjectArrayTypeInfo<?, ?> arrayInfo = (ObjectArrayTypeInfo<?, ?>) friendsType;
    RowTypeInfo elementTypeInfo = (RowTypeInfo) arrayInfo.getComponentInfo();

    assertArrayEquals(new String[]{"name", "age"}, elementTypeInfo.getFieldNames());
}

@Test
void testParseNestedArrayInObjectArray() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();

    List<Map<String, Object>> friendsDef = List.of(
        new LinkedHashMap<String, Object>() {{
            put("name", "STRING");
            put("tags", "ARRAY<STRING>");
        }}
    );
    schemaConfig.put("friends", friendsDef);

    EtlSchema schema = SchemaParser.parse(schemaConfig);

    ObjectArrayTypeInfo<?, ?> friendsType = (ObjectArrayTypeInfo<?, ?>) schema.getFieldType(0);
    RowTypeInfo friendType = (RowTypeInfo) friendsType.getComponentInfo();

    // tags 字段是 ARRAY<STRING>
    TypeInformation<?> tagsType = friendType.getFieldTypes()[1];
    assertTrue(tagsType instanceof BasicArrayTypeInfo);
}

@Test
void testParseCompleteNestedStructure() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    schemaConfig.put("id", "LONG");
    schemaConfig.put("hobby", "ARRAY<STRING>");

    // address: {city: STRING, zipcodes: ARRAY<INT>}
    Map<String, Object> addressDef = new LinkedHashMap<>();
    addressDef.put("city", "STRING");
    addressDef.put("zipcodes", "ARRAY<INT>");
    schemaConfig.put("address", addressDef);

    // friends: [{name: STRING, tags: ARRAY<STRING>}]
    List<Map<String, Object>> friendsDef = List.of(
        new LinkedHashMap<String, Object>() {{
            put("name", "STRING");
            put("tags", "ARRAY<STRING>");
        }}
    );
    schemaConfig.put("friends", friendsDef);

    EtlSchema schema = SchemaParser.parse(schemaConfig);

    assertEquals(4, schema.getFieldCount());

    // id: LONG
    assertEquals(Types.LONG, schema.getFieldType(0));

    // hobby: ARRAY<STRING>
    assertTrue(schema.getFieldType(1) instanceof BasicArrayTypeInfo);

    // address: RowTypeInfo
    RowTypeInfo addressType = (RowTypeInfo) schema.getFieldType(2);
    assertTrue(addressType.getFieldTypes()[1] instanceof BasicArrayTypeInfo);

    // friends: ObjectArrayTypeInfo<RowTypeInfo>
    ObjectArrayTypeInfo<?, ?> friendsType = (ObjectArrayTypeInfo<?, ?>) schema.getFieldType(3);
    RowTypeInfo friendType = (RowTypeInfo) friendsType.getComponentInfo();
    assertTrue(friendType.getFieldTypes()[1] instanceof BasicArrayTypeInfo);
}

@Test
void testParseInvalidArrayFormat() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    schemaConfig.put("tags", "ARRAY<INVALID>");

    assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool && mvn test -pl flink-etl-core -Dtest=SchemaParserTest -q`
Expected: FAIL

- [ ] **Step 3: Update SchemaParser to support complex types**

```java
package com.etl.core.schema;

import com.etl.core.exception.SchemaConfigException;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Schema 解析器
 * 从配置对象解析 EtlSchema
 *
 * <p>支持的格式：
 * <ul>
 *   <li>简单类型：{ "fieldName": "TYPE" }</li>
 *   <li>ARRAY&lt;简单类型&gt;：{ "fieldName": "ARRAY&lt;STRING&gt;" }</li>
 *   <li>OBJECT 类型：{ "fieldName": { "subField": "TYPE" } }</li>
 *   <li>ARRAY&lt;OBJECT&gt;：{ "fieldName": [{ "subField": "TYPE" }] }</li>
 * </ul>
 *
 * <p>支持类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP
 *
 * <p>类型映射：
 * <ul>
 *   <li>简单类型 → BasicTypeInfo (Types.STRING, Types.INT 等)</li>
 *   <li>ARRAY&lt;简单类型&gt; → BasicArrayTypeInfo</li>
 *   <li>OBJECT → RowTypeInfo</li>
 *   <li>ARRAY&lt;OBJECT&gt; → ObjectArrayTypeInfo&lt;RowTypeInfo&gt;</li>
 * </ul>
 */
public class SchemaParser {

    private static final Pattern ARRAY_PATTERN = Pattern.compile("^ARRAY<(.+)>$", Pattern.CASE_INSENSITIVE);

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

    private static EtlSchema parseObjectFormat(Map<String, Object> schemaConfig) {
        List<String> names = new ArrayList<>();
        List<TypeInformation<?>> types = new ArrayList<>();

        Map<String, Object> orderedConfig = schemaConfig instanceof LinkedHashMap
            ? schemaConfig
            : new LinkedHashMap<>(schemaConfig);

        for (Map.Entry<String, Object> entry : orderedConfig.entrySet()) {
            String fieldName = entry.getKey();
            Object typeObj = entry.getValue();
            TypeInformation<?> type = parseType(fieldName, typeObj);
            names.add(fieldName);
            types.add(type);
        }

        return new EtlSchema(
            names.toArray(new String[0]),
            types.toArray(new TypeInformation<?>[0])
        );
    }

    /**
     * 解析字段类型
     */
    @SuppressWarnings("unchecked")
    private static TypeInformation<?> parseType(String fieldName, Object typeObj) {
        // 字符串：简单类型 或 ARRAY<ELEMENT>
        if (typeObj instanceof String) {
            return parseStringType(fieldName, (String) typeObj);
        }

        // Map：OBJECT 类型
        if (typeObj instanceof Map) {
            return parseObjectType(fieldName, (Map<String, Object>) typeObj);
        }

        // List：ARRAY<OBJECT> 类型
        if (typeObj instanceof List) {
            return parseObjectArrayType(fieldName, (List<?>) typeObj);
        }

        throw new SchemaConfigException(
            "字段 '" + fieldName + "' 的类型必须是字符串、对象或数组");
    }

    /**
     * 解析字符串类型
     */
    private static TypeInformation<?> parseStringType(String fieldName, String typeStr) {
        // 检查是否是 ARRAY<ELEMENT> 格式
        Matcher matcher = ARRAY_PATTERN.matcher(typeStr.trim());
        if (matcher.matches()) {
            String elementType = matcher.group(1).trim();
            return parseArrayType(fieldName, elementType);
        }

        // 简单类型
        return parseSimpleType(fieldName, typeStr);
    }

    /**
     * 解析 ARRAY 类型
     */
    private static TypeInformation<?> parseArrayType(String fieldName, String elementTypeStr) {
        TypeInformation<?> elementType = parseSimpleType(fieldName, elementTypeStr);

        // 简单类型数组 → BasicArrayTypeInfo
        if (elementType instanceof BasicTypeInfo) {
            BasicTypeInfo<?> basicTypeInfo = (BasicTypeInfo<?>) elementType;
            return BasicArrayTypeInfo.getInfoFor(
                Array.newInstance(basicTypeInfo.getTypeClass(), 0).getClass()
            );
        }

        // 其他类型数组 → ObjectArrayTypeInfo
        return Types.OBJECT_ARRAY(elementType);
    }

    /**
     * 解析 OBJECT 类型
     */
    @SuppressWarnings("unchecked")
    private static TypeInformation<?> parseObjectType(String fieldName, Map<String, Object> objectDef) {
        List<String> names = new ArrayList<>();
        List<TypeInformation<?>> types = new ArrayList<>();

        Map<String, Object> orderedDef = objectDef instanceof LinkedHashMap
            ? objectDef
            : new LinkedHashMap<>(objectDef);

        for (Map.Entry<String, Object> entry : orderedDef.entrySet()) {
            String subFieldName = entry.getKey();
            Object subTypeObj = entry.getValue();
            TypeInformation<?> subType = parseType(subFieldName, subTypeObj);
            names.add(subFieldName);
            types.add(subType);
        }

        return Types.ROW_NAMED(
            names.toArray(new String[0]),
            types.toArray(new TypeInformation<?>[0])
        );
    }

    /**
     * 解析 ARRAY<OBJECT> 类型
     */
    @SuppressWarnings("unchecked")
    private static TypeInformation<?> parseObjectArrayType(String fieldName, List<?> arrayDef) {
        if (arrayDef.isEmpty()) {
            throw new SchemaConfigException(
                "字段 '" + fieldName + "' 的对象数组定义不能为空，" +
                "需要提供对象结构定义，如: [{\"field\": \"TYPE\"}]");
        }

        Object firstElement = arrayDef.get(0);
        if (!(firstElement instanceof Map)) {
            throw new SchemaConfigException(
                "字段 '" + fieldName + "' 的数组元素必须是对象");
        }

        // 解析对象类型，返回 ObjectArrayTypeInfo
        TypeInformation<?> elementType = parseObjectType(fieldName, (Map<String, Object>) firstElement);
        return Types.OBJECT_ARRAY(elementType);
    }

    /**
     * 解析简单类型
     */
    private static TypeInformation<?> parseSimpleType(String fieldName, String typeStr) {
        switch (typeStr.toUpperCase()) {
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
                    "字段 '" + fieldName + "' 的类型 '" + typeStr + "' 不支持，" +
                    "支持的类型: STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool && mvn test -pl flink-etl-core -Dtest=SchemaParserTest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java
git commit -m "feat: SchemaParser 支持 ARRAY 和 OBJECT 类型解析"
```

---

## Task 2: 更新 CsvFormatPlugin 添加复杂类型校验

**Files:**

- Modify:
  `flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java`

- [ ] **Step 1: 添加复杂类型校验**

CSV 格式不支持复杂类型，需要添加校验：

```java
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

// 在 parse 方法中添加校验
@Override
public Iterable<Row> parse(LocalFileSourceConfig localFileSourceConfig, InputStream inputStream) {
    EtlSchema schema = localFileSourceConfig.getSchema();

    // 校验：CSV 格式不支持复杂类型
    validateSchema(schema);

    // ... 其余代码不变
}

private void validateSchema(EtlSchema schema) {
    if (schema == null) {
        throw new SchemaConfigException("CSV Source 必须配置 schema");
    }
    for (int i = 0; i < schema.getFieldCount(); i++) {
        TypeInformation<?> type = schema.getFieldType(i);
        if (isComplexType(type)) {
            throw new SchemaConfigException(
                "CSV 格式不支持复杂类型字段 '" + schema.getFieldName(i) + "'。" +
                "请使用简单类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
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
```

- [ ] **Step 2: Run tests to verify**

Run: `cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool && mvn test -pl flink-etl-source/flink-etl-source-localfile -q`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add flink-etl-source/flink-etl-source-localfile/src/main/java/com/etl/source/localfile/format/CsvFormatPlugin.java
git commit -m "feat: CsvFormatPlugin 添加复杂类型校验"
```

---

## Task 3: 更新文档

**Files:**

- Modify: `PLUGINS.md`

- [ ] **Step 1: 更新 schema 配置说明**

添加复杂类型配置说明（见上文配置格式部分）

- [ ] **Step 2: Commit**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md 添加复杂类型 schema 说明"
```

---

## Task 4: 运行全量测试

- [ ] **Step 1: 运行所有单元测试**

Run: `cd /Users/chenzhouwen/IdeaProjects/flink-etl-tool && mvn test -q`
Expected: PASS

- [ ] **Step 2: 最终提交**

```bash
git add -A
git commit -m "feat: schema 支持 ARRAY 和 OBJECT 类型解析

- SchemaParser 支持 ARRAY<STRING> 和 ARRAY<OBJECT> 语法
- OBJECT 类型使用 {} 定义，ARRAY<OBJECT> 使用 [{}] 定义
- 复杂类型直接使用 Flink TypeInformation (RowTypeInfo, ObjectArrayTypeInfo)
- CSV 格式添加复杂类型校验"
```

---

## 验证清单

- [ ] SchemaParser 正确解析简单类型配置
- [ ] SchemaParser 正确解析 ARRAY<STRING> 配置 → BasicArrayTypeInfo
- [ ] SchemaParser 正确解析 ARRAY<OBJECT> 配置 → ObjectArrayTypeInfo
- [ ] SchemaParser 正确解析 OBJECT 类型配置 → RowTypeInfo
- [ ] SchemaParser 正确解析 OBJECT 嵌套 ARRAY 结构
- [ ] SchemaParser 正确解析 ARRAY<OBJECT> 嵌套 ARRAY 结构
- [ ] EtlSchema 无需修改，fieldTypes 可存储复杂 TypeInformation
- [ ] CsvFormatPlugin 正确校验复杂类型
- [ ] 所有单元测试通过
- [ ] 文档已更新