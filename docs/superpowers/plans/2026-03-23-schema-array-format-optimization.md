# Schema 基础类型数组格式优化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 schema 中基础类型数组的配置格式从 `"tags": "ARRAY<STRING>"` 改为 `"tags": ["STRING"]`，使其更加直观且与 JSON 惯例一致。

**Architecture:** 修改 `SchemaParser`，删除旧的 `ARRAY<TYPE>` 字符串格式，统一使用 `["TYPE"]` 数组格式。

**Tech Stack:** Java 11, Flink TypeInformation, JUnit 5

---

## 文件结构

| 文件 | 职责 | 操作 |
|------|------|------|
| `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java` | Schema 解析核心逻辑 | 修改 |
| `flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java` | 单元测试 | 修改 |
| `PLUGINS.md` | 插件配置文档 | 修改 |
| `CLAUDE.md` | 项目说明文档 | 修改 |

---

### Task 1: 修改 SchemaParser 支持新格式

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java`

- [ ] **Step 1: 写失败的测试用例**

在 `SchemaParserTest.java` 中添加新测试：

```java
@Test
void testParseArraySimpleTypeNewFormat() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    schemaConfig.put("tags", List.of("STRING"));
    schemaConfig.put("scores", List.of("INT"));

    EtlSchema schema = SchemaParser.parse(schemaConfig);

    assertEquals(2, schema.getFieldCount());

    // tags: ["STRING"] → BasicArrayTypeInfo
    TypeInformation<?> tagsType = schema.getFieldType(0);
    assertTrue(tagsType instanceof BasicArrayTypeInfo);
    BasicArrayTypeInfo<?, ?> arrayInfo = (BasicArrayTypeInfo<?, ?>) tagsType;
    assertEquals(Types.STRING, arrayInfo.getComponentInfo());

    // scores: ["INT"] → BasicArrayTypeInfo
    TypeInformation<?> scoresType = schema.getFieldType(1);
    assertTrue(scoresType instanceof BasicArrayTypeInfo);
    BasicArrayTypeInfo<?, ?> scoresArrayInfo = (BasicArrayTypeInfo<?, ?>) scoresType;
    assertEquals(Types.INT, scoresArrayInfo.getComponentInfo());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `mvn test -Dtest=SchemaParserTest#testParseArraySimpleTypeNewFormat -pl flink-etl-core`
Expected: FAIL - 当前不支持 `["STRING"]` 格式解析基础类型数组

- [ ] **Step 3: 修改 parseType 方法**

修改 `SchemaParser.java` 的 `parseType` 方法：

```java
@SuppressWarnings("unchecked")
private static TypeInformation<?> parseType(String fieldName, Object typeObj) {
    if (typeObj instanceof String) {
        return parseSimpleType(fieldName, (String) typeObj);
    } else if (typeObj instanceof Map) {
        // OBJECT 类型
        return parseObjectType(fieldName, (Map<String, Object>) typeObj);
    } else if (typeObj instanceof List) {
        // 数组类型：["STRING"] 或 [{"name": "STRING"}]
        return parseArrayType(fieldName, (List<?>) typeObj);
    } else {
        throw new SchemaConfigException(
            "字段 '" + fieldName + "' 的类型必须是字符串、对象或数组");
    }
}
```

- [ ] **Step 4: 删除旧的 parseStringType 方法，替换为 parseSimpleType**

删除原有的 `parseStringType` 方法（包含 `ARRAY<TYPE>` 解析逻辑），替换为简单的 `parseSimpleType`：

```java
/**
 * 解析简单类型
 */
private static TypeInformation<?> parseSimpleType(String fieldName, String typeName) {
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
```

- [ ] **Step 5: 删除旧的 parseArrayType(String, String) 方法**

删除原有的 `parseArrayType(String fieldName, String elementTypeStr)` 方法和相关常量：

```java
// 删除这些
private static final String ARRAY_TYPE_PREFIX = "ARRAY<";
private static final String TYPE_SUFFIX = ">";

private static TypeInformation<?> parseArrayType(String fieldName, String elementTypeStr) {
    // ... 整个方法删除
}
```

- [ ] **Step 6: 重命名 parseObjectArrayType 为 parseArrayType**

将原有的 `parseObjectArrayType(String fieldName, List<?> arrayDef)` 重命名为 `parseArrayType`，并增加基础类型数组支持：

```java
/**
 * 解析数组类型
 * 支持两种格式：
 * - 基础类型数组：["STRING"], ["INT"]
 * - 对象数组：[{"name": "STRING", "age": "INT"}]
 */
@SuppressWarnings("unchecked")
private static TypeInformation<?> parseArrayType(String fieldName, List<?> arrayDef) {
    if (arrayDef.size() != 1) {
        throw new SchemaConfigException(
            "字段 '" + fieldName + "' 的数组类型定义长度必须为 1");
    }

    Object elementDef = arrayDef.get(0);

    if (elementDef instanceof String) {
        // 基础类型数组：["STRING"]
        return parseBasicArrayType(fieldName, (String) elementDef);
    } else if (elementDef instanceof Map) {
        // 对象数组：[{"name": "STRING"}]
        TypeInformation<?> elementType = parseObjectType(fieldName + "[]", (Map<String, Object>) elementDef);
        return Types.OBJECT_ARRAY(elementType);
    } else {
        throw new SchemaConfigException(
            "字段 '" + fieldName + "' 的数组元素类型必须是字符串（基础类型）或对象");
    }
}

/**
 * 解析基础类型数组：["STRING"], ["INT"]
 */
private static TypeInformation<?> parseBasicArrayType(String fieldName, String elementTypeName) {
    TypeInformation<?> elementType = parseSimpleType(fieldName + "[]", elementTypeName);

    if (elementType instanceof BasicTypeInfo) {
        BasicTypeInfo<?> basicTypeInfo = (BasicTypeInfo<?>) elementType;
        return BasicArrayTypeInfo.getInfoFor(
            Array.newInstance(basicTypeInfo.getTypeClass(), 0).getClass()
        );
    }

    return Types.OBJECT_ARRAY(elementType);
}
```

- [ ] **Step 7: 更新类注释**

```java
/**
 * Schema 解析器
 * 从配置对象解析 EtlSchema
 *
 * <p>配置格式：
 * <ul>
 *   <li>简单类型：{ "fieldName": "TYPE" }</li>
 *   <li>基础类型数组：{ "tags": ["STRING"] }</li>
 *   <li>OBJECT 类型：{ "address": { "city": "STRING" } }</li>
 *   <li>对象数组：{ "friends": [{"name": "STRING"}] }</li>
 * </ul>
 *
 * <p>支持类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP
 */
```

- [ ] **Step 8: 运行测试确认通过**

Run: `mvn test -Dtest=SchemaParserTest#testParseArraySimpleTypeNewFormat -pl flink-etl-core`
Expected: PASS

- [ ] **Step 9: 添加异常情况测试用例**

```java
@Test
void testParseEmptyArrayThrowsException() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    schemaConfig.put("tags", List.of());
    assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
}

@Test
void testParseArrayWithInvalidBasicTypeThrowsException() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    schemaConfig.put("tags", List.of("INVALID"));
    assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
}

@Test
void testParseArrayWithMultipleElementsThrowsException() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    schemaConfig.put("tags", List.of("STRING", "INT"));
    assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
}

@Test
void testParseArrayWithNonStringElementThrowsException() {
    Map<String, Object> schemaConfig = new LinkedHashMap<>();
    schemaConfig.put("tags", List.of(123));
    assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
}
```

- [ ] **Step 10: 删除旧格式测试用例**

删除 `testParseArraySimpleType` 测试方法（测试旧的 `ARRAY<STRING>` 格式）。

- [ ] **Step 11: 运行所有测试确认通过**

Run: `mvn test -Dtest=SchemaParserTest -pl flink-etl-core`
Expected: PASS - 所有测试通过

- [ ] **Step 12: 提交代码**

```bash
git add flink-etl-core/src/main/java/com/etl/core/schema/SchemaParser.java flink-etl-core/src/test/java/com/etl/core/schema/SchemaParserTest.java
git commit -m "feat: schema 基础类型数组改用 [\"TYPE\"] 格式"
```

---

### Task 2: 更新文档

**Files:**
- Modify: `PLUGINS.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: 更新 PLUGINS.md 文档**

需要更新的位置：

| 行号 | 当前内容 | 更新为 |
|------|---------|--------|
| 232 | `"tags": "ARRAY<STRING>"` | `"tags": ["STRING"]` |
| 270-280 | `ARRAY<简单类型>` 章节 | 使用新格式说明 |
| 336 | `"hobby": "ARRAY<STRING>"` | `"hobby": ["STRING"]` |
| 339 | `"zipcodes": "ARRAY<INT>"` | `"zipcodes": ["INT"]` |
| 345 | `"tags": "ARRAY<STRING>"` | `"tags": ["STRING"]` |

将 `ARRAY<简单类型>` 章节标题改为 `基础类型数组`，内容更新为：

```markdown
### 基础类型数组

数组类型，元素为简单类型，使用 JSON 数组格式：

```json
{
  "schema": {
    "tags": ["STRING"],
    "scores": ["INT"]
  }
}
```
```

- [ ] **Step 2: 更新 CLAUDE.md 文档**

更新 schema 配置部分的类型说明：

```markdown
**类型说明：**
- `["简单类型"]` - 简单类型数组，如 `["STRING"]`、`["INT"]`
- `OBJECT` - 对象类型，使用 `{}` 定义子字段
- `ARRAY<OBJECT>` - 对象数组，使用 `[{}]` 定义，数组第一个元素定义对象结构
```

同时更新所有示例中的 `"ARRAY<STRING>"` 为 `["STRING"]`。

- [ ] **Step 3: 提交文档更新**

```bash
git add PLUGINS.md CLAUDE.md
git commit -m "docs: 更新 schema 基础类型数组格式文档"
```

---

### Task 3: 验证集成测试

**Files:**
- 无文件修改，仅运行测试

- [ ] **Step 1: 编译项目**

Run: `mvn clean compile`
Expected: BUILD SUCCESS

- [ ] **Step 2: 运行完整测试**

Run: `mvn test`
Expected: 所有测试通过

---

## 验收标准

- [ ] 新格式 `["STRING"]` 正确解析为基础类型数组
- [ ] 旧格式 `"ARRAY<STRING>"` 不再支持
- [ ] 所有现有测试通过
- [ ] 文档已更新
- [ ] 代码已提交