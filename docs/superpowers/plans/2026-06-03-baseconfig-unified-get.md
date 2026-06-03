# BaseConfig 统一 get(key, Class<T>) 接口实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `BaseConfig` 现有按类型命名的 11 个 getter 全部替换为统一的泛型入口 `get(key, Class<T>)`，并迁移项目内全部调用点。

**Architecture:** 在 `BaseConfig` 内用 if-else 链分派（恒等比较 `clazz`），每个支持的类型分支内联保留旧 getter 的转换语义；不支持的类型抛 `IllegalArgumentException`。所有调用点同步替换为新形式，保证 `mvn clean test` 编译并通过。

**Tech Stack:** Java 1.8、Apache Flink 1.15.2、Maven、JUnit 5、Lombok。

**关联设计文档:** [docs/superpowers/specs/2026-06-03-baseconfig-unified-get-design.md](../specs/2026-06-03-baseconfig-unified-get-design.md)

---

## 文件结构

### 改动 (Modify)

- `flink-etl-core/src/main/java/com/etl/core/config/BaseConfig.java` — 重写公共 API
- `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/config/JdbcSinkConfig.java` — 10 处调用替换
- `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/config/JdbcSourceConfig.java` — 9 处
- `flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/config/HttpSourceConfig.java` — 8 处
- `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/config/MySqlCdcConfig.java` — 8 处
- `flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/config/LocalFileSourceConfig.java` — 7 处
- `flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/source/config/KafkaSourceConfig.java` — 7 处
- `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java` — 6 处
- `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/config/MqttSourceConfig.java` — 5 处
- `flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/sink/config/KafkaSinkConfig.java` — 4 处
- `flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/config/MockSourceConfig.java` — 3 处
- `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/NumericSplitter.java` — 1 处（注意：还有 2 处 `rs.getLong`/`rs.getObject` 属 JDBC ResultSet API，**不改**）
- `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/JdbcSplitReader.java` — 0 处（仅 `rs.getObject` 属 ResultSet，**不改**；已列出仅为提醒人工核对）
- `flink-etl-transform/src/main/java/com/etl/transform/SqlTransformPlugin.java` — 1 处
- `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java` — 1 处
- `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/executor/BufferReducedExecutorTest.java` — 3 处

### 新增 (Create)

- `flink-etl-core/src/test/java/com/etl/core/config/BaseConfigTest.java` — `BaseConfig` 单元测试

### 不动 (Skip - 假阳性)

- `JdbcSplitReader.java:133` — `currentResultSet.getObject(i + 1)` 是 JDBC ResultSet API
- `NumericSplitter.java:121,124,125` — `rs.getObject(1)` 与 `rs.getLong(1/2)` 是 JDBC ResultSet API

---

## Task 1: 在 BaseConfig 中先建立单元测试骨架 (TDD)

新建测试类，先写测试覆盖目标行为；这一步只新增文件、不动 `BaseConfig`，让所有测试因新 API 不存在而**编译失败**——这就是 TDD 的红灯。

**Files:**
- Create: `flink-etl-core/src/test/java/com/etl/core/config/BaseConfigTest.java`

- [ ] **Step 1: 写测试类**

完整内容写入 `flink-etl-core/src/test/java/com/etl/core/config/BaseConfigTest.java`：

```java
package com.etl.core.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaseConfigTest {

    private TestConfig conf;

    @BeforeEach
    void setUp() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("strKey", "hello");
        map.put("intKey", 42);
        map.put("intStrKey", "100");
        map.put("longKey", 123L);
        map.put("longFromInt", 7);
        map.put("longStr", "9999999999");
        map.put("boolKey", true);
        map.put("boolStr", "true");
        map.put("listKey", Arrays.asList("a", 1, true));
        map.put("mapKey", new LinkedHashMap<>(new HashMap<String, Object>() {{
            put("k1", "v1");
            put("k2", 2);
        }}));
        map.put("badInt", "not-a-number");
        map.put("notList", "x");
        map.put("notMap", "x");
        conf = new TestConfig(map);
    }

    // ---------- String ----------
    @Test
    void getString_returnsString() {
        assertEquals("hello", conf.get("strKey", String.class));
    }

    @Test
    void getString_coercesNonString() {
        assertEquals("42", conf.get("intKey", String.class));
    }

    @Test
    void getString_missingReturnsNull() {
        assertNull(conf.get("missing", String.class));
    }

    @Test
    void getString_default() {
        assertEquals("x", conf.get("missing", String.class, "x"));
        assertEquals("hello", conf.get("strKey", String.class, "x"));
    }

    // ---------- Integer ----------
    @Test
    void getInteger_returnsInteger() {
        assertEquals(Integer.valueOf(42), conf.get("intKey", Integer.class));
    }

    @Test
    void getInteger_parsesString() {
        assertEquals(Integer.valueOf(100), conf.get("intStrKey", Integer.class));
    }

    @Test
    void getInteger_missingReturnsNull() {
        assertNull(conf.get("missing", Integer.class));
    }

    @Test
    void getInteger_invalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("badInt", Integer.class));
    }

    @Test
    void getInteger_default() {
        assertEquals(Integer.valueOf(5), conf.get("missing", Integer.class, 5));
    }

    @Test
    void getInteger_defaultDoesNotSwallowConversionError() {
        assertThrows(IllegalArgumentException.class,
                () -> conf.get("badInt", Integer.class, 99));
    }

    // ---------- Long ----------
    @Test
    void getLong_returnsLong() {
        assertEquals(Long.valueOf(123L), conf.get("longKey", Long.class));
    }

    @Test
    void getLong_promotesInteger() {
        assertEquals(Long.valueOf(7L), conf.get("longFromInt", Long.class));
    }

    @Test
    void getLong_parsesString() {
        assertEquals(Long.valueOf(9999999999L), conf.get("longStr", Long.class));
    }

    @Test
    void getLong_invalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("badInt", Long.class));
    }

    @Test
    void getLong_default() {
        assertEquals(Long.valueOf(10L), conf.get("missing", Long.class, 10L));
    }

    // ---------- Boolean ----------
    @Test
    void getBoolean_returnsBoolean() {
        assertTrue(conf.get("boolKey", Boolean.class));
    }

    @Test
    void getBoolean_parsesString() {
        assertTrue(conf.get("boolStr", Boolean.class));
    }

    @Test
    void getBoolean_missingReturnsNull() {
        assertNull(conf.get("missing", Boolean.class));
    }

    @Test
    void getBoolean_default() {
        assertFalse(conf.get("missing", Boolean.class, false));
        assertTrue(conf.get("boolKey", Boolean.class, false));
    }

    // ---------- List ----------
    @Test
    void getList_returnsStringList() {
        List<String> list = conf.get("listKey", List.class);
        assertEquals(Arrays.asList("a", "1", "true"), list);
    }

    @Test
    void getList_missingReturnsNull() {
        assertNull(conf.get("missing", List.class));
    }

    @Test
    void getList_notListThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("notList", List.class));
    }

    // ---------- Map ----------
    @Test
    void getMap_returnsMap() {
        Map<String, Object> map = conf.get("mapKey", Map.class);
        assertEquals("v1", map.get("k1"));
        assertEquals(2, map.get("k2"));
    }

    @Test
    void getMap_missingReturnsNull() {
        assertNull(conf.get("missing", Map.class));
    }

    @Test
    void getMap_notMapThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("notMap", Map.class));
    }

    // ---------- 不支持的类型 ----------
    @Test
    void unsupportedClassThrows() {
        assertThrows(IllegalArgumentException.class, () -> conf.get("strKey", Date.class));
    }

    // ---------- get(key) ----------
    @Test
    void getRaw_returnsObject() {
        assertEquals(42, conf.get("intKey"));
    }

    @Test
    void getRaw_missingReturnsNull() {
        assertNull(conf.get("missing"));
    }

    // ---------- contains ----------
    @Test
    void contains_works() {
        assertTrue(conf.contains("strKey"));
        assertFalse(conf.contains("missing"));
    }

    // ---------- null config ----------
    @Test
    void nullConfig_allMethodsReturnNullOrDefault() {
        TestConfig empty = new TestConfig(null);
        assertNull(empty.get("k", String.class));
        assertEquals("x", empty.get("k", String.class, "x"));
        assertNull(empty.get("k"));
        assertFalse(empty.contains("k"));
    }

    /** 测试用具体子类 */
    static class TestConfig extends BaseConfig {
        TestConfig(Map<String, Object> map) {
            super(map);
        }
    }
}
```

- [ ] **Step 2: 确认测试因 API 不存在而编译失败**

Run: `mvn -pl flink-etl-core test-compile`
Expected: 编译失败，错误信息包含 `cannot find symbol method get(...)` 之类提示 —— 这是 TDD 红灯。

- [ ] **Step 3: 暂不提交**

测试代码暂留工作区，等 BaseConfig 改完一起提交。

---

## Task 2: 重写 BaseConfig，删旧 getter、加新泛型 API

直接覆盖 `BaseConfig.java`：删除全部按类型命名的 getter，新增 `get(key, Class)` / `get(key, Class, default)` / `get(key)`，保留 `contains`。

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/config/BaseConfig.java`

- [ ] **Step 1: 整体替换 BaseConfig 文件内容**

把 `flink-etl-core/src/main/java/com/etl/core/config/BaseConfig.java` 整个文件替换为：

```java
package com.etl.core.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 配置基类
 * 提供统一的 get(key, Class) 接口
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class BaseConfig implements Serializable {

    private Map<String, Object> config;

    /**
     * 根据期望类型获取配置值
     *
     * <p>支持的类型：String、Integer、Long、Boolean、List、Map。
     * 不支持的类型抛 IllegalArgumentException。
     * 当 config 为 null、key 不存在或值为 null 时返回 null。
     *
     * @param key   配置键
     * @param clazz 期望的返回类型
     * @param <T>   返回类型参数
     * @return 转换后的配置值；不存在时返回 null
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> clazz) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        if (value == null) {
            return null;
        }
        if (clazz == String.class) {
            return (T) String.valueOf(value);
        }
        if (clazz == Integer.class) {
            if (value instanceof Integer) {
                return (T) value;
            }
            try {
                return (T) Integer.valueOf(Integer.parseInt(String.valueOf(value)));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "配置项 '" + key + "' 的值 '" + value + "' 无法转换为整数", e);
            }
        }
        if (clazz == Long.class) {
            if (value instanceof Long) {
                return (T) value;
            }
            if (value instanceof Integer) {
                return (T) Long.valueOf(((Integer) value).longValue());
            }
            try {
                return (T) Long.valueOf(Long.parseLong(String.valueOf(value)));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "配置项 '" + key + "' 的值 '" + value + "' 无法转换为长整数", e);
            }
        }
        if (clazz == Boolean.class) {
            if (value instanceof Boolean) {
                return (T) value;
            }
            return (T) Boolean.valueOf(Boolean.parseBoolean(String.valueOf(value)));
        }
        if (clazz == List.class) {
            if (!(value instanceof List<?>)) {
                throw new IllegalArgumentException("配置项 '" + key + "' 不是列表类型");
            }
            List<String> list = new ArrayList<>();
            for (Object item : (List<?>) value) {
                list.add(String.valueOf(item));
            }
            return (T) list;
        }
        if (clazz == Map.class) {
            if (!(value instanceof Map<?, ?>)) {
                throw new IllegalArgumentException("配置项 '" + key + "' 不是映射类型");
            }
            Map<String, Object> map = new LinkedHashMap<>();
            ((Map<?, ?>) value).forEach((k, v) -> map.put(String.valueOf(k), v));
            return (T) map;
        }
        throw new IllegalArgumentException("不支持的类型: " + clazz.getName());
    }

    /**
     * 根据期望类型获取配置值，未设置时返回默认值
     *
     * <p>类型转换异常仍然抛出，不会被默认值掩盖。
     */
    public <T> T get(String key, Class<T> clazz, T defaultValue) {
        T value = get(key, clazz);
        return value != null ? value : defaultValue;
    }

    /**
     * 直接获取原始对象（不做类型转换）
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }

    /**
     * 检查配置项是否存在
     */
    public boolean contains(String key) {
        return config != null && config.containsKey(key);
    }
}
```

- [ ] **Step 2: 编译 BaseConfig 与其测试**

Run: `mvn -pl flink-etl-core test-compile`
Expected: 编译通过（如果有其它 core 模块代码引用了已删除的 getter，会在此暴露——按调用点替换处理；本计划已确认 `flink-etl-core` 内只有 `SqlUtils.java` 一处需改，留到 Task 3）。

如果因 `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java` 调用 `config.getString(...)` 报错，先跳到 Task 3 把该文件改完再回来编译。

- [ ] **Step 3: 运行 BaseConfigTest**

Run: `mvn -pl flink-etl-core test -Dtest=BaseConfigTest`
Expected: 全部测试通过（绿灯）。

- [ ] **Step 4: 暂不提交**

`flink-etl-core` 改完后会有大量下游模块编译失败（调用点未替换），待 Task 3–14 全部完成后再统一提交。

---

## Task 3: 替换 flink-etl-core 内调用点（SqlUtils）

**Files:**
- Modify: `flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java`

- [ ] **Step 1: 查看当前调用**

Run: `grep -n "getString\|getInteger\|getBoolean\|getLong\|getList\|getMap\|getObject" flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java`
Expected: 输出 1 行 `getString` 调用。

- [ ] **Step 2: 用 Edit 工具替换**

把唯一一处 `config.getString("xxx")` 替换为 `config.get("xxx", String.class)`（保留原 key）。`config.getString("xxx", default)` 形式则替换为 `config.get("xxx", String.class, default)`。

- [ ] **Step 3: 编译 flink-etl-core**

Run: `mvn -pl flink-etl-core compile`
Expected: 编译通过。

---

## Task 4: 替换 connector-jdbc 调用点

**Files:**
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/config/JdbcSinkConfig.java`
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/config/JdbcSourceConfig.java`
- Modify: `flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/NumericSplitter.java`
- Modify: `flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/executor/BufferReducedExecutorTest.java`

- [ ] **Step 1: 查看每个文件的调用点（先看清楚再改）**

Run:
```
grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" \
  flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/config/JdbcSinkConfig.java \
  flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/config/JdbcSourceConfig.java \
  flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/NumericSplitter.java \
  flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/executor/BufferReducedExecutorTest.java
```
Expected: 看到形如 `config.getString("url")`、`config.getInteger("batchSize", default)`、`config.getList("keyFields")`、`config.getLong("batchIntervalMs", 0L)` 等。

⚠️ **不要**改任何 `rs.getXxx(...)` 调用，它们是 JDBC `ResultSet` API。`NumericSplitter.java` 内 `rs.getObject(1)`、`rs.getLong(1)`、`rs.getLong(2)` 必须保留原样；该文件应只有 1 处 `config.getXxx` 调用需要替换。

- [ ] **Step 2: 按映射表逐处替换**

替换映射（用 Edit 工具，逐文件逐行）：

| 旧 | 新 |
|---|---|
| `config.getString("k")` | `config.get("k", String.class)` |
| `config.getString("k", v)` | `config.get("k", String.class, v)` |
| `config.getInteger("k")` | `config.get("k", Integer.class)` |
| `config.getInteger("k", v)` | `config.get("k", Integer.class, v)` |
| `config.getLong("k")` | `config.get("k", Long.class)` |
| `config.getLong("k", v)` | `config.get("k", Long.class, v)` |
| `config.getBoolean("k", v)` | `config.get("k", Boolean.class, v)` |
| `config.getList("k")` | `config.get("k", List.class)` |
| `config.getMap("k")` | `config.get("k", Map.class)` |
| `config.getObject("k")` | `config.get("k")` |

注意：`config.get("k", List.class)` / `config.get("k", Map.class)` 在 Java 1.8 下会有 unchecked 警告——如果原代码就有 `@SuppressWarnings("unchecked")` 注解，保留；没有则不新增（项目中各 Config 类已习惯接受这一警告）。

- [ ] **Step 3: 编译 connector-jdbc**

Run: `mvn -pl flink-etl-connector/connector-jdbc -am compile`
Expected: 编译通过。

- [ ] **Step 4: 编译 connector-jdbc 测试**

Run: `mvn -pl flink-etl-connector/connector-jdbc -am test-compile`
Expected: 编译通过。

---

## Task 5: 替换 connector-http 调用点

**Files:**
- Modify: `flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/config/HttpSourceConfig.java`

- [ ] **Step 1: 查看调用点**

Run: `grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/config/HttpSourceConfig.java`
Expected: 8 行输出，包括 `getString` × 4、`getInteger` × 1（如有）、`getMap` × 2、`getObject` × 1。

- [ ] **Step 2: 按映射表替换**

替换映射（与 Task 4 一致，此处再列以便独立阅读）：

| 旧 | 新 |
|---|---|
| `config.getString("k")` | `config.get("k", String.class)` |
| `config.getString("k", v)` | `config.get("k", String.class, v)` |
| `config.getInteger("k")` | `config.get("k", Integer.class)` |
| `config.getInteger("k", v)` | `config.get("k", Integer.class, v)` |
| `config.getLong("k")` | `config.get("k", Long.class)` |
| `config.getLong("k", v)` | `config.get("k", Long.class, v)` |
| `config.getBoolean("k", v)` | `config.get("k", Boolean.class, v)` |
| `config.getList("k")` | `config.get("k", List.class)` |
| `config.getMap("k")` | `config.get("k", Map.class)` |
| `config.getObject("k")` | `config.get("k")` |

⚠️ `Object object = config.getObject("body");` → `Object object = config.get("body");`（不带 Class 参数的重载）。

- [ ] **Step 3: 编译 connector-http**

Run: `mvn -pl flink-etl-connector/connector-http -am compile`
Expected: 编译通过。

---

## Task 6: 替换 connector-cdc 调用点

**Files:**
- Modify: `flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/config/MySqlCdcConfig.java`

- [ ] **Step 1: 查看调用点**

Run: `grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/config/MySqlCdcConfig.java`
Expected: 8 行。

- [ ] **Step 2: 按映射表替换**

注意 `MySqlCdcConfig.java:59` 的形式：`config.getInteger("serverId", generateAutoServerId())` → `config.get("serverId", Integer.class, generateAutoServerId())`。

- [ ] **Step 3: 编译**

Run: `mvn -pl flink-etl-connector/connector-cdc -am compile`
Expected: 编译通过。

---

## Task 7: 替换 connector-localfile 调用点

**Files:**
- Modify: `flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/config/LocalFileSourceConfig.java`

- [ ] **Step 1: 查看调用点**

Run: `grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/config/LocalFileSourceConfig.java`
Expected: 7 行，含 `getBoolean` × 2、`getInteger` × 1、`getString` × 余下。

- [ ] **Step 2: 按映射表替换**

注意 `boolean recursive = config.getBoolean("recursive", false);` → `boolean recursive = config.get("recursive", Boolean.class, false);`（左边是原生 `boolean`，自动拆箱仍然工作，因为 `defaultValue` 保证非 null）。

- [ ] **Step 3: 编译**

Run: `mvn -pl flink-etl-connector/connector-localfile -am compile`
Expected: 编译通过。

---

## Task 8: 替换 connector-kafka 调用点

**Files:**
- Modify: `flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/source/config/KafkaSourceConfig.java`
- Modify: `flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/sink/config/KafkaSinkConfig.java`

- [ ] **Step 1: 查看调用点**

Run: `grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/source/config/KafkaSourceConfig.java flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/sink/config/KafkaSinkConfig.java`
Expected: 共 11 行（source 7 + sink 4）。

- [ ] **Step 2: 按映射表替换**

`List<String> topics = config.getList("topics");` → `List<String> topics = config.get("topics", List.class);`。

- [ ] **Step 3: 编译**

Run: `mvn -pl flink-etl-connector/connector-kafka -am compile`
Expected: 编译通过。

---

## Task 9: 替换 connector-modbus 调用点

**Files:**
- Modify: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java`

- [ ] **Step 1: 查看调用点**

Run: `grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java`
Expected: 6 行。

- [ ] **Step 2: 按映射表替换**

注意 `int deviceId = config.getInteger("deviceId", 1);` → `int deviceId = config.get("deviceId", Integer.class, 1);`，原生 `int` 自动拆箱。

- [ ] **Step 3: 编译**

Run: `mvn -pl flink-etl-connector/connector-modbus -am compile`
Expected: 编译通过。

---

## Task 10: 替换 connector-mqtt 调用点

**Files:**
- Modify: `flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/config/MqttSourceConfig.java`

- [ ] **Step 1: 查看调用点**

Run: `grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/config/MqttSourceConfig.java`
Expected: 5 行。

- [ ] **Step 2: 按映射表替换**

- [ ] **Step 3: 编译**

Run: `mvn -pl flink-etl-connector/connector-mqtt -am compile`
Expected: 编译通过。

---

## Task 11: 替换 connector-mock 调用点

**Files:**
- Modify: `flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/config/MockSourceConfig.java`

- [ ] **Step 1: 查看调用点**

Run: `grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/config/MockSourceConfig.java`
Expected: 3 行，含 `getInteger` × 1、`getLong` × 1、`getObject` × 1。

- [ ] **Step 2: 按映射表替换**

`Object dataObj = config.getObject("data");` → `Object dataObj = config.get("data");`。

- [ ] **Step 3: 编译**

Run: `mvn -pl flink-etl-connector/connector-mock -am compile`
Expected: 编译通过。

---

## Task 12: 替换 flink-etl-transform 调用点

**Files:**
- Modify: `flink-etl-transform/src/main/java/com/etl/transform/SqlTransformPlugin.java`

- [ ] **Step 1: 查看调用点**

Run: `grep -nE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" flink-etl-transform/src/main/java/com/etl/transform/SqlTransformPlugin.java`
Expected: 1 行 `config.getString("sql")`。

- [ ] **Step 2: 替换**

`String sql = config.getString("sql");` → `String sql = config.get("sql", String.class);`。

- [ ] **Step 3: 编译**

Run: `mvn -pl flink-etl-transform -am compile`
Expected: 编译通过。

---

## Task 13: 全量编译与测试

到这一步，所有 main 源代码与测试代码应该都已迁移完毕。验证整体闭环。

- [ ] **Step 1: 全量编译**

Run: `mvn clean compile`
Expected: BUILD SUCCESS，无任何 `cannot find symbol method getString/getInteger/...` 错误。

- [ ] **Step 2: 全量测试编译**

Run: `mvn test-compile`
Expected: BUILD SUCCESS。

- [ ] **Step 3: 跑 BaseConfigTest**

Run: `mvn -pl flink-etl-core test -Dtest=BaseConfigTest`
Expected: Tests run > 25，全部通过。

- [ ] **Step 4: 跑全量测试**

Run: `mvn test`
Expected: BUILD SUCCESS，无 failures 无 errors。

- [ ] **Step 5: 如果有失败，定位并修复**

如果出现编译失败，运行：

```
grep -rnE "config\.(getString|getInteger|getBoolean|getLong|getList|getMap|getObject)\(" \
  --include="*.java" . | grep -v "/docs/" | grep -v "rs\." | grep -v "currentResultSet\."
```

这是漏改的列表。挨个补齐再回到 Step 1。

---

## Task 14: 统一提交

所有改动一次性提交（含 BaseConfig 重写 + 全部调用点迁移 + 单元测试）。

- [ ] **Step 1: 查看 staging 状态**

Run: `git status --short`
Expected: 看到 `BaseConfig.java`、`BaseConfigTest.java` 及 15 个调用点文件的修改。

- [ ] **Step 2: 全部 add 并提交**

```
git add flink-etl-core/src/main/java/com/etl/core/config/BaseConfig.java \
        flink-etl-core/src/test/java/com/etl/core/config/BaseConfigTest.java \
        flink-etl-core/src/main/java/com/etl/core/utils/SqlUtils.java \
        flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/sink/config/JdbcSinkConfig.java \
        flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/config/JdbcSourceConfig.java \
        flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/splitter/NumericSplitter.java \
        flink-etl-connector/connector-jdbc/src/test/java/com/etl/connector/jdbc/sink/executor/BufferReducedExecutorTest.java \
        flink-etl-connector/connector-http/src/main/java/com/etl/connector/http/source/config/HttpSourceConfig.java \
        flink-etl-connector/connector-cdc/src/main/java/com/etl/connector/cdc/mysql/config/MySqlCdcConfig.java \
        flink-etl-connector/connector-localfile/src/main/java/com/etl/connector/localfile/source/config/LocalFileSourceConfig.java \
        flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/source/config/KafkaSourceConfig.java \
        flink-etl-connector/connector-kafka/src/main/java/com/etl/connector/kafka/sink/config/KafkaSinkConfig.java \
        flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java \
        flink-etl-connector/connector-mqtt/src/main/java/com/etl/connector/mqtt/source/config/MqttSourceConfig.java \
        flink-etl-connector/connector-mock/src/main/java/com/etl/connector/mock/source/config/MockSourceConfig.java \
        flink-etl-transform/src/main/java/com/etl/transform/SqlTransformPlugin.java

git commit -m "refactor: BaseConfig 统一为 get(key, Class) 泛型接口

删除按类型命名的 getString/getInteger/getLong/getBoolean/getList/getMap/getObject
等 11 个 getter，统一为：

  - <T> T get(String key, Class<T> clazz)
  - <T> T get(String key, Class<T> clazz, T defaultValue)
  - Object get(String key)
  - boolean contains(String key)

迁移全部 70+ 处调用点；新增 BaseConfigTest 覆盖所有类型分支与异常路径。"
```

- [ ] **Step 3: 验证 git log**

Run: `git log -1 --stat`
Expected: 单次提交，~17 个文件改动。

---

## 完工标准

- `mvn clean test` 全绿
- `BaseConfig.java` 仅保留 `get(key, Class)` / `get(key, Class, default)` / `get(key)` / `contains` 四个公共方法
- `BaseConfigTest` 覆盖每个支持类型的命中/缺失/转换异常/不支持类型路径
- `git status` 无未提交修改
