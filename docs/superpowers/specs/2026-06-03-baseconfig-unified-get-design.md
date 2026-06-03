# BaseConfig 统一 `get(key, Class<T>)` 接口

- 日期：2026-06-03
- 范围：`flink-etl-core` 的 `com.etl.core.config.BaseConfig` 及其全部调用点

## 1. 背景与目标

`BaseConfig` 当前对外暴露 7 类共 11 个 getter（`getString` / `getInteger` / `getLong` / `getBoolean` / `getList` / `getMap` / `getObject`，其中多数还带 `defaultValue` 重载），项目内共有 76 处调用，分布在 15 个文件中（主要是各连接器的 Config 类）。

目标：将所有按类型命名的 getter 收敛成一个统一的泛型入口，由调用方通过 `Class<T>` 显式声明期望类型；同时迁移项目内全部调用点。

## 2. 范围

**包含**：

- 重写 `BaseConfig` 公共 API。
- 迁移 `flink-etl-core` / `flink-etl-connector/*` / `flink-etl-transform` 下全部 76 处调用点。
- 为 `BaseConfig` 新增单元测试。

**不包含**：

- 配置文件 JSON schema 变更。
- 其它 Config 子类（`SourceConfig` / `SinkConfig` / `TransformConfig` 等）的语义改动。
- 引入新的依赖或类型系统。

## 3. 新 API 表面

`BaseConfig` 对外仅保留以下四个公共方法：

```java
// 主接口：根据 clazz 决定返回类型与转换语义
public <T> T get(String key, Class<T> clazz);

// 默认值重载
public <T> T get(String key, Class<T> clazz, T defaultValue);

// 取代旧 getObject(key)：未指定 clazz 时直接返回原始 Object
public Object get(String key);

// 保留不变
public boolean contains(String key);
```

旧的 `getString` / `getInteger` / `getLong` / `getBoolean` / `getList` / `getMap` / `getObject` 及其各自的 default 重载**全部删除**。

## 4. 行为规范

### 4.1 `get(String key, Class<T> clazz)`

`config == null` 或 `key` 不存在或对应值为 `null` 时一律返回 `null`。否则按下表分派：

| `clazz` 入参 | 返回类型 | 转换规则 |
|---|---|---|
| `String.class` | `String` | `String.valueOf(value)` |
| `Integer.class` | `Integer` | 已是 `Integer` 直接返回；否则 `Integer.parseInt(String.valueOf(value))`，失败抛 `IllegalArgumentException` |
| `Long.class` | `Long` | 已是 `Long` 直接返回；`Integer` 提升为 `Long`；其它走 `Long.parseLong(String.valueOf(value))`，失败抛 `IllegalArgumentException` |
| `Boolean.class` | `Boolean` | 已是 `Boolean` 直接返回；否则 `Boolean.parseBoolean(String.valueOf(value))` |
| `List.class` | `List<String>` | 值必须是 `List<?>`，否则抛 `IllegalArgumentException`；元素逐个 `String.valueOf` |
| `Map.class` | `Map<String, Object>` | 值必须是 `Map<?,?>`，否则抛 `IllegalArgumentException`；key 转 `String`，value 保持原始引用 |
| 其它任何 `Class` | — | 抛 `IllegalArgumentException("不支持的类型: " + clazz.getName())` |

异常信息要求：转换失败时携带 `key` 与原始 `value`，便于排查。

### 4.2 `get(String key, Class<T> clazz, T defaultValue)`

等价于：

```java
T value = get(key, clazz);
return value != null ? value : defaultValue;
```

注意：仅在结果为 `null` 时回退到默认值。**类型转换异常仍然抛出**，不会被默认值掩盖。

### 4.3 `get(String key)`

等价于旧的 `getObject(key)`：`config != null ? config.get(key) : null`，不做任何转换。

## 5. 内部实现（方案 A：if-else 链分派）

`get(key, clazz)` 的方法主体结构：

1. `if (config == null) return null;`
2. `Object value = config.get(key); if (value == null) return null;`
3. 7 个 `if (clazz == X.class) { ... }` 分支（恒等比较），命中即按 §4.1 的规则转换并返回。
4. 末尾 `throw new IllegalArgumentException("不支持的类型: " + clazz.getName());`

在 `List` / `Map` 分支使用局部 `@SuppressWarnings("unchecked")` 抑制泛型告警。所有转换逻辑直接内联在分支里，不再抽取私有方法（每个分支独立且短）。

## 6. 调用点迁移

机械替换规则：

| 旧调用 | 新调用 |
|---|---|
| `config.getString("k")` | `config.get("k", String.class)` |
| `config.getString("k", "x")` | `config.get("k", String.class, "x")` |
| `config.getInteger("k")` | `config.get("k", Integer.class)` |
| `config.getInteger("k", 5)` | `config.get("k", Integer.class, 5)` |
| `config.getLong("k")` | `config.get("k", Long.class)` |
| `config.getLong("k", 5L)` | `config.get("k", Long.class, 5L)` |
| `config.getBoolean("k", false)` | `config.get("k", Boolean.class, false)` |
| `config.getList("k")` | `config.get("k", List.class)` |
| `config.getMap("k")` | `config.get("k", Map.class)` |
| `config.getObject("k")` | `config.get("k")` |

涉及文件（按调用密度排序）：

| 调用数 | 文件 |
|---:|---|
| 10 | `flink-etl-connector/connector-jdbc/.../sink/config/JdbcSinkConfig.java` |
| 9 | `flink-etl-connector/connector-jdbc/.../source/config/JdbcSourceConfig.java` |
| 8 | `flink-etl-connector/connector-http/.../source/config/HttpSourceConfig.java` |
| 8 | `flink-etl-connector/connector-cdc/.../mysql/config/MySqlCdcConfig.java` |
| 7 | `flink-etl-connector/connector-localfile/.../source/config/LocalFileSourceConfig.java` |
| 7 | `flink-etl-connector/connector-kafka/.../source/config/KafkaSourceConfig.java` |
| 6 | `flink-etl-connector/connector-modbus/.../source/config/ModbusSourceConfig.java` |
| 5 | `flink-etl-connector/connector-mqtt/.../source/config/MqttSourceConfig.java` |
| 4 | `flink-etl-connector/connector-kafka/.../sink/config/KafkaSinkConfig.java` |
| 3 | `flink-etl-connector/connector-mock/.../source/config/MockSourceConfig.java` |
| 3 | `flink-etl-connector/connector-jdbc/.../source/splitter/NumericSplitter.java` |
| 3 | `flink-etl-connector/connector-jdbc/src/test/.../BufferReducedExecutorTest.java` |
| 1 | `flink-etl-transform/.../SqlTransformPlugin.java` |
| 1 | `flink-etl-core/.../utils/SqlUtils.java` |
| 1 | `flink-etl-connector/connector-jdbc/.../source/JdbcSplitReader.java` |

迁移与 `BaseConfig` 重写在同一变更内完成，编译期不留中间态。

## 7. 测试

在 `flink-etl-core/src/test/java/com/etl/core/config/` 新增 `BaseConfigTest`，覆盖：

- 每个支持的 `clazz` 分支的命中返回；
- 值为 `null` 或 key 不存在时返回 `null`；
- 带 `defaultValue` 重载在 `null` 时返回默认值；
- `Integer` / `Long` 转换失败抛 `IllegalArgumentException`；
- `List` / `Map` 类型不匹配抛 `IllegalArgumentException`；
- 不支持的 `clazz`（如 `Date.class`）抛 `IllegalArgumentException`；
- `get(String key)` 无转换地返回原始 `Object`。

`mvn test` 全绿作为完工标准；现有各连接器单元测试不应需要逻辑改动，仅做调用形式替换。

## 8. 兼容性与风险

- **编译期破坏**：旧方法全删，未迁移的调用点会编译失败。这是有意为之——比运行时静默错误更安全；且迁移与重写一次完成。
- **没有外部使用者**：所有调用点都在项目内（见 §6 文件清单）。
- **`getList` 严格语义保留**：值不是 `List<?>` 时仍抛 `IllegalArgumentException`，与旧版一致，不会因为容忍度变化引入运行时差异。
- **`getBoolean` 语义微变**：旧版 `getBoolean(key, defaultValue)` 返回原生 `boolean`；新版 `get(key, Boolean.class)` 在未设置时返回 `null`，带默认值重载与旧行为等价。所有现存调用点都使用带默认值的形式，迁移后行为一致。

## 9. 完工标准

- `BaseConfig` 仅保留 §3 列出的四个公共方法；
- 全部 76 处调用点迁移完成；
- `BaseConfigTest` 覆盖 §7 列出的全部场景；
- `mvn clean test` 全绿。
