# Mock Source 配置简化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 简化 Mock Source 配置，将 `rows` 配置改为 `data`，删除 `kind` 字段（batch 模式只支持 INSERT）。

**架构说明:** 配置格式从 `rows: [{kind, data}]` 简化为 `data: [...]`（直接 JSON 数组），`MockSourceConfig` 从 `List<RowData>` 改为 `JsonNode data`。

**技术栈:** Java, Jackson JsonNode, Flink Row

---

## 变更文件清单

| 文件 | 操作 |
|------|------|
| `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/config/MockSourceConfig.java` | 修改：删除 `RowData` 内部类，`rows` → `data` |
| `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSource.java` | 修改：`parseRowsConfig` → `parseDataConfig` |
| `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/generator/DataRowGenerator.java` | 修改：直接接收 `JsonNode` 数组 |
| `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplitReader.java` | 修改：`getRows()` 调用适配 |
| `docs/examples/mock-batch-fixed.json` | 修改：配置示例 |
| `docs/examples/mock-cdc-test.json` | 修改：配置示例 |
| `docs/examples/mock-streaming.json` | 检查并修改 |
| `PLUGINS.md` | 检查并更新文档 |

---

## 任务一：修改 MockSourceConfig

**文件:**
- 修改: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/config/MockSourceConfig.java`

### 任务一：简化 MockSourceConfig

**文件:**
- 修改: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/config/MockSourceConfig.java`

- [ ] **Step 1: 备份并修改 MockSourceConfig.java**

将配置类简化为只有一个 `JsonNode data` 属性，删除 `RowData` 内部类：

```java
package com.etl.source.mock.config;

import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

/**
 * Mock Source 配置封装类
 */
@Data
@Builder
public class MockSourceConfig implements Serializable {

    /** 是否有界 */
    private boolean bounded;

    /** Schema 定义 */
    private EtlSchema schema;

    /**
     * 固定数据配置（JSON 数组）
     * 配置后数据读取完毕程序自然停止
     */
    private JsonNode data;

    /**
     * 随机生成的行数
     * 配置后数据读取完毕程序自然停止；未配置时按 intervalMs 持续生成
     */
    private Integer numRows;

    /**
     * 数据生成间隔（毫秒）
     * 仅在未配置 data 和 numRows 时生效
     */
    private Long intervalMs;
}
```

---

## 任务二：修改 DataRowGenerator

**文件:**
- 修改: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/generator/DataRowGenerator.java`

- [ ] **Step 1: 修改 DataRowGenerator，接收 JsonNode 数组**

```java
package com.etl.source.mock.generator;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * 从配置生成固定数据的 Row 列表
 */
public class DataRowGenerator {

    /**
     * 从 JSON 数组节点生成 Row 列表
     *
     * @param data JsonNode 数组节点
     * @param schema Schema 定义
     * @return 生成的 Row 列表
     */
    public static List<Row> generateRows(JsonNode data, EtlSchema schema) {
        if (data == null || !data.isArray() || data.isEmpty()) {
            return new ArrayList<>();
        }

        List<Row> rows = new ArrayList<>();
        for (JsonNode item : data) {
            Row row = JsonToRowConverter.convertJsonToRow(item, schema);
            rows.add(row);
        }
        return rows;
    }
}
```

---

## 任务三：修改 MockSource

**文件:**
- 修改: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSource.java`

- [ ] **Step 1: 修改 parseRowsConfig → parseDataConfig**

将配置键名从 `rows` 改为 `data`，直接存储为 `JsonNode`：

```java
// 第 74-101 行，替换 parseRowsConfig 方法
@SuppressWarnings("unchecked")
private JsonNode parseDataConfig(SourceConfig config) {
    if (!config.contains("data")) {
        return null;
    }

    Object dataObj = config.get("data");
    return JsonUtils.valueToTree(dataObj);
}
```

- [ ] **Step 2: 修改构造函数中的配置解析**

第 57-68 行的构造函数中：

```java
// 修改前
List<MockSourceConfig.RowData> rows = parseRowsConfig(config);
Integer numRows = config.getInteger("numRows", 10);
Long intervalMs = config.getLong("intervalMs", 1000L);

// 封装配置对象
this.mockConfig = MockSourceConfig.builder()
        .bounded(bounded)
        .schema(schema)
        .rows(rows)
        .numRows(numRows)
        .intervalMs(intervalMs)
        .build();

// 修改后
JsonNode data = parseDataConfig(config);
Integer numRows = config.getInteger("numRows", 10);
Long intervalMs = config.getLong("intervalMs", 1000L);

// 封装配置对象
this.mockConfig = MockSourceConfig.builder()
        .bounded(bounded)
        .schema(schema)
        .data(data)
        .numRows(numRows)
        .intervalMs(intervalMs)
        .build();
```

- [ ] **Step 3: 修改日志输出（第 70-71 行）**

```java
// 修改前
log.info("创建 MockSource: bounded={}, rows={}, numRows={}, intervalMs={}",
    bounded, rows != null ? rows.size() : null, mockConfig.getNumRows(), intervalMs);

// 修改后
log.info("创建 MockSource: bounded={}, data={}, numRows={}, intervalMs={}",
    bounded, data != null ? data.size() : null, mockConfig.getNumRows(), intervalMs);
```

---

## 任务四：修改 MockSplitReader

**文件:**
- 修改: `flink-etl-source/flink-etl-source-mock/src/main/java/com/etl/source/mock/MockSplitReader.java`

- [ ] **Step 1: 修改 fetchBoundedData 方法中的 rows 调用（第 87-90 行）**

```java
// 修改前（第 87-90 行）
if (mockConfig.getRows() != null) {
    List<Row> rows = DataRowGenerator.generateRows(mockConfig.getRows(), schema);

// 修改后
if (mockConfig.getData() != null) {
    List<Row> rows = DataRowGenerator.generateRows(mockConfig.getData(), schema);
```

---

## 任务五：更新配置示例

**文件:**
- 修改: `docs/examples/mock-batch-fixed.json`
- 修改: `docs/examples/mock-cdc-test.json`
- 检查并修改: `docs/examples/mock-streaming.json`

- [ ] **Step 1: 更新 mock-batch-fixed.json**

配置从：
```json
"rows": [
  {
    "kind": "INSERT",
    "data": {
      "id": 1,
      "name": "Alice",
      "age": 25,
      "active": true
    }
  }
]
```

改为：
```json
"data": [
  {
    "id": 1,
    "name": "Alice",
    "age": 25,
    "active": true
  },
  {
    "id": 2,
    "name": "Bob",
    "age": 30,
    "active": false
  },
  {
    "id": 1,
    "name": "Alice Updated",
    "age": 26,
    "active": true
  }
]
```

- [ ] **Step 2: 更新 mock-cdc-test.json**

同样将 `rows: [{kind, data}]` 改为 `data: [...]` 格式（移除 kind）。

- [ ] **Step 3: 检查 mock-streaming.json 并同步更新**

---

## 任务六：编译验证

**文件:**
- 修改: `flink-etl-source/flink-etl-source-mock/pom.xml`（如有需要）

- [ ] **Step 1: 编译项目**

```bash
cd d:/work/idea/flink-etl-tool
mvn clean compile -pl flink-etl-source/flink-etl-source-mock -am
```

- [ ] **Step 2: 运行测试（如有）**

```bash
mvn test -pl flink-etl-source/flink-etl-source-mock
```

- [ ] **Step 3: 全量编译验证**

```bash
mvn clean compile
```

---

## 任务七：检查并更新 PLUGINS.md

- [ ] **Step 1: 检查 PLUGINS.md 中 Mock Source 文档**

确认文档中 `rows` 配置说明是否需要更新为 `data` 格式。

---

## 依赖关系

```
任务一（MockSourceConfig）
    ↓
任务二（DataRowGenerator） ← 并行
任务三（MockSource）
    ↓
任务四（MockSplitReader） ← 依赖任务一、二、三
    ↓
任务五（配置示例）← 依赖任务一
    ↓
任务六（编译验证）
    ↓
任务七（文档更新）
```
