# HTTP Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 HTTP Source 插件，支持通过 HTTP 请求获取 JSON 数据并转换为 Flink Row 输出。

**Architecture:** 继承 `AbstractSplitSource`，实现单分片模式。HTTP 请求在 `HttpSplitReader` 中执行，响应体通过 JSONPath 提取后，根据 Schema 转换为 Row 类型。

**Tech Stack:** Flink 1.19.0, Jackson (JSON), JsonPath (com.jayway.jsonpath)

---

## File Structure

```
flink-etl-source-http/
├── pom.xml
└── src/main/java/com/etl/source/http/
    ├── HttpSourcePlugin.java          # SPI 入口
    ├── HttpSource.java                # Source 主类
    ├── HttpSourceConfig.java          # 配置封装
    ├── HttpSplit.java                 # 分片定义
    ├── HttpSplitEnumerator.java       # 分片枚举器
    ├── HttpSourceReader.java          # 源阅读器
    ├── HttpSplitReader.java           # 分片读取器（核心逻辑）
    ├── HttpSplitState.java            # 分片状态
    ├── HttpEnumCheckpoint.java        # 枚举器检查点
    └── HttpRecordEmitter.java         # 记录发射器
```

**修改文件：**
- `pom.xml` - 添加 json-path 依赖版本
- `flink-etl-source/pom.xml` - 添加 http 模块
- `flink-etl-client/pom.xml` - 添加 http 依赖
- `PLUGINS.md` - 添加 HTTP Source 文档

---

### Task 1: 添加 JSONPath 依赖

**Files:**
- Modify: `pom.xml:27-28`

- [ ] **Step 1: 在父 pom.xml 中添加 json-path 依赖版本**

```xml
<!-- 在 <properties> 中添加 -->
<json-path.version>2.9.0</json-path.version>
```

- [ ] **Step 2: 在 dependencyManagement 中添加依赖声明**

```xml
<!-- 在 <dependencyManagement><dependencies> 中添加 -->
<dependency>
    <groupId>com.jayway.jsonpath</groupId>
    <artifactId>json-path</artifactId>
    <version>${json-path.version}</version>
</dependency>
```

- [ ] **Step 3: 验证 pom.xml 修改**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn help:effective-pom | grep -A 3 "json-path"`
Expected: 显示 json-path 依赖信息

- [ ] **Step 4: Commit**

```bash
git add pom.xml
git commit -m "feat: 添加 json-path 依赖版本配置"
```

---

### Task 2: 创建 HTTP Source 模块结构

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/pom.xml`
- Modify: `flink-etl-source/pom.xml`

- [ ] **Step 1: 创建模块 pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-source</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>flink-etl-source-http</artifactId>
    <name>Flink ETL Source - HTTP</name>
    <description>HTTP Source 插件，支持从 REST API 获取 JSON 数据</description>

    <dependencies>
        <!-- JSONPath 解析 -->
        <dependency>
            <groupId>com.jayway.jsonpath</groupId>
            <artifactId>json-path</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 修改父模块添加 http 子模块**

在 `flink-etl-source/pom.xml` 的 `<modules>` 中添加：

```xml
<module>flink-etl-source-http</module>
```

- [ ] **Step 3: 验证模块创建**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn -pl flink-etl-source/flink-etl-source-http validate`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/pom.xml flink-etl-source/pom.xml
git commit -m "feat: 创建 flink-etl-source-http 模块"
```

---

### Task 3: 创建 HttpSourceConfig

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSourceConfig.java`

- [ ] **Step 1: 创建配置封装类**

```java
package com.etl.source.http;

import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

/**
 * HTTP Source 配置
 * 用于传递所有参数到 Enumerator 和 SplitReader
 */
@Getter
@Builder
public class HttpSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 请求 URL */
    private final String url;
    /** HTTP 方法，GET 或 POST */
    private final String method;
    /** 请求头 */
    private final Map<String, String> headers;
    /** 查询参数 */
    private final Map<String, String> params;
    /** 请求体（JSON 对象序列化后的字符串） */
    private final String body;
    /** JSONPath 表达式，提取数据 */
    private final String dataPath;
    /** Schema 定义 */
    private final EtlSchema schema;
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSourceConfig.java
git commit -m "feat: 添加 HttpSourceConfig 配置类"
```

---

### Task 4: 创建 HttpSplit

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSplit.java`

- [ ] **Step 1: 创建分片定义类**

```java
package com.etl.source.http;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.Getter;

/**
 * HTTP 分片
 * 单分片模式，包含完整的请求配置
 */
@Getter
public class HttpSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    /** 分片 ID */
    private final String splitId;

    /** HTTP 配置 */
    private final HttpSourceConfig config;

    /**
     * 构造函数
     *
     * @param splitId 分片 ID
     * @param config  HTTP 配置
     */
    public HttpSplit(String splitId, HttpSourceConfig config) {
        this.splitId = splitId;
        this.config = config;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "HttpSplit{" +
                "splitId='" + splitId + '\'' +
                ", url='" + config.getUrl() + '\'' +
                '}';
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSplit.java
git commit -m "feat: 添加 HttpSplit 分片定义"
```

---

### Task 5: 创建 HttpEnumCheckpoint

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpEnumCheckpoint.java`

- [ ] **Step 1: 创建枚举器检查点类**

```java
package com.etl.source.http;

import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;

import java.util.Collection;

/**
 * HTTP 分片枚举器检查点
 */
public class HttpEnumCheckpoint extends BaseEnumCheckpoint<HttpSplit> {

    private static final long serialVersionUID = DefaultCheckpointSerializer.VERSION;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public HttpEnumCheckpoint(Collection<HttpSplit> pendingSplits) {
        super(pendingSplits);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpEnumCheckpoint.java
git commit -m "feat: 添加 HttpEnumCheckpoint 检查点类"
```

---

### Task 6: 创建 HttpSplitState

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSplitState.java`

- [ ] **Step 1: 创建分片状态类**

```java
package com.etl.source.http;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;
import lombok.Setter;

/**
 * HTTP 分片状态
 */
@Getter
@Setter
public class HttpSplitState extends BaseSplitState<HttpSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param split HTTP 分片
     */
    public HttpSplitState(HttpSplit split) {
        super(split);
    }

    @Override
    public String toString() {
        return "HttpSplitState{" +
                "split=" + getSplit() +
                ", recordsRead=" + getRecordsRead() +
                '}';
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSplitState.java
git commit -m "feat: 添加 HttpSplitState 分片状态类"
```

---

### Task 7: 创建 HttpRecordEmitter

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpRecordEmitter.java`

- [ ] **Step 1: 创建记录发射器类**

```java
package com.etl.source.http;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

/**
 * HTTP 记录发射器
 * 将 Row 直接发射到下游
 */
@Slf4j
public class HttpRecordEmitter implements RecordEmitter<Row, Row, HttpSplitState> {

    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, HttpSplitState splitState) throws Exception {
        // 发射记录到下游
        output.collect(record);

        // 更新状态
        splitState.addRecordsRead(1);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpRecordEmitter.java
git commit -m "feat: 添加 HttpRecordEmitter 记录发射器"
```

---

### Task 8: 创建 JSON 转 Row 工具类

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/JsonToRowConverter.java`

- [ ] **Step 1: 创建 JSON 转 Row 转换器**

```java
package com.etl.source.http;

import com.etl.core.schema.EtlSchema;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON 转 Row 转换器
 * 根据 Schema 定义将 JSON 数据转换为 Flink Row
 */
@Slf4j
public class JsonToRowConverter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * 从 JSON 字符串提取数据并转换为 Row 列表
     *
     * @param jsonResponse JSON 响应字符串
     * @param dataPath     JSONPath 表达式，可为 null
     * @param schema       Schema 定义
     * @return Row 列表
     */
    public static List<Row> convert(String jsonResponse, String dataPath, EtlSchema schema) {
        List<Row> rows = new ArrayList<>();

        try {
            // 解析 JSON
            DocumentContext document = JsonPath.parse(jsonResponse);

            // 提取 root
            Object root;
            if (dataPath != null && !dataPath.isEmpty()) {
                try {
                    root = document.read(dataPath);
                } catch (PathNotFoundException e) {
                    throw new IllegalArgumentException("JSONPath 提取失败: " + dataPath, e);
                }
            } else {
                root = document.json();
            }

            if (root == null) {
                throw new IllegalArgumentException("提取的数据为空");
            }

            // 转换为 JsonNode 便于处理
            JsonNode rootNode = OBJECT_MAPPER.valueToTree(root);

            // 根据 root 类型处理
            if (rootNode.isArray()) {
                // JSONArray: 遍历数组
                ArrayNode arrayNode = (ArrayNode) rootNode;
                for (JsonNode element : arrayNode) {
                    Row row = convertToRow(element, schema);
                    rows.add(row);
                }
            } else if (rootNode.isObject()) {
                // JSONObject: 单条记录
                Row row = convertToRow(rootNode, schema);
                rows.add(row);
            } else {
                throw new IllegalArgumentException("提取的数据既不是 JSONObject 也不是 JSONArray: " + rootNode.getNodeType());
            }

        } catch (Exception e) {
            log.error("JSON 转换失败: {}", e.getMessage(), e);
            throw new RuntimeException("JSON 转换失败: " + e.getMessage(), e);
        }

        return rows;
    }

    /**
     * 将单个 JsonNode 转换为 Row
     */
    private static Row convertToRow(JsonNode node, EtlSchema schema) {
        int fieldCount = schema.getFieldCount();
        Row row = Row.withPositions(fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = schema.getFieldName(i);
            JsonNode fieldNode = node.get(fieldName);
            Object value = convertValue(fieldNode, schema.getFieldType(i));
            row.setField(i, value);
        }

        return row;
    }

    /**
     * 根据 TypeInformation 转换值
     */
    private static Object convertValue(JsonNode node, org.apache.flink.api.common.typeinfo.TypeInformation<?> type) {
        if (node == null || node.isNull()) {
            return null;
        }

        String typeName = type.getTypeClass().getSimpleName();

        switch (typeName) {
            case "String":
                return node.asText();
            case "Integer":
                return node.asInt();
            case "Long":
                return node.asLong();
            case "Double":
                return node.asDouble();
            case "Boolean":
                return node.asBoolean();
            case "BigDecimal":
                return new BigDecimal(node.asText());
            case "LocalDateTime":
                return LocalDateTime.parse(node.asText(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case "Object[]":
                // 数组类型
                return convertArray(node, type);
            case "Row":
                // 嵌套对象类型
                return convertRow(node, type);
            default:
                // 尝试作为嵌套 Row 处理
                if (type instanceof org.apache.flink.api.java.typeutils.RowTypeInfo) {
                    return convertRow(node, type);
                }
                return node.asText();
        }
    }

    /**
     * 转换数组类型
     */
    private static Object[] convertArray(JsonNode node, org.apache.flink.api.common.typeinfo.TypeInformation<?> type) {
        if (!node.isArray()) {
            throw new IllegalArgumentException("期望数组类型，但得到: " + node.getNodeType());
        }

        List<Object> list = new ArrayList<>();
        for (JsonNode element : node) {
            // 简单类型数组的元素类型
            if (type instanceof org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo) {
                org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo<?, ?> arrayTypeInfo =
                        (org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo<?, ?>) type;
                org.apache.flink.api.common.typeinfo.TypeInformation<?> componentType = arrayTypeInfo.getComponentInfo();
                list.add(convertValue(element, componentType));
            } else if (type instanceof org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo) {
                org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo<?, ?> arrayTypeInfo =
                        (org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo<?, ?>) type;
                org.apache.flink.api.common.typeinfo.TypeInformation<?> componentType = arrayTypeInfo.getComponentInfo();
                list.add(convertValue(element, componentType));
            } else {
                list.add(element.asText());
            }
        }

        return list.toArray();
    }

    /**
     * 转换嵌套 Row 类型
     */
    private static Row convertRow(JsonNode node, org.apache.flink.api.common.typeinfo.TypeInformation<?> type) {
        if (!node.isObject()) {
            throw new IllegalArgumentException("期望对象类型，但得到: " + node.getNodeType());
        }

        org.apache.flink.api.java.typeutils.RowTypeInfo rowTypeInfo = (org.apache.flink.api.java.typeutils.RowTypeInfo) type;
        String[] fieldNames = rowTypeInfo.getFieldNames();
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();

        Row row = Row.withPositions(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            JsonNode fieldNode = node.get(fieldNames[i]);
            Object value = convertValue(fieldNode, fieldTypes[i]);
            row.setField(i, value);
        }

        return row;
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/JsonToRowConverter.java
git commit -m "feat: 添加 JsonToRowConverter 转换器"
```

---

### Task 9: 创建 HttpSplitReader（核心逻辑）

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSplitReader.java`

- [ ] **Step 1: 创建分片读取器**

```java
package com.etl.source.http;

import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * HTTP 分片读取器
 * 执行 HTTP 请求并将响应转换为 Row
 */
@Slf4j
public class HttpSplitReader implements BaseSplitReader<Row, HttpSplit> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int CONNECT_TIMEOUT = 30000;
    private static final int READ_TIMEOUT = 60000;

    private final Queue<HttpSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    @Override
    public RecordsWithSplitIds<Row> fetch() throws Exception {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        HttpSplit split = pendingSplits.poll();
        if (split == null) {
            // 没有待处理的分片，返回空结果
            builder.addFinishedSplits(finishedSplits);
            return builder.build();
        }

        try {
            // 执行 HTTP 请求
            String jsonResponse = executeRequest(split.getConfig());

            // 转换为 Row
            List<Row> rows = JsonToRowConverter.convert(
                    jsonResponse,
                    split.getConfig().getDataPath(),
                    split.getConfig().getSchema()
            );

            log.info("HTTP 请求完成，获取 {} 条记录", rows.size());

            // 添加记录
            for (Row row : rows) {
                builder.add(split.splitId(), row);
            }

            // 标记分片完成
            finishedSplits.add(split.splitId());

        } catch (Exception e) {
            log.error("HTTP 请求失败: {}", e.getMessage(), e);
            throw new RuntimeException("HTTP 请求失败: " + e.getMessage(), e);
        }

        builder.addFinishedSplits(finishedSplits);
        return builder.build();
    }

    /**
     * 执行 HTTP 请求
     */
    private String executeRequest(HttpSourceConfig config) throws Exception {
        String urlString = config.getUrl();

        // 添加查询参数
        if (config.getParams() != null && !config.getParams().isEmpty()) {
            StringBuilder urlBuilder = new StringBuilder(urlString);
            urlBuilder.append("?");
            for (Map.Entry<String, String> entry : config.getParams().entrySet()) {
                urlBuilder.append(entry.getKey())
                        .append("=")
                        .append(java.net.URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                        .append("&");
            }
            urlString = urlBuilder.substring(0, urlBuilder.length() - 1);
        }

        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        try {
            connection.setRequestMethod(config.getMethod());
            connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);

            // 设置请求头
            connection.setRequestProperty("Accept", "application/json");
            if (config.getHeaders() != null) {
                for (Map.Entry<String, String> entry : config.getHeaders().entrySet()) {
                    connection.setRequestProperty(entry.getKey(), entry.getValue());
                }
            }

            // POST 请求体
            if ("POST".equalsIgnoreCase(config.getMethod()) && config.getBody() != null) {
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                try (OutputStream os = connection.getOutputStream()) {
                    byte[] input = config.getBody().getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }
            }

            // 检查响应码
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("HTTP 请求失败，响应码: " + responseCode);
            }

            // 读取响应
            StringBuilder response = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
            }

            return response.toString();

        } finally {
            connection.disconnect();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<HttpSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个 HTTP 分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        log.info("HttpSplitReader 关闭");
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSplitReader.java
git commit -m "feat: 添加 HttpSplitReader 分片读取器"
```

---

### Task 10: 创建 HttpSourceReader

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSourceReader.java`

- [ ] **Step 1: 创建源阅读器**

```java
package com.etl.source.http;

import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.BaseSourceReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * HTTP Source Reader
 * 继承 BaseSourceReader，自动处理线程模型和状态管理
 */
@Slf4j
public class HttpSourceReader extends BaseSourceReader<Row, Row, HttpSplit, HttpSplitState> {

    public HttpSourceReader(
            Supplier<BaseSplitReader<Row, HttpSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new HttpRecordEmitter(), new Configuration(), context);
    }

    @Override
    public HttpSplitState initializedState(HttpSplit split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new HttpSplitState(split);
    }

    @Override
    protected HttpSplit toSplitType(String splitId, HttpSplitState splitState) {
        return splitState.getSplit();
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSourceReader.java
git commit -m "feat: 添加 HttpSourceReader 源阅读器"
```

---

### Task 11: 创建 HttpSplitEnumerator

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSplitEnumerator.java`

- [ ] **Step 1: 创建分片枚举器**

```java
package com.etl.source.http;

import com.etl.core.source.BaseSplitEnumerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * HTTP 分片枚举器
 * 单分片模式，创建一个包含完整配置的分片
 */
@Slf4j
public class HttpSplitEnumerator extends BaseSplitEnumerator<HttpSplit, HttpEnumCheckpoint> {

    private final HttpSourceConfig httpSourceConfig;

    /**
     * 构造函数
     *
     * @param context           枚举器上下文
     * @param httpSourceConfig  HTTP 配置
     */
    public HttpSplitEnumerator(
            SplitEnumeratorContext<HttpSplit> context,
            HttpSourceConfig httpSourceConfig) {
        super(context);
        this.httpSourceConfig = httpSourceConfig;
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context           枚举器上下文
     * @param checkpoint        检查点
     * @param httpSourceConfig  HTTP 配置
     */
    public HttpSplitEnumerator(
            SplitEnumeratorContext<HttpSplit> context,
            HttpEnumCheckpoint checkpoint,
            HttpSourceConfig httpSourceConfig) {
        super(context, checkpoint);
        this.httpSourceConfig = httpSourceConfig;
    }

    @Override
    public void start() {
        log.info("HttpSplitEnumerator 启动，URL: {}", httpSourceConfig.getUrl());

        // 创建单分片
        HttpSplit split = new HttpSplit("http-split-0", httpSourceConfig);

        // 添加到待处理队列
        addPendingSplits(Collections.singletonList(split));
        log.info("创建 HTTP 分片: {}", split);
    }

    @Override
    public HttpEnumCheckpoint snapshotState(long checkpointId) {
        List<HttpSplit> pending = List.copyOf(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new HttpEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("HttpSplitEnumerator 关闭");
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSplitEnumerator.java
git commit -m "feat: 添加 HttpSplitEnumerator 分片枚举器"
```

---

### Task 12: 创建 HttpSource

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSource.java`

- [ ] **Step 1: 创建 Source 主类**

```java
package com.etl.source.http;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.function.Supplier;

/**
 * HTTP Source 实现
 * 支持 GET/POST 请求获取 JSON 数据
 */
@Slf4j
public class HttpSource extends AbstractSplitSource<HttpSplit, HttpEnumCheckpoint> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final HttpSourceConfig httpSourceConfig;

    @SuppressWarnings("unchecked")
    public HttpSource(SourceConfig config) {
        super(config);

        // URL（必填）
        String url = config.getString("url");
        Preconditions.checkArgument(url != null && !url.isEmpty(), "url is null or empty");

        // HTTP 方法（可选，默认 GET）
        String method = config.getString("method", "GET");
        Preconditions.checkArgument("GET".equalsIgnoreCase(method) || "POST".equalsIgnoreCase(method),
                "method must be GET or POST");

        // 请求头（可选）
        Map<String, String> headers = (Map<String, String>) config.get("headers");

        // 查询参数（可选）
        Map<String, String> params = (Map<String, String>) config.get("params");

        // 请求体（可选）
        String body = null;
        Object bodyObj = config.get("body");
        if (bodyObj != null) {
            if (bodyObj instanceof String) {
                body = (String) bodyObj;
            } else {
                try {
                    body = OBJECT_MAPPER.writeValueAsString(bodyObj);
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException("body 序列化失败: " + e.getMessage(), e);
                }
            }
        }

        // JSONPath（可选）
        String dataPath = config.getString("dataPath");

        // Schema（必填）
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");

        // 封装配置
        this.httpSourceConfig = HttpSourceConfig.builder()
                .url(url)
                .method(method.toUpperCase())
                .headers(headers)
                .params(params)
                .body(body)
                .dataPath(dataPath)
                .schema(schema)
                .build();

        log.info("创建 HttpSource: url={}, method={}, dataPath={}", url, method, dataPath);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<HttpSplit, HttpEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new HttpSplitEnumerator(enumContext, httpSourceConfig);
    }

    @Override
    public SplitEnumerator<HttpSplit, HttpEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext,
            HttpEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new HttpSplitEnumerator(enumContext, checkpoint, httpSourceConfig);
    }

    @Override
    public SourceReader<Row, HttpSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, HttpSplit>>) () ->
                new HttpSplitReader();
        return new HttpSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<HttpSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<HttpEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSource.java
git commit -m "feat: 添加 HttpSource 主类"
```

---

### Task 13: 创建 HttpSourcePlugin

**Files:**
- Create: `flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSourcePlugin.java`

- [ ] **Step 1: 创建 SPI 插件入口**

```java
package com.etl.source.http;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;

/**
 * HTTP Source 插件
 * 支持从 REST API 获取 JSON 数据
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class HttpSourcePlugin implements SourcePlugin {

    @Override
    public String getType() {
        return "http";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        log.info("创建 HTTP Source");
        return new HttpSource(config);
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-source/flink-etl-source-http/src/main/java/com/etl/source/http/HttpSourcePlugin.java
git commit -m "feat: 添加 HttpSourcePlugin SPI 插件入口"
```

---

### Task 14: 更新客户端依赖

**Files:**
- Modify: `flink-etl-client/pom.xml`

- [ ] **Step 1: 添加 HTTP Source 依赖**

在 `flink-etl-client/pom.xml` 的 `<dependencies>` 中添加：

```xml
<!-- HTTP Source 插件 -->
<dependency>
    <groupId>com.etl</groupId>
    <artifactId>flink-etl-source-http</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 2: Commit**

```bash
git add flink-etl-client/pom.xml
git commit -m "feat: 客户端添加 HTTP Source 依赖"
```

---

### Task 15: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 在 Source 插件目录中添加 HTTP Source 条目**

在 `<details>` 的 Source 插件目录中添加：

```markdown
  - [HTTP Source](#http-source)
```

- [ ] **Step 2: 在 LocalFile Source 后添加 HTTP Source 文档**

```markdown
---

### HTTP Source

从 HTTP API 获取 JSON 数据，支持 GET 和 POST 请求。

#### 配置参数

| 参数 | 必填 | 默认值 | 说明 |
|------|:----:|--------|------|
| `url` | 是 | - | 请求 URL |
| `method` | 否 | `GET` | HTTP 方法，支持 `GET`、`POST` |
| `headers` | 否 | `{}` | 请求头，键值对形式 |
| `params` | 否 | `{}` | 查询参数，键值对形式 |
| `body` | 否 | `null` | 请求体，JSON 对象形式 |
| `dataPath` | 否 | `null` | JSONPath 表达式，提取数据 |
| `schema` | 是 | - | Schema 定义，描述单条记录结构 |

#### dataPath 结果处理

| 提取结果类型 | 处理方式 |
|-------------|---------|
| JSONArray | 遍历数组，每个元素作为一行数据发送 |
| JSONObject | 作为单行数据发送 |
| 其他类型 | 抛出异常，提示数据格式错误 |

#### 配置示例

**GET 请求，直接返回数组：**

```json
{
  "source": {
    "type": "http",
    "outputTable": "users",
    "config": {
      "url": "https://api.example.com/users",
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "email": "STRING"
      }
    }
  }
}
```

**POST 请求，带请求体和 JSONPath 提取：**

```json
{
  "source": {
    "type": "http",
    "outputTable": "users",
    "config": {
      "url": "https://api.example.com/users/query",
      "method": "POST",
      "headers": {
        "Authorization": "Bearer token123"
      },
      "body": {
        "status": "active"
      },
      "dataPath": "$.data.items",
      "schema": {
        "id": "LONG",
        "name": "STRING",
        "tags": "ARRAY<STRING>",
        "address": {
          "city": "STRING",
          "zip": "STRING"
        }
      }
    }
  }
}
```

#### 数据解析说明

- 未配置 `dataPath` 时，使用整个响应体作为数据源
- 配置 `dataPath` 后，使用 JSONPath 提取结果作为数据源
- Schema 始终描述单条记录的结构
- 支持复杂类型：`ARRAY<简单类型>` 和 `OBJECT`（嵌套结构）
```

- [ ] **Step 3: Commit**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 PLUGINS.md 添加 HTTP Source 文档"
```

---

### Task 16: 编译验证

**Files:**
- 无新增/修改

- [ ] **Step 1: 编译项目**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn clean compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 2: 安装到本地仓库**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn clean install -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: 打包测试**

Run: `cd c:/Users/admin/Desktop/data-processer && mvn clean package -DskipTests`
Expected: BUILD SUCCESS，生成 JAR 文件

---

### Task 17: 创建测试配置文件

**Files:**
- Create: `docs/examples/http-to-console.json`

- [ ] **Step 1: 创建示例配置文件**

```json
{
  "job": {
    "name": "http-to-console",
    "mode": "batch",
    "parallelism": 1
  },
  "sources": [
    {
      "type": "http",
      "outputTable": "api_data",
      "config": {
        "url": "https://jsonplaceholder.typicode.com/users",
        "schema": {
          "id": "LONG",
          "name": "STRING",
          "email": "STRING",
          "username": "STRING"
        }
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "api_data",
      "config": {}
    }
  ]
}
```

- [ ] **Step 2: Commit**

```bash
git add docs/examples/http-to-console.json
git commit -m "docs: 添加 HTTP Source 示例配置"
```

---

### Task 18: 功能验证

**Files:**
- 无新增/修改

- [ ] **Step 1: 运行测试任务**

Run: `cd c:/Users/admin/Desktop/data-processer && java -jar flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar --file docs/examples/http-to-console.json`
Expected: 成功执行，控制台输出 API 数据

- [ ] **Step 2: 最终提交**

```bash
git add -A
git commit -m "feat: 完成 HTTP Source 插件实现"
```