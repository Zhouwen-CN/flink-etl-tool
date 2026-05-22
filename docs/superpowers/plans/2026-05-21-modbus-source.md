# Modbus Source 连接器实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 Modbus TCP Source 连接器，通过 Modbus TCP 协议读取 Holding Registers，支持批处理和流处理模式。

**Architecture:** 完全参照 connector-mock 的 Source 架构（单分片模式），使用 modbus4j 库建立 TCP 连接读取寄存器。配置类做参数校验和
host 拆分，Split 持有完整配置，SplitReader 负责建连和数据读取。固定输出 Schema (address: INT, value: INT)，不需要用户配置
schema。

**Tech Stack:** Java 1.8, Apache Flink 1.15.2 (FLIP-27 Source API), modbus4j 3.1.0, Lombok, Google AutoService

---

## 文件结构

| 操作 | 文件路径                                                                                                                    | 职责                                     |
|----|-------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| 创建 | `flink-etl-connector/connector-modbus/pom.xml`                                                                          | Maven 模块定义，引入 modbus4j 依赖              |
| 修改 | `flink-etl-connector/pom.xml`                                                                                           | 添加 connector-modbus 子模块                |
| 修改 | `pom.xml` (根)                                                                                                           | dependencyManagement 中添加 modbus4j 版本管理 |
| 修改 | `flink-etl-client/pom.xml`                                                                                              | 添加 connector-modbus 依赖                 |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java`     | 配置类，参数校验 + host 拆分                     |
| 创建 | `flink-etl-connector/connector-modbus/src/test/java/com/etl/connector/modbus/source/config/ModbusSourceConfigTest.java` | 配置类单元测试                                |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplit.java`                   | 单分片，持有 ModbusSourceConfig              |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitState.java`              | 分片状态                                   |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusEnumCheckpoint.java`          | 枚举器检查点                                 |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusRecordEmitter.java`           | 记录发射器                                  |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitEnumerator.java`         | 分片枚举器，创建单分片                            |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java`             | 核心读取逻辑，连接 Modbus TCP 读寄存器              |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSourceReader.java`            | 阅读器，包装 SplitReader                     |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSource.java`                  | 主类，继承 AbstractSplitSource              |
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSourcePlugin.java`            | SPI 入口                                 |
| 创建 | `docs/examples/batch-modbus2console.json`                                                                               | 批处理示例配置                                |
| 创建 | `docs/examples/stream-modbus2console.json`                                                                              | 流处理示例配置                                |
| 修改 | `PLUGINS.md`                                                                                                            | 添加 Modbus Source 插件文档                  |

---

### Task 1: Maven 模块配置

**Files:**

- Create: `flink-etl-connector/connector-modbus/pom.xml`
- Modify: `flink-etl-connector/pom.xml`
- Modify: `pom.xml` (根)
- Modify: `flink-etl-client/pom.xml`

- [ ] **Step 1: 创建 connector-modbus/pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.etl</groupId>
        <artifactId>flink-etl-connector</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>connector-modbus</artifactId>

    <name>Flink ETL Connector - Modbus</name>
    <description>Modbus TCP 连接器（Source，读取 Holding Registers）</description>

    <dependencies>
        <dependency>
            <groupId>com.infiniteautomation</groupId>
            <artifactId>modbus4j</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 在根 pom.xml 的 `<properties>` 中添加 modbus4j 版本属性**

在 `<org.eclipse.paho.client.mqttv3.version>1.2.5</org.eclipse.paho.client.mqttv3.version>` 之后添加：

```xml

<modbus4j.version>3.1.0</modbus4j.version>
```

- [ ] **Step 3: 在根 pom.xml 的 `<dependencyManagement>` 中添加 modbus4j 版本管理**

在 `<!-- JSONPath 解析 -->` 块之后添加：

```xml
<!-- Modbus 通信 -->
<dependency>
    <groupId>com.infiniteautomation</groupId>
    <artifactId>modbus4j</artifactId>
    <version>${modbus4j.version}</version>
</dependency>
```

- [ ] **Step 4: 在 `flink-etl-connector/pom.xml` 的 `<modules>` 中添加子模块**

在 `<module>connector-mqtt</module>` 之后添加：

```xml

<module>connector-modbus</module>
```

- [ ] **Step 5: 在 `flink-etl-client/pom.xml` 的 `<dependencies>` 中添加 connector-modbus 依赖**

在 connector-mqtt 依赖之后添加：

```xml

<dependency>
    <groupId>com.etl</groupId>
    <artifactId>connector-modbus</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 6: 验证编译**

运行：`mvn clean compile -pl flink-etl-connector/connector-modbus -am`

预期：BUILD SUCCESS（模块结构正确，依赖可解析）

- [ ] **Step 7: 提交**

```bash
git add flink-etl-connector/connector-modbus/pom.xml flink-etl-connector/pom.xml pom.xml flink-etl-client/pom.xml
git commit -m "feat(modbus): 新增 connector-modbus 模块，引入 modbus4j 依赖"
```

---

### Task 2: ModbusSourceConfig 配置类（TDD）

**Files:**

- Create:
  `flink-etl-connector/connector-modbus/src/test/java/com/etl/connector/modbus/source/config/ModbusSourceConfigTest.java`
- Create:
  `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java`

- [ ] **Step 1: 编写配置类测试**

```java
package com.etl.connector.modbus.source.config;

import com.etl.core.config.SourceConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ModbusSourceConfigTest {

    private SourceConfig createSourceConfig(Map<String, Object> configMap) {
        SourceConfig config = new SourceConfig();
        config.setType("modbus");
        config.setOutputTable("test_table");
        config.setConfig(configMap);
        return config;
    }

    @Test
    void testValidConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("slaveId", 1);
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.BATCH);

        assertEquals("192.168.1.100", result.getIp());
        assertEquals(502, result.getPort());
        assertEquals(1, result.getSlaveId());
        assertEquals(0, result.getStartAddress());
        assertEquals(10, result.getQuantity());
        assertTrue(result.isBounded());
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "10.0.0.1:502");
        configMap.put("startAddress", 100);
        configMap.put("quantity", 5);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.STREAMING);

        assertEquals(1, result.getSlaveId());
        assertEquals(1000L, result.getIntervalMs());
        assertFalse(result.isBounded());
    }

    @Test
    void testCustomIntervalMs() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "10.0.0.1:502");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 5);
        configMap.put("intervalMs", 2000);

        ModbusSourceConfig result = ModbusSourceConfig.fromSourceConfig(
                createSourceConfig(configMap), RuntimeExecutionMode.STREAMING);

        assertEquals(2000L, result.getIntervalMs());
    }

    @Test
    void testHostMissing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_noPort() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_portNotNumber() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:abc");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testHostInvalidFormat_portOutOfRange() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:99999");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testSlaveIdOutOfRange() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("slaveId", 248);
        configMap.put("startAddress", 0);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testStartAddressNegative() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("startAddress", -1);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testQuantityMissing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("startAddress", 0);

        assertThrows(NullPointerException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testQuantityZero() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("startAddress", 0);
        configMap.put("quantity", 0);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }

    @Test
    void testAddressPlusQuantityOverflow() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100:502");
        configMap.put("startAddress", 65530);
        configMap.put("quantity", 10);

        assertThrows(IllegalArgumentException.class, () ->
                ModbusSourceConfig.fromSourceConfig(
                        createSourceConfig(configMap), RuntimeExecutionMode.BATCH));
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

运行：`mvn test -pl flink-etl-connector/connector-modbus -Dtest=ModbusSourceConfigTest -am`

预期：编译失败，`ModbusSourceConfig` 类不存在

- [ ] **Step 3: 实现 ModbusSourceConfig**

```java
package com.etl.connector.modbus.source.config;

import com.etl.core.config.SourceConfig;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Modbus Source 配置封装类
 */
@Data
@Builder
@Slf4j
public class ModbusSourceConfig implements Serializable {

    /** 是否有界 */
    private final boolean bounded;

    /** Modbus TCP IP 地址 */
    private final String ip;

    /** Modbus TCP 端口 */
    private final int port;

    /** 从站地址 */
    private final int slaveId;

    /** 寄存器起始地址（0-based） */
    private final int startAddress;

    /** 读取寄存器数量 */
    private final int quantity;

    /** 轮询间隔（毫秒），流处理模式使用 */
    private final long intervalMs;

    /**
     * 从 SourceConfig 创建 ModbusSourceConfig，在此完成所有参数校验
     */
    public static ModbusSourceConfig fromSourceConfig(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        boolean bounded = runtimeMode == RuntimeExecutionMode.BATCH;

        // 1. 校验并拆分 host
        String host = config.getString("host");
        Preconditions.checkArgument(host != null && !host.isEmpty(), "配置项 'host' 不能为空");

        String[] parts = host.split(":");
        Preconditions.checkArgument(parts.length == 2, "配置项 'host' 格式必须为 'ip:port'，当前值: %s", host);

        String ip = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 'host' 中的端口必须为数字，当前值: " + parts[1]);
        }
        Preconditions.checkArgument(port >= 1 && port <= 65535,
                "配置项 'host' 中的端口范围必须为 1-65535，当前值: %s", port);

        // 2. 校验 slaveId
        int slaveId = config.getInteger("slaveId", 1);
        Preconditions.checkArgument(slaveId >= 1 && slaveId <= 247,
                "配置项 'slaveId' 范围必须为 1-247，当前值: %s", slaveId);

        // 3. 校验 startAddress
        Integer startAddress = config.getInteger("startAddress");
        Preconditions.checkNotNull(startAddress, "配置项 'startAddress' 不能为空");
        Preconditions.checkArgument(startAddress >= 0,
                "配置项 'startAddress' 必须 >= 0，当前值: %s", startAddress);

        // 4. 校验 quantity
        Integer quantity = config.getInteger("quantity");
        Preconditions.checkNotNull(quantity, "配置项 'quantity' 不能为空");
        Preconditions.checkArgument(quantity > 0,
                "配置项 'quantity' 必须 > 0，当前值: %s", quantity);
        Preconditions.checkArgument(startAddress + quantity <= 65536,
                "startAddress(%s) + quantity(%s) 不能超过 65536", startAddress, quantity);

        // 5. 校验 intervalMs
        long intervalMs = config.getLong("intervalMs", 1000L);
        Preconditions.checkArgument(intervalMs > 0,
                "配置项 'intervalMs' 必须 > 0，当前值: %s", intervalMs);

        log.info("创建 ModbusSource: bounded={}, host={}:{}, slaveId={}, startAddress={}, quantity={}, intervalMs={}",
                bounded, ip, port, slaveId, startAddress, quantity, intervalMs);

        return ModbusSourceConfig.builder()
                .bounded(bounded)
                .ip(ip)
                .port(port)
                .slaveId(slaveId)
                .startAddress(startAddress)
                .quantity(quantity)
                .intervalMs(intervalMs)
                .build();
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

运行：`mvn test -pl flink-etl-connector/connector-modbus -Dtest=ModbusSourceConfigTest -am`

预期：全部 12 个测试通过

- [ ] **Step 5: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/
git commit -m "feat(modbus): 新增 ModbusSourceConfig 配置类及单元测试"
```

---

### Task 3: 分片相关类（ModbusSplit、ModbusSplitState、ModbusEnumCheckpoint、ModbusRecordEmitter）

**Files:**

- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplit.java`
- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitState.java`
- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusEnumCheckpoint.java`
- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusRecordEmitter.java`

- [ ] **Step 1: 创建 ModbusSplit**

```java
package com.etl.connector.modbus.source;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import lombok.Getter;

/**
 * Modbus Source 单分片
 */
@Getter
public class ModbusSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    private final String splitId = "modbus-split-0";

    private final ModbusSourceConfig modbusConfig;

    public ModbusSplit(ModbusSourceConfig modbusConfig) {
        this.modbusConfig = modbusConfig;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "ModbusSplit{" +
                "splitId='" + splitId + '\'' +
                '}';
    }
}
```

- [ ] **Step 2: 创建 ModbusSplitState**

```java
package com.etl.connector.modbus.source;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;

/**
 * Modbus Split 状态
 */
@Getter
public class ModbusSplitState extends BaseSplitState<ModbusSplit> {

    public ModbusSplitState(ModbusSplit split) {
        super(split);
    }
}
```

- [ ] **Step 3: 创建 ModbusEnumCheckpoint**

```java
package com.etl.connector.modbus.source;

import com.etl.core.source.AbstractEnumCheckpoint;
import lombok.Getter;

import java.util.List;

/**
 * Modbus Source Enumerator 检查点
 */
@Getter
public class ModbusEnumCheckpoint extends AbstractEnumCheckpoint<ModbusSplit> {

    public ModbusEnumCheckpoint(List<ModbusSplit> pendingSplits) {
        super(pendingSplits);
    }
}
```

- [ ] **Step 4: 创建 ModbusRecordEmitter**

```java
package com.etl.connector.modbus.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

/**
 * Modbus Source 记录发射器
 */
public class ModbusRecordEmitter implements RecordEmitter<Row, Row, ModbusSplitState> {

    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, ModbusSplitState splitState) throws Exception {
        output.collect(record);
        splitState.addRecordsRead(1);
    }
}
```

- [ ] **Step 5: 验证编译**

运行：`mvn clean compile -pl flink-etl-connector/connector-modbus -am`

预期：BUILD SUCCESS

- [ ] **Step 6: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplit.java flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitState.java flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusEnumCheckpoint.java flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusRecordEmitter.java
git commit -m "feat(modbus): 新增分片相关类 ModbusSplit、ModbusSplitState、ModbusEnumCheckpoint、ModbusRecordEmitter"
```

---

### Task 4: ModbusSplitEnumerator

**Files:**

- Create:
  `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitEnumerator.java`

- [ ] **Step 1: 创建 ModbusSplitEnumerator**

```java
package com.etl.connector.modbus.source;

import com.etl.core.source.BaseSplitEnumerator;
import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Modbus Source 分片枚举器
 * 在 start() 时创建单个 ModbusSplit 并分配到队列
 */
@Slf4j
public class ModbusSplitEnumerator
        extends BaseSplitEnumerator<ModbusSplit, ModbusEnumCheckpoint> {

    private final ModbusSourceConfig modbusConfig;

    public ModbusSplitEnumerator(
            SplitEnumeratorContext<ModbusSplit> context,
            ModbusSourceConfig modbusConfig) {
        super(context);
        this.modbusConfig = modbusConfig;
    }

    public ModbusSplitEnumerator(
            SplitEnumeratorContext<ModbusSplit> context,
            ModbusEnumCheckpoint checkpoint,
            ModbusSourceConfig modbusConfig) {
        super(context, checkpoint);
        this.modbusConfig = modbusConfig;
    }

    @Override
    public void start() {
        ModbusSplit split = new ModbusSplit(modbusConfig);
        pendingSplits.add(split);
        log.info("Modbus Source 创建单分片: {}", split.splitId());
    }

    @Override
    public ModbusEnumCheckpoint snapshotState(long checkpointId) {
        List<ModbusSplit> pending = new ArrayList<>(pendingSplits);
        return new ModbusEnumCheckpoint(pending);
    }

    @Override
    public void close() {
        log.info("ModbusSplitEnumerator 关闭");
    }
}
```

- [ ] **Step 2: 验证编译**

运行：`mvn clean compile -pl flink-etl-connector/connector-modbus -am`

预期：BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitEnumerator.java
git commit -m "feat(modbus): 新增 ModbusSplitEnumerator 分片枚举器"
```

---

### Task 5: ModbusSplitReader（核心读取逻辑）

**Files:**

- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java`

- [ ] **Step 1: 创建 ModbusSplitReader**

```java
package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import com.etl.core.source.BaseSplitReader;
import com.serotonin.modbus4j.ModbusFactory;
import com.serotonin.modbus4j.ModbusMaster;
import com.serotonin.modbus4j.code.DataType;
import com.serotonin.modbus4j.code.RegisterRange;
import com.serotonin.modbus4j.ip.IpParameters;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

/**
 * Modbus Split 读取器
 * <p>
 * 通过 Modbus TCP 协议读取 Holding Registers，每个寄存器生成一行 Row(address, value)。
 * <ul>
 *   <li>批处理模式：读取一次后标记分片完成</li>
 *   <li>流处理模式：每次读取后 sleep intervalMs，持续循环</li>
 * </ul>
 * <p>
 * 配置信息从 Split 中获取，不通过构造函数传递
 */
@Slf4j
public class ModbusSplitReader implements BaseSplitReader<Row, ModbusSplit> {

    /** Modbus 保持寄存器的手册地址起始偏移 */
    private static final int HOLDING_REGISTER_OFFSET = 40001;

    private final Queue<ModbusSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    private ModbusSplit currentSplit;
    private ModbusSourceConfig currentConfig;
    private ModbusMaster master;
    private long readCount = 0;

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        if (currentSplit == null) {
            ModbusSplit split = pendingSplits.poll();
            if (split == null) {
                RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }

            currentSplit = split;
            currentConfig = split.getModbusConfig();
            initMaster();
            log.info("开始读取分片: {}", split.splitId());
        }

        return readRegisters();
    }

    /**
     * 初始化 Modbus TCP 连接
     */
    private void initMaster() throws IOException {
        IpParameters params = new IpParameters();
        params.setHost(currentConfig.getIp());
        params.setPort(currentConfig.getPort());

        ModbusFactory factory = new ModbusFactory();
        master = factory.createTcpMaster(params, false);
        try {
            master.init();
            log.info("Modbus TCP 连接成功: {}:{}", currentConfig.getIp(), currentConfig.getPort());
        } catch (Exception e) {
            throw new IOException("Modbus TCP 连接失败: " + currentConfig.getIp() + ":" + currentConfig.getPort(), e);
        }
    }

    /**
     * 读取所有寄存器并生成 Row 数据
     */
    private RecordsWithSplitIds<Row> readRegisters() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        try {
            int slaveId = currentConfig.getSlaveId();
            int startAddress = currentConfig.getStartAddress();
            int quantity = currentConfig.getQuantity();

            for (int i = 0; i < quantity; i++) {
                int offset = startAddress + i;
                Number value = (Number) master.getValue(
                        slaveId,
                        RegisterRange.HOLDING_REGISTER,
                        offset,
                        DataType.TWO_BYTE_INT_SIGNED
                );

                int address = HOLDING_REGISTER_OFFSET + offset;
                Row row = Row.withPositions(RowKind.INSERT, 2);
                row.setField(0, address);
                row.setField(1, value.intValue());

                builder.add(currentSplit.splitId(), row);
                readCount++;
                log.debug("读取寄存器: address={}, value={}", address, value.intValue());
            }
        } catch (Exception e) {
            throw new IOException("读取 Modbus 寄存器失败", e);
        }

        if (currentConfig.isBounded()) {
            // 批处理模式：读完标记分片完成
            finishedSplits.add(currentSplit.splitId());
            log.info("批处理模式数据读取完毕，共 {} 行", readCount);
            destroyMaster();
            currentSplit = null;
            currentConfig = null;
        } else {
            // 流处理模式：sleep 后继续
            try {
                Thread.sleep(currentConfig.getIntervalMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return builder.build();
    }

    /**
     * 释放 Modbus 连接
     */
    private void destroyMaster() {
        if (master != null) {
            master.destroy();
            master = null;
            log.info("Modbus TCP 连接已释放");
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<ModbusSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个新分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        destroyMaster();
        log.info("ModbusSplitReader 关闭，共读取 {} 行数据", readCount);
    }
}
```

- [ ] **Step 2: 验证编译**

运行：`mvn clean compile -pl flink-etl-connector/connector-modbus -am`

预期：BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java
git commit -m "feat(modbus): 新增 ModbusSplitReader，实现 Modbus TCP 寄存器读取"
```

---

### Task 6: ModbusSourceReader 和 ModbusSource

**Files:**

- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSourceReader.java`
- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSource.java`

- [ ] **Step 1: 创建 ModbusSourceReader**

```java
package com.etl.connector.modbus.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.BaseSplitReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Modbus Source 阅读器
 */
public class ModbusSourceReader
        extends AbstractSourceReader<Row, Row, ModbusSplit, ModbusSplitState> {

    public ModbusSourceReader(
            Supplier<BaseSplitReader<Row, ModbusSplit>> splitReaderSupplier,
            SourceReaderContext context) {
        super(splitReaderSupplier, new ModbusRecordEmitter(), context);
    }

    @Override
    public ModbusSplitState initializedState(ModbusSplit split) {
        return new ModbusSplitState(split);
    }

    @Override
    protected ModbusSplit toSplitType(String splitId, ModbusSplitState splitState) {
        return splitState.getSplit();
    }
}
```

- [ ] **Step 2: 创建 ModbusSource**

```java
package com.etl.connector.modbus.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Modbus Source 主类
 * <p>
 * 通过 Modbus TCP 协议读取 Holding Registers。
 * 输出固定 Schema: Row(address: INT, value: INT)
 */
@Slf4j
public class ModbusSource extends AbstractSplitSource<ModbusSplit, ModbusEnumCheckpoint> {

    private final ModbusSourceConfig modbusConfig;
    private final boolean bounded;

    public ModbusSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        super(config);
        this.modbusConfig = ModbusSourceConfig.fromSourceConfig(config, runtimeMode);
        this.bounded = runtimeMode == RuntimeExecutionMode.BATCH;
    }

    @Override
    public Boundedness getBoundedness() {
        if (bounded) {
            return Boundedness.BOUNDED;
        }
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(
                new String[]{"address", "value"},
                Types.INT, Types.INT
        );
    }

    @Override
    public SplitEnumerator<ModbusSplit, ModbusEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<ModbusSplit> enumContext) {
        log.info("创建 ModbusSplitEnumerator");
        return new ModbusSplitEnumerator(enumContext, modbusConfig);
    }

    @Override
    public SplitEnumerator<ModbusSplit, ModbusEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<ModbusSplit> enumContext,
            ModbusEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 ModbusSplitEnumerator");
        return new ModbusSplitEnumerator(enumContext, checkpoint, modbusConfig);
    }

    @Override
    public SourceReader<Row, ModbusSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 ModbusSourceReader");
        Supplier<BaseSplitReader<Row, ModbusSplit>> splitReaderSupplier = ModbusSplitReader::new;
        return new ModbusSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<ModbusSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<ModbusEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
```

- [ ] **Step 3: 验证编译**

运行：`mvn clean compile -pl flink-etl-connector/connector-modbus -am`

预期：BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSourceReader.java flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSource.java
git commit -m "feat(modbus): 新增 ModbusSourceReader 和 ModbusSource 主类"
```

---

### Task 7: ModbusSourcePlugin（SPI 入口）

**Files:**

- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSourcePlugin.java`

- [ ] **Step 1: 创建 ModbusSourcePlugin**

```java
package com.etl.connector.modbus.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.types.Row;

/**
 * Modbus Source 插件
 * 通过 Modbus TCP 读取 Holding Registers
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class ModbusSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "modbus";
    }

    @Override
    public Source<Row, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 Modbus Source");
        return new ModbusSource(config, runtimeMode);
    }
}
```

- [ ] **Step 2: 验证编译（确保 AutoService 生成 SPI 配置）**

运行：`mvn clean compile -pl flink-etl-connector/connector-modbus -am`

预期：BUILD SUCCESS，且 `target/classes/META-INF/services/com.etl.core.spi.SourcePlugin` 文件存在并包含
`com.etl.connector.modbus.source.ModbusSourcePlugin`

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSourcePlugin.java
git commit -m "feat(modbus): 新增 ModbusSourcePlugin SPI 入口"
```

---

### Task 8: 运行全量测试

- [ ] **Step 1: 运行所有测试确保无回归**

运行：`mvn clean test`

预期：BUILD SUCCESS，所有测试通过

- [ ] **Step 2: 运行全量编译打包**

运行：`mvn clean package -DskipTests`

预期：BUILD SUCCESS，flink-etl-client 的 fat jar 包含 modbus 相关类

- [ ] **Step 3: 提交（如有修复）**

如果全量测试暴露需要修复的问题，修复后提交。

---

### Task 9: 示例配置和文档更新

**Files:**

- Create: `docs/examples/batch-modbus2console.json`
- Create: `docs/examples/stream-modbus2console.json`
- Modify: `PLUGINS.md`

- [ ] **Step 1: 创建批处理示例配置**

```json
{
  "job": {
    "name": "batch-modbus2console",
    "mode": "batch",
    "parallelism": 1
  },
  "sources": [
    {
      "type": "modbus",
      "outputTable": "modbus_data",
      "config": {
        "host": "192.168.1.100:502",
        "slaveId": 1,
        "startAddress": 0,
        "quantity": 10
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "modbus_data"
    }
  ]
}
```

- [ ] **Step 2: 创建流处理示例配置**

```json
{
  "job": {
    "name": "stream-modbus2console",
    "mode": "streaming",
    "parallelism": 1
  },
  "sources": [
    {
      "type": "modbus",
      "outputTable": "modbus_data",
      "config": {
        "host": "192.168.1.100:502",
        "slaveId": 1,
        "startAddress": 0,
        "quantity": 10,
        "intervalMs": 2000
      }
    }
  ],
  "sinks": [
    {
      "type": "console",
      "inputTable": "modbus_data"
    }
  ]
}
```

- [ ] **Step 3: 在 PLUGINS.md 的目录中添加 Modbus Source 链接**

在 `- [Mock Source](#mock-source)` 之后添加：

```markdown
    - [Modbus Source](#modbus-source)
```

- [ ] **Step 4: 在 PLUGINS.md 的 Mock Source 章节之后添加 Modbus Source 文档**

在 Mock Source 章节（流处理示例之后的下一个 `###` 之前）添加：

```markdown
### Modbus Source

通过 Modbus TCP 协议读取 Holding Registers（功能码 03），支持批处理和流处理模式。

#### 配置参数

| 参数             | 必填 | 默认值    | 说明                                             |
|----------------|:--:|--------|------------------------------------------------|
| `host`         | 是  | -      | Modbus TCP 地址，格式 `ip:port`                     |
| `slaveId`      | 否  | `1`    | 从站地址（Device ID），范围 1-247                       |
| `startAddress` | 是  | -      | 寄存器起始地址（0-based）                               |
| `quantity`     | 是  | -      | 读取寄存器数量，`startAddress + quantity` 不超过 65536     |
| `intervalMs`   | 否  | `1000` | 流处理模式下的轮询间隔（毫秒）                               |

**输出 Schema（固定，无需配置）：**

| 列名        | 类型  | 说明                                    |
|-----------|-----|---------------------------------------|
| `address` | INT | 设备手册地址，从 `40001 + startAddress` 开始    |
| `value`   | INT | 寄存器值（有符号）                             |

每次读取产生 `quantity` 行数据，每行对应一个寄存器。

**运行模式说明：**

- **Batch 模式**：读取一次所有寄存器后程序退出
- **Streaming 模式**：按 `intervalMs` 间隔持续轮询寄存器

#### 配置示例

**Batch 模式：**

​```json
{
  "source": {
    "type": "modbus",
    "outputTable": "modbus_data",
    "config": {
      "host": "192.168.1.100:502",
      "slaveId": 1,
      "startAddress": 0,
      "quantity": 10
    }
  }
}
​```

**Streaming 模式：**

​```json
{
  "source": {
    "type": "modbus",
    "outputTable": "modbus_data",
    "config": {
      "host": "192.168.1.100:502",
      "slaveId": 1,
      "startAddress": 0,
      "quantity": 10,
      "intervalMs": 2000
    }
  }
}
​```
```

- [ ] **Step 5: 提交**

```bash
git add docs/examples/batch-modbus2console.json docs/examples/stream-modbus2console.json PLUGINS.md
git commit -m "docs(modbus): 新增 Modbus Source 示例配置和插件文档"
```
