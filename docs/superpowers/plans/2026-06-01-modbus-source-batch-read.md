# Modbus Source 分批读取 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Modbus Source 的寄存器读取从单次批量请求改为分批循环读取（每批最多 32 个寄存器），以兼容存在单次读取数量限制的
Modbus 设备。

**Architecture:** 仅修改 `ModbusSplitReader.readRegisters()` 方法，将单次 `ReadHoldingRegistersRequest` 改为 while
循环分批请求，每批最多 `MAX_READ_SIZE = 32` 个寄存器。提取寄存器数据处理逻辑为独立方法以便测试。

**Tech Stack:** Java 1.8, modbus4j 3.1.0, JUnit 5

---

### Task 1: 提取数据处理方法并添加分批常量

**Files:**

- Modify: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java`

- [ ] **Step 1: 添加 MAX_READ_SIZE 常量**

在 `ModbusSplitReader` 类中 `finishedSplits` 字段之前添加常量：

```java
/**
 * 单次请求最大读取寄存器数量，兼容存在读取数量限制的 Modbus 设备
 */
private static final int MAX_READ_SIZE = 32;
```

- [ ] **Step 2: 提取 processRegisterData 方法**

将 `readRegisters()` 中的寄存器数据处理逻辑提取为独立方法，供分批循环调用：

```java
/**
 * 处理寄存器数据并生成 Row 记录
 *
 * @param data       寄存器原始数据
 * @param startAddr  当前批次的起始地址
 * @param wordSize   每个数据值占用的寄存器数
 * @param builder    记录构建器
 */
private void processRegisterData(short[] data, int startAddr, int wordSize, RecordsBySplits.Builder<Row> builder) {
    for (int i = 0; i < data.length; i += wordSize) {
        int registerAddress = startAddr + i;
        int value;
        if (wordSize == 1) {
            value = data[i];
        } else {
            // 2 个寄存器组合为 32bit：高位寄存器在前（大端）
            value = (data[i] << 16) | (data[i + 1] & 0xFFFF);
        }
        Row row = Row.withPositions(RowKind.INSERT, 2);
        row.setField(0, registerAddress);
        row.setField(1, value);

        builder.add(currentSplit.splitId(), row);
        readCount++;
        log.debug("读取寄存器: address={}, value={}", registerAddress, value);
    }
}
```

- [ ] **Step 3: 编译验证**

Run: `mvn compile -pl flink-etl-connector/connector-modbus -am`
Expected: BUILD SUCCESS

- [ ] **Step 4: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java
git commit -m "refactor(modbus): 提取寄存器数据处理方法并添加 MAX_READ_SIZE 常量"
```

---

### Task 2: 改造 readRegisters 为分批循环读取

**Files:**

- Modify: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java`

- [ ] **Step 1: 替换 readRegisters 方法实现**

将 `readRegisters()` 方法中的单次读取逻辑替换为分批循环。完整替换后的方法：

```java
/**
 * 分批读取寄存器并生成 Row 数据
 * 每次请求最多读取 MAX_READ_SIZE 个寄存器，循环读取直到所有寄存器读完
 */
private RecordsWithSplitIds<Row> readRegisters() throws IOException {
    RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

    try {
        int deviceId = currentConfig.getDeviceId();
        int address = currentConfig.getAddress();
        int count = currentConfig.getCount();
        int wordSize = currentConfig.getWordSize();

        int remaining = count;
        int currentAddress = address;
        while (remaining > 0) {
            int batchSize = Math.min(remaining, MAX_READ_SIZE);

            ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(deviceId, currentAddress, batchSize);
            ModbusResponse response = master.send(request);
            if (response.isException()) {
                throw new IOException("Modbus 异常响应: " + response.getExceptionMessage());
            }

            short[] data = ((ReadHoldingRegistersResponse) response).getShortData();
            if (data == null || data.length < batchSize) {
                throw new IOException(String.format("Modbus 响应数据不足: 期望 %d 个寄存器, 实际 %s",
                        batchSize, data == null ? "null" : data.length));
            }

            processRegisterData(data, currentAddress, wordSize, builder);

            currentAddress += batchSize;
            remaining -= batchSize;
        }
    } catch (Exception e) {
        throw new IOException("读取 Modbus 寄存器失败", e);
    }

    if (currentConfig.isBounded()) {
        finishedSplits.add(currentSplit.splitId());
        log.info("批处理模式数据读取完毕，共 {} 行", readCount);
        destroyMaster();
        currentSplit = null;
        currentConfig = null;
    } else {
        try {
            Thread.sleep(currentConfig.getIntervalMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    return builder.build();
}
```

- [ ] **Step 2: 编译验证**

Run: `mvn compile -pl flink-etl-connector/connector-modbus -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java
git commit -m "feat(modbus): readRegisters 改为分批循环读取，每批最多 32 个寄存器"
```

---

### Task 3: 添加 processRegisterData 单元测试

**Files:**

- Create:
  `flink-etl-connector/connector-modbus/src/test/java/com/etl/connector/modbus/source/ModbusSplitReaderTest.java`

- [ ] **Step 1: 创建测试类**

```java
package com.etl.connector.modbus.source;

import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ModbusSplitReaderTest {

    private ModbusSplitReader reader;

    @BeforeEach
    void setUp() {
        reader = new ModbusSplitReader();
    }

    /**
     * 通过反射调用 processRegisterData，验证数据处理逻辑
     */
    private List<Row> invokeProcessRegisterData(short[] data, int startAddr, int wordSize) throws Exception {
        // 构造一个 split 用于 splitId
        ModbusSourceConfig config = ModbusSourceConfig.builder()
                .ip("127.0.0.1").port(502).deviceId(1)
                .address(0).count(data.length).wordSize(wordSize)
                .bounded(true).intervalMs(1000)
                .build();
        ModbusSplit split = new ModbusSplit("test-split", config);

        // 设置 currentSplit 字段
        java.lang.reflect.Field currentSplitField = ModbusSplitReader.class.getDeclaredField("currentSplit");
        currentSplitField.setAccessible(true);
        currentSplitField.set(reader, split);

        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        Method method = ModbusSplitReader.class.getDeclaredMethod(
                "processRegisterData", short[].class, int.class, int.class, RecordsBySplits.Builder.class);
        method.setAccessible(true);
        method.invoke(reader, data, startAddr, wordSize, builder);

        List<Row> rows = new ArrayList<>();
        builder.build().nextSplit().forEachRemaining(record -> rows.add(record.getValue()));
        return rows;
    }

    @Test
    void testProcessRegisterData_wordSize1() throws Exception {
        short[] data = {100, 200, 300};
        List<Row> rows = invokeProcessRegisterData(data, 0, 1);

        assertEquals(3, rows.size());
        assertEquals(0, rows.get(0).getField(0));
        assertEquals(100, rows.get(0).getField(1));
        assertEquals(1, rows.get(1).getField(0));
        assertEquals(200, rows.get(1).getField(1));
        assertEquals(2, rows.get(2).getField(0));
        assertEquals(300, rows.get(2).getField(1));
    }

    @Test
    void testProcessRegisterData_wordSize2() throws Exception {
        // data[0]=1, data[1]=2 → (1 << 16) | 2 = 65538
        short[] data = {1, 2, 3, 4};
        List<Row> rows = invokeProcessRegisterData(data, 10, 2);

        assertEquals(2, rows.size());
        assertEquals(10, rows.get(0).getField(0));
        assertEquals(65538, rows.get(0).getField(1));
        assertEquals(12, rows.get(1).getField(0));
        assertEquals(196612, rows.get(1).getField(1)); // (3 << 16) | 4
    }

    @Test
    void testProcessRegisterData_withOffset() throws Exception {
        short[] data = {42};
        List<Row> rows = invokeProcessRegisterData(data, 100, 1);

        assertEquals(1, rows.size());
        assertEquals(100, rows.get(0).getField(0));
        assertEquals(42, rows.get(0).getField(1));
    }
}
```

- [ ] **Step 2: 运行测试验证通过**

Run: `mvn test -pl flink-etl-connector/connector-modbus -Dtest=ModbusSplitReaderTest`
Expected: Tests run: 3, Failures: 0

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/test/java/com/etl/connector/modbus/source/ModbusSplitReaderTest.java
git commit -m "test(modbus): 添加 processRegisterData 单元测试"
```

---

### Task 4: 更新文档

**Files:**

- Modify: `PLUGINS.md`

- [ ] **Step 1: 更新 PLUGINS.md 中 Modbus Source 的读取行为描述**

在 Modbus Source 章节的"输出说明"部分，补充分批读取的说明。找到相关描述位置，在行为说明中添加：

> Modbus Source 每次请求最多读取 32 个寄存器，当 count 超过 32 时自动分批循环读取，确保兼容存在单次读取数量限制的设备。

- [ ] **Step 2: 提交**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 Modbus Source 分批读取行为说明"
```
