# Modbus Source 改用原生 Socket 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 Java 原生 Socket 自写的 `ModbusTcpClient` 替换 modbus4j 库，实现 Modbus TCP 功能码 0x03（Read Holding Registers），并彻底移除 modbus4j 依赖。

**Architecture:** 新增纯 Java 的 `ModbusTcpClient` 类承载 socket 连接与 MBAP 帧编解码，协议编解码逻辑拆为包级静态方法以便脱离 socket 单测。`ModbusSplitReader` 将 `ModbusMaster master` 字段替换为 `ModbusTcpClient client`，其余分批读取与数据处理逻辑不变。`ModbusSourceConfig` 新增 `timeoutMs` 配置项（连接与读取共用，默认 3000ms）。

**Tech Stack:** Java 1.8, Apache Flink 1.15.2 (FLIP-27 Source API), JUnit 5, Lombok

**设计文档:** `docs/superpowers/specs/2026-06-05-modbus-socket-source-design.md`

---

## 文件结构

| 操作 | 文件 | 职责 |
|------|------|------|
| 创建 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusTcpClient.java` | socket 连接 + MBAP 帧编解码 + 0x03 功能码 |
| 创建 | `flink-etl-connector/connector-modbus/src/test/java/com/etl/connector/modbus/source/ModbusTcpClientTest.java` | 协议编解码单元测试（不开真实 socket） |
| 修改 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java` | 新增 `timeoutMs` 字段与校验 |
| 修改 | `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java` | `ModbusMaster` → `ModbusTcpClient` |
| 修改 | `flink-etl-connector/connector-modbus/pom.xml` | 移除 modbus4j 依赖 |
| 修改 | `pom.xml`（根） | 移除 modbus4j 版本属性、dependencyManagement、ias-releases 仓库、log4j-jcl 桥接 |
| 修改 | `PLUGINS.md` | 更新 Modbus Source 说明（去除 modbus4j、新增 timeoutMs） |

---

## Task 1: 新增 ModbusTcpClient 协议编解码（静态方法 + 单测）

先实现可脱离 socket 测试的纯编解码静态方法。

**Files:**
- Create: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusTcpClient.java`
- Test: `flink-etl-connector/connector-modbus/src/test/java/com/etl/connector/modbus/source/ModbusTcpClientTest.java`

- [ ] **Step 1: 编写编解码失败测试**

创建 `ModbusTcpClientTest.java`：

```java
package com.etl.connector.modbus.source;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class ModbusTcpClientTest {

    /**
     * 验证请求帧格式：MBAP(7) + PDU(5) = 12 字节
     * transactionId=1, deviceId=1, address=0x0010, quantity=0x0002
     */
    @Test
    void testBuildReadRequest() {
        byte[] frame = ModbusTcpClient.buildReadRequest(1, 1, 0x0010, 0x0002);

        assertEquals(12, frame.length);
        // Transaction ID = 1
        assertEquals(0x00, frame[0] & 0xFF);
        assertEquals(0x01, frame[1] & 0xFF);
        // Protocol ID = 0
        assertEquals(0x00, frame[2] & 0xFF);
        assertEquals(0x00, frame[3] & 0xFF);
        // Length = 6
        assertEquals(0x00, frame[4] & 0xFF);
        assertEquals(0x06, frame[5] & 0xFF);
        // Unit ID = 1
        assertEquals(0x01, frame[6] & 0xFF);
        // Function Code = 0x03
        assertEquals(0x03, frame[7] & 0xFF);
        // Start Address = 0x0010
        assertEquals(0x00, frame[8] & 0xFF);
        assertEquals(0x10, frame[9] & 0xFF);
        // Quantity = 0x0002
        assertEquals(0x00, frame[10] & 0xFF);
        assertEquals(0x02, frame[11] & 0xFF);
    }

    /**
     * 验证正常响应解析：读取 2 个寄存器 [0x000A, 0x000B]
     */
    @Test
    void testParseReadResponse_normal() throws IOException {
        // MBAP: tid=1, proto=0, len=7(unit+fc+bytecount+4data), unit=1
        // PDU: fc=0x03, byteCount=4, data=00 0A 00 0B
        byte[] response = {
                0x00, 0x01,       // transaction id
                0x00, 0x00,       // protocol id
                0x00, 0x07,       // length = 7
                0x01,             // unit id
                0x03,             // function code
                0x04,             // byte count
                0x00, 0x0A,       // register 0 = 10
                0x00, 0x0B        // register 1 = 11
        };
        short[] data = ModbusTcpClient.parseReadResponse(
                new ByteArrayInputStream(response), 1, 2);

        assertEquals(2, data.length);
        assertEquals(10, data[0]);
        assertEquals(11, data[1]);
    }

    /**
     * 验证异常响应：function code = 0x83，异常码 0x02
     */
    @Test
    void testParseReadResponse_exception() {
        byte[] response = {
                0x00, 0x01,       // transaction id
                0x00, 0x00,       // protocol id
                0x00, 0x03,       // length = 3 (unit + fc + exceptionCode)
                0x01,             // unit id
                (byte) 0x83,      // function code = 0x03 | 0x80
                0x02              // exception code = ILLEGAL DATA ADDRESS
        };
        IOException ex = assertThrows(IOException.class, () ->
                ModbusTcpClient.parseReadResponse(
                        new ByteArrayInputStream(response), 1, 2));
        assertTrue(ex.getMessage().contains("异常"));
    }

    /**
     * 验证 Transaction ID 不匹配抛异常
     */
    @Test
    void testParseReadResponse_transactionIdMismatch() {
        byte[] response = {
                0x00, 0x02,       // transaction id = 2 (期望 1)
                0x00, 0x00,
                0x00, 0x05,       // length
                0x01,
                0x03,
                0x02,
                0x00, 0x0A
        };
        IOException ex = assertThrows(IOException.class, () ->
                ModbusTcpClient.parseReadResponse(
                        new ByteArrayInputStream(response), 1, 1));
        assertTrue(ex.getMessage().contains("Transaction"));
    }

    /**
     * 验证响应字节不足（流提前结束）抛异常
     */
    @Test
    void testParseReadResponse_insufficientData() {
        byte[] response = {
                0x00, 0x01,
                0x00, 0x00,
                0x00, 0x07,       // 声称还有 7 字节，实际不足
                0x01,
                0x03,
                0x04,
                0x00, 0x0A        // 只给了 1 个寄存器，缺 1 个
        };
        assertThrows(IOException.class, () ->
                ModbusTcpClient.parseReadResponse(
                        new ByteArrayInputStream(response), 1, 2));
    }
}
```

- [ ] **Step 2: 运行测试确认编译失败**

Run: `mvn test -pl flink-etl-connector/connector-modbus -Dtest=ModbusTcpClientTest`
Expected: 编译失败，`ModbusTcpClient` 不存在 / 找不到符号 `buildReadRequest`、`parseReadResponse`

- [ ] **Step 3: 实现 ModbusTcpClient（含编解码静态方法）**

创建 `ModbusTcpClient.java`：

```java
package com.etl.connector.modbus.source;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 轻量 Modbus TCP 客户端
 * <p>
 * 使用 Java 原生 Socket 实现 Modbus TCP 协议，仅支持功能码 0x03（Read Holding Registers）。
 * 协议编解码逻辑拆为包级静态方法 {@link #buildReadRequest} 与 {@link #parseReadResponse}，
 * 便于脱离 socket 进行单元测试。
 */
@Slf4j
public class ModbusTcpClient implements Closeable {

    /** 读取保持寄存器功能码 */
    private static final int FUNCTION_READ_HOLDING_REGISTERS = 0x03;
    /** 异常响应标志位：功能码 | 0x80 */
    private static final int EXCEPTION_FLAG = 0x80;
    /** MBAP 头长度 */
    private static final int MBAP_HEADER_LENGTH = 7;

    private final String ip;
    private final int port;
    private final int deviceId;
    private final int timeoutMs;

    private Socket socket;
    private InputStream in;
    private OutputStream out;
    /** 事务标识符，每次请求自增（0-65535 回绕） */
    private int transactionId = 0;

    public ModbusTcpClient(String ip, int port, int deviceId, int timeoutMs) {
        this.ip = ip;
        this.port = port;
        this.deviceId = deviceId;
        this.timeoutMs = timeoutMs;
    }

    /**
     * 建立 TCP 连接，设置连接超时与读取超时（共用 timeoutMs）
     */
    public void connect() throws IOException {
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(ip, port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            in = socket.getInputStream();
            out = socket.getOutputStream();
            log.info("Modbus TCP 连接成功: {}:{}", ip, port);
        } catch (IOException e) {
            close();
            throw new IOException("Modbus TCP 连接失败: " + ip + ":" + port, e);
        }
    }

    /**
     * 读取保持寄存器（功能码 0x03）
     *
     * @param address  起始地址
     * @param quantity 寄存器数量
     * @return 寄存器值数组
     */
    public short[] readHoldingRegisters(int address, int quantity) throws IOException {
        transactionId = (transactionId + 1) & 0xFFFF;
        byte[] request = buildReadRequest(transactionId, deviceId, address, quantity);
        out.write(request);
        out.flush();
        return parseReadResponse(in, transactionId, quantity);
    }

    /**
     * 构建 Read Holding Registers 请求帧（MBAP 7 字节 + PDU 5 字节 = 12 字节）
     */
    static byte[] buildReadRequest(int transactionId, int deviceId, int address, int quantity) {
        byte[] frame = new byte[12];
        // MBAP
        frame[0] = (byte) ((transactionId >> 8) & 0xFF);
        frame[1] = (byte) (transactionId & 0xFF);
        frame[2] = 0x00; // protocol id high
        frame[3] = 0x00; // protocol id low
        frame[4] = 0x00; // length high
        frame[5] = 0x06; // length low = 6
        frame[6] = (byte) (deviceId & 0xFF);
        // PDU
        frame[7] = (byte) FUNCTION_READ_HOLDING_REGISTERS;
        frame[8] = (byte) ((address >> 8) & 0xFF);
        frame[9] = (byte) (address & 0xFF);
        frame[10] = (byte) ((quantity >> 8) & 0xFF);
        frame[11] = (byte) (quantity & 0xFF);
        return frame;
    }

    /**
     * 解析 Read Holding Registers 响应帧
     *
     * @param in                 输入流
     * @param expectedTxId       期望的 Transaction ID
     * @param expectedQuantity   期望的寄存器数量
     * @return 寄存器值数组
     */
    static short[] parseReadResponse(InputStream in, int expectedTxId, int expectedQuantity)
            throws IOException {
        // 1. 读满 MBAP 头（7 字节）
        byte[] header = new byte[MBAP_HEADER_LENGTH];
        readFully(in, header, MBAP_HEADER_LENGTH);

        int txId = ((header[0] & 0xFF) << 8) | (header[1] & 0xFF);
        if (txId != expectedTxId) {
            throw new IOException(String.format(
                    "Modbus 响应 Transaction ID 不匹配: 期望 %d, 实际 %d", expectedTxId, txId));
        }

        int length = ((header[4] & 0xFF) << 8) | (header[5] & 0xFF);
        // length 包含 unitId(1) + 后续 PDU；已读掉 unitId，故 PDU 剩余 = length - 1
        int pduLength = length - 1;
        if (pduLength < 2) {
            throw new IOException("Modbus 响应长度字段非法: " + length);
        }

        // 2. 读满 PDU 剩余字节
        byte[] pdu = new byte[pduLength];
        readFully(in, pdu, pduLength);

        int functionCode = pdu[0] & 0xFF;
        // 3. 异常响应判断
        if ((functionCode & EXCEPTION_FLAG) != 0) {
            int exceptionCode = pdu[1] & 0xFF;
            throw new IOException("Modbus 异常响应: 功能码=0x" +
                    Integer.toHexString(functionCode) + ", 异常码=" + exceptionCode);
        }

        // 4. 正常响应：pdu[1] = byteCount, 后续为寄存器数据
        int byteCount = pdu[1] & 0xFF;
        int expectedByteCount = expectedQuantity * 2;
        if (byteCount != expectedByteCount) {
            throw new IOException(String.format(
                    "Modbus 响应字节数不符: 期望 %d, 实际 %d", expectedByteCount, byteCount));
        }
        if (pdu.length < 2 + byteCount) {
            throw new IOException(String.format(
                    "Modbus 响应数据不足: 期望 %d 字节, 实际 %d", byteCount, pdu.length - 2));
        }

        short[] registers = new short[expectedQuantity];
        for (int i = 0; i < expectedQuantity; i++) {
            int hi = pdu[2 + i * 2] & 0xFF;
            int lo = pdu[2 + i * 2 + 1] & 0xFF;
            registers[i] = (short) ((hi << 8) | lo);
        }
        return registers;
    }

    /**
     * 从流中读满 len 字节，不足则抛异常
     */
    private static void readFully(InputStream in, byte[] buffer, int len) throws IOException {
        int offset = 0;
        while (offset < len) {
            int read = in.read(buffer, offset, len - offset);
            if (read < 0) {
                throw new IOException(String.format(
                        "Modbus 响应流提前结束: 期望 %d 字节, 实际读取 %d", len, offset));
            }
            offset += read;
        }
    }

    @Override
    public void close() {
        if (socket != null) {
            try {
                socket.close();
                log.info("Modbus TCP 连接已释放");
            } catch (IOException e) {
                log.warn("关闭 Modbus TCP 连接异常", e);
            } finally {
                socket = null;
                in = null;
                out = null;
            }
        }
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `mvn test -pl flink-etl-connector/connector-modbus -Dtest=ModbusTcpClientTest`
Expected: 5 个测试全部 PASS

- [ ] **Step 5: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusTcpClient.java flink-etl-connector/connector-modbus/src/test/java/com/etl/connector/modbus/source/ModbusTcpClientTest.java
git commit -m "feat(modbus): 新增原生 Socket 实现的 ModbusTcpClient"
```

---

## Task 2: ModbusSourceConfig 新增 timeoutMs 配置项

**Files:**
- Modify: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java`

- [ ] **Step 1: 新增 timeoutMs 字段**

在字段区（`intervalMs` 之后）添加：

```java
    private final long intervalMs;
    private final long timeoutMs;
```

- [ ] **Step 2: 在 fromSourceConfig 中读取并校验 timeoutMs**

在「6. 校验 intervalMs」代码块之后、`log.info` 之前插入：

```java
        // 7. 校验 timeoutMs（连接与读取共用，默认 3000ms）
        long timeoutMs = config.get("timeoutMs", Long.class, 3000L);
        Preconditions.checkArgument(timeoutMs > 0,
                "配置项 'timeoutMs' 必须 > 0，当前值: %s", timeoutMs);
```

- [ ] **Step 3: 更新 log.info 与 builder**

将 `log.info(...)` 一行替换为（追加 timeoutMs）：

```java
        log.info("创建 ModbusSource: bounded={}, host={}:{}, deviceId={}, address={}, count={}, wordSize={}, intervalMs={}, timeoutMs={}",
                bounded, ip, port, deviceId, address, count, wordSize, intervalMs, timeoutMs);
```

将 builder 链中 `.intervalMs(intervalMs)` 之后追加：

```java
                .intervalMs(intervalMs)
                .timeoutMs(timeoutMs)
```

- [ ] **Step 4: 编译确认通过**

Run: `mvn compile -pl flink-etl-connector/connector-modbus`
Expected: BUILD SUCCESS

- [ ] **Step 5: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java
git commit -m "feat(modbus): ModbusSourceConfig 新增 timeoutMs 配置项"
```

---

## Task 3: ModbusSplitReader 切换到 ModbusTcpClient

**Files:**
- Modify: `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java`

- [ ] **Step 1: 替换 import**

删除 modbus4j 相关 import（第 5-10 行）：

```java
import com.serotonin.modbus4j.ModbusFactory;
import com.serotonin.modbus4j.ModbusMaster;
import com.serotonin.modbus4j.ip.IpParameters;
import com.serotonin.modbus4j.msg.ModbusResponse;
import com.serotonin.modbus4j.msg.ReadHoldingRegistersRequest;
import com.serotonin.modbus4j.msg.ReadHoldingRegistersResponse;
```

（无需新增 import，`ModbusTcpClient` 与本类同包）

- [ ] **Step 2: 替换 master 字段**

将：

```java
    private ModbusMaster master;
```

替换为：

```java
    private ModbusTcpClient client;
```

- [ ] **Step 3: 替换 initMaster 方法**

将整个 `initMaster()` 方法（含 javadoc）替换为：

```java
    /**
     * 初始化 Modbus TCP 连接
     */
    private void initClient() throws IOException {
        client = new ModbusTcpClient(
                currentConfig.getIp(),
                currentConfig.getPort(),
                currentConfig.getDeviceId(),
                currentConfig.getTimeoutMs());
        client.connect();
    }
```

- [ ] **Step 4: 更新 fetch() 中的调用**

将 `fetch()` 中的 `initMaster();` 改为 `initClient();`

- [ ] **Step 5: 替换 readRegisters 中的读取逻辑**

将 `readRegisters()` 中 while 循环内的请求/响应代码块：

```java
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
```

替换为：

```java
                short[] data = client.readHoldingRegisters(currentAddress, batchSize);
```

（数据不足与异常响应校验已在 `ModbusTcpClient.parseReadResponse` 内完成）

- [ ] **Step 6: 替换 destroyMaster 方法**

将整个 `destroyMaster()` 方法替换为：

```java
    private void destroyClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }
```

- [ ] **Step 7: 更新 destroyMaster 的两处调用**

将 `readRegisters()` 批处理分支中的 `destroyMaster();` 与 `close()` 中的 `destroyMaster();` 均改为 `destroyClient();`

- [ ] **Step 8: 编译确认通过**

Run: `mvn compile -pl flink-etl-connector/connector-modbus`
Expected: BUILD SUCCESS，无 modbus4j 引用残留

- [ ] **Step 9: 运行全部 modbus 测试确认通过**

Run: `mvn test -pl flink-etl-connector/connector-modbus`
Expected: `ModbusTcpClientTest` 与 `ModbusSplitReaderTest` 全部 PASS

- [ ] **Step 10: 提交**

```bash
git add flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java
git commit -m "refactor(modbus): ModbusSplitReader 改用 ModbusTcpClient"
```

---

## Task 4: 移除 connector-modbus 模块的 modbus4j 依赖

**Files:**
- Modify: `flink-etl-connector/connector-modbus/pom.xml`

- [ ] **Step 1: 删除 modbus4j 依赖**

将 `<dependencies>` 块中的 modbus4j 依赖删除：

```xml
        <dependency>
            <groupId>com.infiniteautomation</groupId>
            <artifactId>modbus4j</artifactId>
        </dependency>
```

删除后 `<dependencies>` 变为空块，保留空的 `<dependencies></dependencies>` 或整体删除该块均可。建议删除整个空 `<dependencies>` 元素，使 pom 仅保留 parent 与坐标定义。

- [ ] **Step 2: 编译确认通过**

Run: `mvn compile -pl flink-etl-connector/connector-modbus`
Expected: BUILD SUCCESS（不再依赖 modbus4j）

- [ ] **Step 3: 提交**

```bash
git add flink-etl-connector/connector-modbus/pom.xml
git commit -m "build(modbus): 移除 connector-modbus 的 modbus4j 依赖"
```

---

## Task 5: 清理根 pom.xml 的 modbus4j 相关配置

去除 modbus4j 版本属性、dependencyManagement、专用仓库、以及为其添加的 commons-logging 桥接。

**Files:**
- Modify: `pom.xml`（根）

- [ ] **Step 1: 删除版本属性**

删除 `<properties>` 中的：

```xml
        <modbus4j.version>3.1.0</modbus4j.version>
```

- [ ] **Step 2: 删除 dependencyManagement 中的 modbus4j**

删除：

```xml
            <!-- Modbus 通信 -->
            <dependency>
                <groupId>com.infiniteautomation</groupId>
                <artifactId>modbus4j</artifactId>
                <version>${modbus4j.version}</version>
            </dependency>
```

- [ ] **Step 3: 删除 ias-releases 专用仓库**

删除整个 `<repositories>` 块中的 ias-releases 仓库（含其上方关于 modbus4j 的注释）：

```xml
    <repositories>
        <!--
            Modbus4j 仓库
            settings.xml 中的 mirrorOf 不能是 *
            可以是 central 或者 *,!ias-releases
            不然无法下载 modbus4j
        -->
        <repository>
            <id>ias-releases</id>
            <name>Infinite Automation Release Repository</name>
            <url>https://maven.mangoautomation.net/repository/ias-release/</url>
        </repository>
    </repositories>
```

注：若 `<repositories>` 中仅此一个仓库，连同 `<repositories></repositories>` 一并删除。

- [ ] **Step 4: 删除 commons-logging → Log4j2 桥接**

删除（该桥接注释明确说明仅为 modbus4j 服务）：

```xml
        <!-- Commons Logging → Log4j2 桥接，让 modbus4j 的日志走 Log4j2 -->
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-jcl</artifactId>
            <version>${log4j2.version}</version>
        </dependency>
```

- [ ] **Step 5: 全量编译确认通过**

Run: `mvn clean compile`
Expected: BUILD SUCCESS，所有模块编译通过，无 modbus4j 解析

- [ ] **Step 6: 全量测试确认通过**

Run: `mvn test`
Expected: 所有测试 PASS

- [ ] **Step 7: 提交**

```bash
git add pom.xml
git commit -m "build: 移除根 pom 中 modbus4j 版本管理、专用仓库与日志桥接"
```

---

## Task 6: 更新 PLUGINS.md 文档

**Files:**
- Modify: `PLUGINS.md`

- [ ] **Step 1: 定位 Modbus Source 章节**

Run: `grep -n -i modbus PLUGINS.md`
Expected: 找到 Modbus Source 的配置参数表与说明位置

- [ ] **Step 2: 更新说明与配置参数表**

在 Modbus Source 配置参数表中新增一行 `timeoutMs`：

```
| timeoutMs | 否 | 3000 | 连接与读取超时（毫秒），超时抛出异常由 Flink 重试 |
```

并在该连接器的实现说明中，将"使用 modbus4j 库"相关描述改为"使用 Java 原生 Socket 实现 Modbus TCP 协议（功能码 0x03）"。

（具体行号以 Step 1 输出为准；保持表格列与现有格式一致）

- [ ] **Step 3: 提交**

```bash
git add PLUGINS.md
git commit -m "docs: 更新 Modbus Source 文档（原生 Socket 实现与 timeoutMs）"
```

---

## 验证清单（全部完成后）

- [ ] `mvn clean test` 全绿
- [ ] 全仓库搜索无 `modbus4j` / `com.serotonin` 残留（文档目录除外）：`grep -rn "modbus4j\|com.serotonin" --include=*.java --include=*.xml`
- [ ] `ModbusSource`、`ModbusSplit`、`ModbusSplitEnumerator` 未改动
- [ ] PLUGINS.md 已同步
