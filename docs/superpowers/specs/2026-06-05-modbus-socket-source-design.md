# Modbus Source 改用原生 Socket 实现设计

**日期**：2026-06-05
**模块**：`flink-etl-connector/connector-modbus`

## 背景与目标

当前 Modbus Source 通过 `modbus4j` 库实现 Modbus TCP 通信。该库的开源协议与下载渠道存在不确定性，因此改为直接使用 Java 原生 `Socket` 实现 Modbus TCP 协议，去除对 `modbus4j` 的依赖。

Modbus TCP 协议结构简单（MBAP 头 + PDU），且当前仅使用 **Read Holding Registers（功能码 0x03）** 一个功能，自行实现的工作量小、可控性高。

## 范围

- **仅实现功能码 0x03（Read Holding Registers）**，完全复刻现有功能，不扩展其他功能码。
- 输出格式不变：`Row(address, value)`。
- 分批读取（`MAX_READ_SIZE=32`）、大端 32bit 组合（`wordSize=2`）逻辑不变。
- 批处理 / 流处理两种模式行为不变。

## 架构

去除 `modbus4j` 依赖，新增纯 Java 的 `ModbusTcpClient` 类承载协议逻辑，`ModbusSplitReader` 改为持有它。协议字节处理与 Flink 分片读取逻辑分离，`ModbusTcpClient` 可独立单元测试。

```
connector-modbus/
├── pom.xml                              # 移除 modbus4j 依赖
└── source/
    ├── ModbusTcpClient.java             # 【新增】socket + MBAP 编解码 + 0x03
    ├── ModbusSplitReader.java           # master 字段 → ModbusTcpClient
    └── config/ModbusSourceConfig.java   # 新增 timeoutMs 配置项
```

`ModbusSource`、`ModbusSplit`、`ModbusSplitEnumerator` 不改动（不涉及 modbus4j）。

## ModbusTcpClient

### 接口

```java
public class ModbusTcpClient implements Closeable {
    ModbusTcpClient(String ip, int port, int deviceId, int timeoutMs)
    void connect() throws IOException                                          // 建 socket，设连接 + 读超时
    short[] readHoldingRegisters(int address, int quantity) throws IOException // 功能码 0x03
    void close()                                                              // 关 socket
}
```

### 内部状态

- 一个 `Socket`，`connect(SocketAddress, timeout)` 设置连接超时，`setSoTimeout(timeout)` 设置读取超时。连接与读取共用 `timeoutMs`。
- `transactionId` 字段每次请求自增，从 1 开始，回绕范围 0–65535。

### 协议帧格式（0x03）

**请求帧（MBAP 7 字节 + PDU 5 字节 = 12 字节）：**

| 字节  | 字段             | 值                          |
|-------|------------------|----------------------------|
| 0–1   | Transaction ID   | 自增                        |
| 2–3   | Protocol ID      | 0x0000                      |
| 4–5   | Length           | 0x0006（后续字节数）        |
| 6     | Unit ID          | deviceId                    |
| 7     | Function Code    | 0x03                        |
| 8–9   | Start Address    | 大端                        |
| 10–11 | Quantity         | 大端                        |

**响应帧解析：**

| 字节  | 字段             | 处理                                                       |
|-------|------------------|-----------------------------------------------------------|
| 0–1   | Transaction ID   | 校验与请求一致，不一致抛 `IOException`                     |
| 2–3   | Protocol ID      | —                                                         |
| 4–5   | Length           | 据此读满整帧 PDU（防 TCP 拆包）                            |
| 6     | Unit ID          | —                                                         |
| 7     | Function Code    | 若 == 0x83（0x03 \| 0x80）为异常响应，读 [8] 异常码抛 `IOException` |
| 8     | Byte Count       | = quantity * 2                                             |
| 9..   | Register Data    | 每 2 字节（大端）组成一个 short                            |

### 读响应的关键

先读满 MBAP 头（7 字节），从 Length 字段算出剩余 PDU 长度，再循环读满，避免 TCP 拆包导致读不全。使用辅助方法 `readFully(InputStream, byte[], len)`。

### 编解码可测性

协议编解码逻辑拆为包级可见的静态方法（如 `buildReadRequest(...)`、`parseReadResponse(...)`），供单元测试在不开真实 socket 的情况下直接对字节数组验证。

## ModbusSplitReader 改动

- 字段 `ModbusMaster master` → `ModbusTcpClient client`。
- `initMaster()` → `initClient()`：`new ModbusTcpClient(ip, port, deviceId, timeoutMs); client.connect()`。
- `readRegisters()` 中：`master.send(ReadHoldingRegistersRequest)` → `client.readHoldingRegisters(currentAddress, batchSize)`，直接得到 `short[]`。
- `destroyMaster()` → `destroyClient()`：`client.close()`。
- `processRegisterData`、`MAX_READ_SIZE=32` 分批循环、大端 32bit 组合逻辑**完全不变**。

## 配置变化

`ModbusSourceConfig` 新增字段 `timeoutMs`：

- 通过 `config.get("timeoutMs", Long.class, 3000L)` 读取，默认 3000ms。
- 校验 `> 0`。
- 连接超时与读取超时共用此值。

其余配置项（host、deviceId、address、count、wordSize、intervalMs）不变。

## 错误处理

以下情况全部抛 `IOException`，由 Flink 从 checkpoint 重试（与现有 at-least-once 语义一致）：

- 连接失败 / 读取超时
- 异常响应（功能码 0x83）
- Transaction ID 不匹配
- 响应数据不足

## 测试

- **`ModbusTcpClientTest`（新增）**：对协议编解码做单元测试。
  - 构造请求字节，验证 MBAP + PDU 格式正确。
  - 喂入构造好的响应字节数组，验证解析：正常响应、异常响应（0x83）、Transaction ID 不匹配、字节数不足。
  - 使用 `ByteArrayInputStream` 等内存流测试，不依赖真实 socket。
- **`ModbusSplitReaderTest`（保留不变）**：`processRegisterData` 反射测试不依赖 modbus4j，无需改动。

## 文档维护

完成后同步更新 [PLUGINS.md](../../../PLUGINS.md) 中 Modbus Source 相关说明（如有依赖或行为描述）。
