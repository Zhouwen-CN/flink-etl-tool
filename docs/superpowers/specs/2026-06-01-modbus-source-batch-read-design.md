# Modbus Source 分批读取设计

## 背景

Modbus Source 当前使用单次 `ReadHoldingRegistersRequest` 读取所有寄存器，但部分 Modbus 设备存在单次请求寄存器数量的硬件限制（如最多 32 个）。需要改为分批循环读取，每批最多 32 个寄存器。

## 设计方案

### 改动范围

仅修改 `ModbusSplitReader.java`，将 `readRegisters()` 方法从单次批量读取改为分批循环读取。其他文件不受影响。

### 核心变更

在 `ModbusSplitReader` 中定义固定常量：

```java
private static final int MAX_READ_SIZE = 32;
```

`readRegisters()` 方法改为循环逻辑：

```java
int remaining = count;
int currentAddress = address;
while (remaining > 0) {
    int batchSize = Math.min(remaining, MAX_READ_SIZE);
    ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(deviceId, currentAddress, batchSize);
    ModbusResponse response = master.send(request);
    short[] data = ((ReadHoldingRegistersResponse) response).getShortData();

    // 处理当前批次数据（wordSize 逻辑不变）
    // wordSize=1: 每个寄存器生成一行 Row(address, value)
    // wordSize=2: 每两个寄存器组合为一个 32 位值

    currentAddress += batchSize;
    remaining -= batchSize;
}
```

### 行为说明

| 模式 | 行为 |
|------|------|
| Batch | 循环读完所有分批 → 标记 split 完成 → 关闭连接 |
| Streaming | 每次 poll 周期内循环读完所有分批 → sleep intervalMs → 下次 poll 再全部读一遍 |

### 不变的部分

- `ModbusSourceConfig`：无新增配置参数，32 为固定常量
- `ModbusSplitEnumerator` / `ModbusSplit` / `ModbusSource`：无变化
- 输出 schema：`Row(address: INT, value: INT)` 不变
- 输出数据：与改动前完全一致，仅读取方式从一次请求变为多次小请求

## 测试策略

修改 `ModbusSplitReaderTest`（如存在），验证分批读取逻辑的正确性：
- count < 32：单批读取
- count = 32：单批读取
- count = 33：两批读取（32 + 1）
- count = 64：两批读取（32 + 32）
- count = 100：四批读取（32 + 32 + 32 + 4）

## 影响分析

- 无配置变更，向后兼容
- 无 schema 变更，下游无感知
- 分批读取会增加 TCP 循环次数，但每次请求更小更快，总耗时基本不变
