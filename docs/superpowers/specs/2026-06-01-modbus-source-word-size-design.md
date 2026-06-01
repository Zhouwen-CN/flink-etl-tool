# Modbus Source 增加 wordSize 配置

## 背景

Modbus 协议中每个寄存器为 16bit。实际工业设备中，常将相邻两个寄存器组合表示一个 32bit 数据。当前 `ModbusSource` 只支持每个寄存器输出一行（16bit），需新增配置项支持 2 寄存器合并为 32bit 值。

## 配置项

新增配置项：`wordSize`

- 类型：`int`
- 可选值：`1` 或 `2`
- 默认值：`1`
- 含义：几个 Modbus 寄存器组成一个数据值
  - `1`：每个寄存器（16bit）作为一行（保持现有行为）
  - `2`：每 2 个寄存器（32bit）合并为一行，高位寄存器在前（大端）

## 校验规则

在 `ModbusSourceConfig.fromSourceConfig` 中新增：

1. `wordSize` 仅允许 `1` 或 `2`，默认为 `1`
2. `count % wordSize == 0`，否则抛 `IllegalArgumentException`

## 读取逻辑

`ModbusSplitReader.readRegisters` 变更：

- 仍一次性请求 `count` 个寄存器
- 按 `wordSize` 分组生成 Row：
  - `wordSize=1`：保持现有行为，`row(address = 40001 + address + i, value = (int) short[i])`
  - `wordSize=2`：每两个寄存器合并为一个 32bit 有符号整数
    - `address = 40001 + address + i`（取该组首个寄存器地址）
    - `value = (short[i] << 16) | (short[i+1] & 0xFFFF)`
- 输出行数 = `count / wordSize`

Row schema 不变，仍为 `(address: int, value: int)`。

## 测试

更新 `ModbusSourceConfigTest`：

- 默认 `wordSize=1`
- `wordSize=2` 且 count 为偶数 → 通过
- `wordSize=2` 且 count 为奇数 → 抛异常
- `wordSize=3` → 抛异常
- `wordSize=0` → 抛异常

## 文档

- 更新 `PLUGINS.md` 中 modbus source 的配置说明
- 如有 modbus 示例配置/文档，补充 `wordSize` 说明

## 影响范围

- `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/config/ModbusSourceConfig.java`
- `flink-etl-connector/connector-modbus/src/main/java/com/etl/connector/modbus/source/ModbusSplitReader.java`
- `flink-etl-connector/connector-modbus/src/test/java/com/etl/connector/modbus/source/config/ModbusSourceConfigTest.java`
- `PLUGINS.md`
