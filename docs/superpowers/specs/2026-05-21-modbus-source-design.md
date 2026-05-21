# Modbus Source 连接器设计文档

## 概述

新增 Modbus TCP Source 连接器，通过 Modbus TCP 协议读取 Holding Registers（功能码 03），支持批处理和流处理模式。使用 modbus4j
库实现通信。

## 配置参数

| 参数             | 必填 | 类型      | 默认值  | 说明                                             |
|----------------|----|---------|------|------------------------------------------------|
| `host`         | 是  | String  | -    | Modbus TCP 地址，格式 `ip:port`，配置类中拆分为 ip 和 port   |
| `slaveId`      | 否  | Integer | 1    | 从站地址（Device ID），范围 1-247                       |
| `startAddress` | 是  | Integer | -    | 寄存器起始地址（0-based），>= 0                          |
| `quantity`     | 是  | Integer | -    | 读取寄存器数量，> 0 且 startAddress + quantity <= 65536 |
| `intervalMs`   | 否  | Long    | 1000 | 流处理模式下的轮询间隔（毫秒），> 0                            |

### 参数校验（在 `ModbusSourceConfig.fromSourceConfig()` 中完成）

- `host` 必填，格式必须为 `ip:port`，port 为有效端口号（1-65535）
- `slaveId` 范围 1-247
- `startAddress` >= 0
- `quantity` > 0，且 `startAddress + quantity` <= 65536
- `intervalMs` > 0

## 固定输出 Schema

不需要用户配置 schema。输出固定为两列：

| 列名        | 类型  | 说明                                 |
|-----------|-----|------------------------------------|
| `address` | INT | 设备手册地址，从 `40001 + startAddress` 开始 |
| `value`   | INT | 寄存器值，有符号（Java short → int）         |

每次读取产生 `quantity` 行数据，每行对应一个寄存器。

示例：`startAddress=0, quantity=5` 产生 5 行：

```
address=40001, value=123
address=40002, value=-456
address=40003, value=0
address=40004, value=32767
address=40005, value=-1
```

## 运行模式

### 批处理模式（batch）

读取一次所有寄存器后标记分片完成，程序退出。

### 流处理模式（streaming）

每次读取所有寄存器，sleep `intervalMs` 毫秒后再次读取，持续循环。

## 模块结构

```
flink-etl-connector/connector-modbus/
├── pom.xml
└── src/
    ├── main/java/com/etl/connector/modbus/source/
    │   ├── ModbusSourcePlugin.java       # SPI 入口，identifier="modbus"
    │   ├── ModbusSource.java             # 主类，继承 AbstractSplitSource
    │   ├── ModbusSplitEnumerator.java    # 分片枚举器，创建单分片
    │   ├── ModbusSourceReader.java       # 阅读器，包装 SplitReader
    │   ├── ModbusSplitReader.java        # 核心读取逻辑，连接 Modbus TCP 读寄存器
    │   ├── ModbusSplit.java              # 单分片，持有 ModbusSourceConfig
    │   ├── ModbusSplitState.java         # 分片状态
    │   ├── ModbusEnumCheckpoint.java     # 枚举器检查点
    │   ├── ModbusRecordEmitter.java      # 记录发射器
    │   └── config/
    │       └── ModbusSourceConfig.java   # 配置类，参数校验 + host 拆分
    └── test/java/com/etl/connector/modbus/source/config/
        └── ModbusSourceConfigTest.java   # 配置类单元测试
```

## 类职责

### ModbusSourcePlugin

- SPI 入口，`@AutoService(SourcePlugin.class)`
- `identifier()` 返回 `"modbus"`
- `createSource()` 创建 `ModbusSource`

### ModbusSourceConfig

- 静态方法 `fromSourceConfig(SourceConfig config, RuntimeExecutionMode runtimeMode)` 完成所有参数校验
- 拆分 `host` 为 `ip` 和 `port`
- 字段：`bounded`, `ip`, `port`, `slaveId`, `startAddress`, `quantity`, `intervalMs`
- 实现 `Serializable`（需要通过 Split 序列化传输）

### ModbusSource

- 继承 `AbstractSplitSource<ModbusSplit, ModbusEnumCheckpoint>`
- 构造函数调用 `ModbusSourceConfig.fromSourceConfig()` 完成校验
- `getBoundedness()` 根据运行模式返回 BOUNDED 或 CONTINUOUS_UNBOUNDED
- 重写 `getProducedType()` 返回固定的 `RowTypeInfo(INT, INT)`，字段名为 `["address", "value"]`

### ModbusSplit

- 实现 `BaseSourceSplit`
- 持有 `ModbusSourceConfig`
- splitId 固定为 `"modbus-split-0"`

### ModbusSplitEnumerator

- 继承 `BaseSplitEnumerator<ModbusSplit, ModbusEnumCheckpoint>`
- `start()` 创建单个 `ModbusSplit` 加入 `pendingSplits`

### ModbusSplitReader

- 实现 `BaseSplitReader<Row, ModbusSplit>`
- 首次 `fetch()` 时从 Split 获取配置并创建 `ModbusMaster` 连接（modbus4j TCP）
- 调用 `master.setValue(ReadHoldingRegistersRequest)` 读取寄存器
- 每个寄存器生成 `Row(address=40001+startAddress+i, value=寄存器值)`
- 批处理：读完标记分片完成
- 流处理：读完 sleep `intervalMs` 后循环
- `close()` 释放 `ModbusMaster` 连接

### ModbusSourceReader / ModbusSplitState / ModbusEnumCheckpoint / ModbusRecordEmitter

与 Mock Source 对应类结构完全一致，属于样板代码。

## 数据流

```
1. ModbusSourcePlugin.createSource(config, runtimeMode)
   → new ModbusSource(config, runtimeMode)
     → ModbusSourceConfig.fromSourceConfig(config, runtimeMode)
       → 校验参数、拆分 host 为 ip + port

2. Flink 启动：
   ModbusSplitEnumerator.start()
     → new ModbusSplit(config) → pendingSplits.add(split)
     → BaseSplitEnumerator 自动分配给 Reader

3. 数据读取：
   ModbusSplitReader.fetch()
     → 首次调用：创建 ModbusMaster TCP 连接
     → master.send(ReadHoldingRegistersRequest(slaveId, startAddress, quantity))
     → 解析响应，每个寄存器生成 Row(address, value)
     → 批处理：标记 finishedSplit
     → 流处理：Thread.sleep(intervalMs)，下次 fetch 继续读取
```

## 依赖

```xml
<!-- connector-modbus/pom.xml -->
<dependency>
    <groupId>com.infiniteautomation</groupId>
    <artifactId>modbus4j</artifactId>
    <version>3.1.0</version>
</dependency>
```

在父 pom 的 `<dependencyManagement>` 中统一管理 modbus4j 版本。

## 配置示例

### 批处理

```json
{
  "job": { "name": "batch-modbus2console", "mode": "batch", "parallelism": 1 },
  "sources": [{
    "type": "modbus",
    "outputTable": "modbus_data",
    "config": {
      "host": "192.168.1.100:502",
      "slaveId": 1,
      "startAddress": 0,
      "quantity": 10
    }
  }],
  "sinks": [{ "type": "console", "inputTable": "modbus_data" }]
}
```

### 流处理

```json
{
  "job": { "name": "stream-modbus2console", "mode": "streaming", "parallelism": 1 },
  "sources": [{
    "type": "modbus",
    "outputTable": "modbus_data",
    "config": {
      "host": "192.168.1.100:502",
      "slaveId": 1,
      "startAddress": 0,
      "quantity": 10,
      "intervalMs": 2000
    }
  }],
  "sinks": [{ "type": "console", "inputTable": "modbus_data" }]
}
```

## 错误处理

- **连接失败**：`fetch()` 中创建连接时抛出 `IOException`，Flink 自动重试
- **读取超时**：modbus4j 内置超时机制（默认 500ms），超时抛出 `IOException`
- **参数错误**：`ModbusSourceConfig.fromSourceConfig()` 阶段立即失败，任务不会启动

## 测试

- `ModbusSourceConfigTest`：测试参数校验逻辑
    - host 格式校验（合法/非法格式）
    - host 拆分为 ip + port
    - slaveId 范围校验
    - startAddress 和 quantity 范围校验
    - 默认值验证（slaveId=1, intervalMs=1000）
- 不做 Modbus TCP 集成测试（需要实际设备）
