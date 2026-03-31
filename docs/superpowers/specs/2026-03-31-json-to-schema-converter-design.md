# JSON to Schema Converter 设计文档

## 概述

创建一个单文件 HTML 工具，用于从 JSON 数据样本自动推断并生成 Flink ETL Tool 的 schema 配置。工具纯前端实现，无需外部依赖，完全离线可用。

## 背景

项目中 schema 配置采用特定的 JSON 格式定义字段名和类型（如 `"id": "LONG"`、`"tags": ["STRING"]`）。手动编写 schema 配置容易出错且耗时，需要一个工具从实际 JSON 数据中自动推断 schema。

## 核心需求

### 输入
- 用户粘贴 JSON 数据样本（如数据库查询结果、API 响应）
- JSON 格式必须正确（支持错误提示）

### Schema 推断规则

**简单类型推断：**
- `string` → 检查是否日期格式：
  - 匹配 `YYYY-MM-DD HH:mm:ss` → `TIMESTAMP`
  - 否则 → `STRING`
- `number` → 检查值类型和范围：
  - 浮点数 → `DOUBLE`
  - 整数 <= 2147483647 → `INT`
  - 整数 > 2147483647 → `LONG`
- `boolean` → `BOOLEAN`
- `null` → `STRING`（默认最通用类型，并在结果中高亮显示）

**复杂类型推断：**
- `array` → 检查元素类型：
  - 简单类型数组 → `["TYPE"]`（如 `["STRING"]`）
  - 对象数组 → `[{}]`，数组第一个元素定义对象结构
- `object` → 递归推断子字段 → `{}` 结构

### 输出功能
- 展示生成的 schema JSON（格式化展示）
- **高亮显示 null 值转换的字段**：
  - 使用不同颜色或样式标记从 null 推断的字段（如黄色背景或加粗）
  - 提示用户这些字段需要手动确认类型
- 提供文本编辑框，用户可手动调整字段类型
- "复制到剪贴板"按钮，一键复制结果

## 架构设计

### 技术选型

**方案：纯前端单文件 HTML**
- 所有逻辑（HTML、CSS、JavaScript）在单个文件中实现
- 不依赖任何外部库或 CDN
- 离线可用，无需服务器

**理由：**
1. 工具功能简单，不需要复杂外部库
2. 单文件便于分发和使用
3. 离线可用，不依赖网络
4. 对于内部工具，简洁实用优先

### 文件结构

```
docs/
└── json-to-schema.html  # 单文件 HTML 工具
```

### 核心模块

**1. JSON 输入与解析**
- 文本输入框接收 JSON 数据
- `JSON.parse()` 解析并验证格式
- 显示错误提示（JSON 格式不正确）

**2. Schema 推断算法**
- 递归遍历 JSON 对象
- 根据规则推断每个字段类型
- 支持嵌套对象和数组

**3. UI 交互**
- "转换"按钮触发推断
- "复制"按钮复制结果
- "清空"按钮清空输入输出

### UI 设计

**布局：**
- 左侧：JSON 输入区（40% 宽度，大文本框）
- 右侧：Schema 输出区（40% 宽度，大文本框）
- 中间：控制按钮区（20% 宽度）

**样式：**
- 浅色主题，清晰易读
- 使用内联 CSS
- 错误提示使用红色文本
- **null 值字段高亮**：
  - 在 schema 输出中，从 null 推断的字段使用特殊样式（如黄色背景或加粗）
  - 例如：`"name": "STRING"` 中 STRING 部分高亮显示（因为原始值是 null）
  - 提示用户这些字段类型需要手动确认

## 实现细节

### 日期识别

**正则表达式：**
```javascript
const datetimeRegex = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;
```

**匹配规则：**
- 格式：`YYYY-MM-DD HH:mm:ss`
- 示例：`2024-01-01 10:30:00`
- 匹配成功 → `TIMESTAMP`
- 匹配失败 → `STRING`

### 数字类型判断

**逻辑流程：**
```javascript
function inferNumberType(value) {
  if (!Number.isInteger(value)) {
    return 'DOUBLE';
  }
  if (value >= -2147483648 && value <= 2147483647) {
    return 'INT';
  }
  return 'LONG';
}
```

**边界值：**
- INT 范围：`-2147483648` 到 `2147483647`（32 位整数范围）
- 超出范围 → `LONG`

### null 值处理

**推断规则：**
- 所有 `null` 值默认推断为 `STRING` 类型
- 记录这些字段的位置，用于高亮显示

**高亮显示实现：**
- 在 schema 输出时，使用 HTML 标记包裹 null 值转换的类型
- 例如：`"name": <span class="null-highlight">STRING</span>`
- CSS 样式：黄色背景或加粗文本
- 用户在编辑框中可以看到高亮标记，方便识别需要确认的字段

### 数组类型推断

**逻辑流程：**
1. 检查数组长度（空数组 → 无法推断，返回 `["STRING"]`）
2. 检查第一个元素类型：
   - 简单类型 → `["TYPE"]`（如 `["STRING"]`）
   - 对象 → `[{}]`，推断对象结构并放入数组第一个元素
3. 假设数组元素类型一致（不处理混合类型数组）

### Schema 输出格式

**输出示例：**
```json
{
  "id": "LONG",
  "name": "STRING",
  "age": "INT",
  "created_at": "TIMESTAMP",
  "tags": ["STRING"],
  "address": {
    "city": "STRING",
    "zipcodes": ["INT"]
  },
  "friends": [
    {
      "name": "STRING",
      "age": "INT"
    }
  ]
}
```

## 用户体验

### 使用流程

1. 打开 `docs/json-to-schema.html` 文件（浏览器中打开）
2. 在左侧输入框粘贴 JSON 数据
3. 点击"转换"按钮
4. 在右侧查看生成的 schema
5. 可手动编辑 schema（调整字段类型）
6. 点击"复制"按钮，复制结果到剪贴板
7. 粘贴到配置文件中使用

### 错误处理

- JSON 格式错误 → 显示红色错误提示
- 空 JSON → 显示提示信息
- 复制失败 → 显示错误提示（浏览器不支持 clipboard API）

## 测试计划

### 功能测试

**简单类型测试：**
- 输入：`{"id": 123, "name": "John", "age": 30, "created": "2024-01-01 10:30:00"}`
- 预期输出：`{"id": "INT", "name": "STRING", "age": "INT", "created": "TIMESTAMP"}`

**数字边界测试：**
- 输入：`{"small": 100, "large": 3000000000, "float": 3.14}`
- 预期输出：`{"small": "INT", "large": "LONG", "float": "DOUBLE"}`

**数组类型测试：**
- 输入：`{"tags": ["a", "b"], "scores": [1, 2]}`
- 预期输出：`{"tags": ["STRING"], "scores": ["INT"]}`

**对象嵌套测试：**
- 输入：`{"address": {"city": "Beijing", "zip": 100000}}`
- 预期输出：`{"address": {"city": "STRING", "zip": "INT"}}`

**对象数组测试：**
- 输入：`{"friends": [{"name": "Tom", "age": 25}]}`
- 预期输出：`{"friends": [{"name": "STRING", "age": "INT"}]}`

**null 值测试：**
- 输入：`{"id": 1, "name": null, "age": null}`
- 预期输出：`{"id": "INT", "name": "STRING", "age": "STRING"}`
- 预期行为：`name` 和 `age` 字段的 STRING 类型被高亮显示

### 错误测试

- 输入：无效 JSON（如 `{invalid}`）
- 预期：显示错误提示

- 输入：空字符串
- 预期：显示提示信息

## 后续优化

可选优化方向（不在当前实现范围）：

1. **支持更多日期格式**：ISO 8601、Unix 时间戳等
2. **智能类型合并**：处理混合类型数组（如 `[1, "text"]`）
3. **批量处理**：支持多个 JSON 样本，合并生成统一 schema
4. **配置选项**：允许用户自定义推断规则（如禁用日期识别）
5. **导出功能**：下载为 .json 文件

## 文件位置

工具文件：`docs/json-to-schema.html`