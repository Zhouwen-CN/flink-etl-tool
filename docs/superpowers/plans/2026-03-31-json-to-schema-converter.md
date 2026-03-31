# JSON to Schema Converter 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 创建一个单文件 HTML 工具，从 JSON 数据样本自动推断并生成 Flink ETL Tool 的 schema 配置，支持类型推断、null 值高亮、手动编辑和复制功能。

**Architecture:** 纯前端单文件实现，HTML + CSS + JavaScript 全部内联在单个文件中，无外部依赖，完全离线可用。采用模块化 JavaScript 设计，分离 JSON 解析、schema 推断算法和 UI 交互逻辑。

**Tech Stack:** HTML5, CSS3, JavaScript (ES6), 浏览器原生 API

---

## 文件结构

```
docs/
└── json-to-schema.html  # 单文件 HTML 工具（包含所有 HTML、CSS、JavaScript）
```

**职责划分：**
- HTML 结构：提供输入框、输出框、控制按钮的布局
- CSS 样式：内联样式，实现三列布局、高亮显示、错误提示样式
- JavaScript 逻辑：
  - JSON 解析模块：验证 JSON 格式，显示错误提示
  - Schema 推断模块：递归推断类型，处理 null 值，记录高亮字段
  - UI 交互模块：按钮事件处理、复制功能、显示结果

---

## 实现任务

### Task 1: 创建 HTML 文件框架

**Files:**
- Create: `docs/json-to-schema.html`

- [ ] **Step 1: 创建 HTML 文件并编写基础结构**

创建文件并添加 HTML5 基础结构、布局框架：

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>JSON to Schema Converter</title>
    <style>
        /* CSS 样式将在后续步骤中填充 */
    </style>
</head>
<body>
    <div class="container">
        <h1>JSON to Schema 转换工具</h1>
        <div class="main-content">
            <div class="input-section">
                <h2>JSON 输入</h2>
                <textarea id="jsonInput" placeholder="粘贴 JSON 数据..."></textarea>
            </div>
            <div class="button-section">
                <button id="convertBtn">转换</button>
                <button id="copyBtn">复制</button>
                <button id="clearBtn">清空</button>
            </div>
            <div class="output-section">
                <h2>Schema 输出</h2>
                <textarea id="schemaOutput" placeholder="生成的 schema..."></textarea>
            </div>
        </div>
        <div id="errorMessage" class="error-message"></div>
    </div>

    <script>
        // JavaScript 逻辑将在后续步骤中填充
    </script>
</body>
</html>
```

- [ ] **Step 2: 验证文件创建成功**

运行: `ls -la docs/json-to-schema.html`
预期输出: 文件存在且大小 > 0

- [ ] **Step 3: 提交 HTML 框架**

```bash
git add docs/json-to-schema.html
git commit -m "feat: 新增 JSON to Schema 转换工具 HTML 框架"
```

---

### Task 2: 添加 CSS 样式

**Files:**
- Modify: `docs/json-to-schema.html` (在 `<style>` 标签内)

- [ ] **Step 1: 编写容器和布局样式**

在 `<style>` 标签内添加以下样式：

```css
body {
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 20px;
    background-color: #f5f5f5;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
}

h1 {
    text-align: center;
    color: #333;
}

h2 {
    color: #555;
    margin-bottom: 10px;
}

.main-content {
    display: flex;
    gap: 20px;
    margin-top: 20px;
}

.input-section, .output-section {
    flex: 2;
}

.button-section {
    flex: 1;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    gap: 10px;
}
```

- [ ] **Step 2: 编写输入框和按钮样式**

继续添加以下样式：

```css
textarea {
    width: 100%;
    height: 500px;
    padding: 10px;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 14px;
    font-family: 'Courier New', monospace;
    resize: vertical;
    background-color: white;
}

button {
    width: 80%;
    padding: 12px;
    border: none;
    border-radius: 4px;
    font-size: 16px;
    cursor: pointer;
    transition: background-color 0.3s;
}

#convertBtn {
    background-color: #4CAF50;
    color: white;
}

#convertBtn:hover {
    background-color: #45a049;
}

#copyBtn {
    background-color: #2196F3;
    color: white;
}

#copyBtn:hover {
    background-color: #0b7dda;
}

#clearBtn {
    background-color: #f44336;
    color: white;
}

#clearBtn:hover {
    background-color: #da190b;
}
```

- [ ] **Step 3: 编写错误提示和高亮样式**

继续添加以下样式：

```css
.error-message {
    margin-top: 20px;
    padding: 10px;
    background-color: #ffebee;
    border: 1px solid #f44336;
    border-radius: 4px;
    color: #f44336;
    text-align: center;
    display: none;
}

.null-highlight {
    background-color: #fff9c4;
    padding: 2px 4px;
    border-radius: 2px;
    font-weight: bold;
}
```

- [ ] **Step 4: 在浏览器中打开文件验证样式**

运行: `open docs/json-to-schema.html` (macOS) 或手动在浏览器中打开
预期: 页面显示三列布局，样式清晰美观

- [ ] **Step 5: 提交 CSS 样式**

```bash
git add docs/json-to-schema.html
git commit -m "feat: 添加 JSON to Schema 工具 CSS 样式"
```

---

### Task 3: 实现 JSON 解析功能

**Files:**
- Modify: `docs/json-to-schema.html` (在 `<script>` 标签内)

- [ ] **Step 1: 编写 JSON 解析和错误处理函数**

在 `<script>` 标签内添加以下代码：

```javascript
// JSON 解析模块
function parseJsonInput(jsonString) {
    if (!jsonString || jsonString.trim() === '') {
        return { success: false, error: '请输入 JSON 数据' };
    }

    try {
        const parsed = JSON.parse(jsonString);
        return { success: true, data: parsed };
    } catch (error) {
        return { success: false, error: 'JSON 格式错误: ' + error.message };
    }
}

// 显示错误消息
function showError(message) {
    const errorDiv = document.getElementById('errorMessage');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
}

// 清除错误消息
function clearError() {
    const errorDiv = document.getElementById('errorMessage');
    errorDiv.style.display = 'none';
}
```

- [ ] **Step 2: 在浏览器控制台测试 JSON 解析函数**

打开浏览器控制台，运行:
```javascript
parseJsonInput('{"id": 1, "name": "John"}')
```
预期输出: `{success: true, data: {id: 1, name: "John"}}`

运行:
```javascript
parseJsonInput('invalid json')
```
预期输出: `{success: false, error: 'JSON 格式错误: ...'}`

- [ ] **Step 3: 提交 JSON 解析模块**

```bash
git add docs/json-to-schema.html
git commit -m "feat: 实现 JSON 解析和错误处理功能"
```

---

### Task 4: 实现 Schema 推断算法

**Files:**
- Modify: `docs/json-to-schema.html` (在 `<script>` 标签内，JSON 解析模块后)

- [ ] **Step 1: 编写日期识别函数**

在 JSON 解析模块后添加以下代码：

```javascript
// Schema 推断模块

// 日期格式识别：YYYY-MM-DD HH:mm:ss
function isDateTimeString(value) {
    const datetimeRegex = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;
    return datetimeRegex.test(value);
}
```

- [ ] **Step 2: 编写数字类型推断函数**

继续添加：

```javascript
// 数字类型推断
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

- [ ] **Step 3: 编写递归 schema 推断核心函数**

继续添加核心推断逻辑：

```javascript
// 递归推断 schema，返回 {schema: object, nullFields: Set}
function inferSchema(data) {
    const nullFields = new Set(); // 记录 null 值字段的路径

    function infer(value, path = '') {
        if (value === null) {
            nullFields.add(path);
            return 'STRING'; // null 值默认为 STRING
        }

        if (typeof value === 'string') {
            return isDateTimeString(value) ? 'TIMESTAMP' : 'STRING';
        }

        if (typeof value === 'number') {
            return inferNumberType(value);
        }

        if (typeof value === 'boolean') {
            return 'BOOLEAN';
        }

        if (Array.isArray(value)) {
            if (value.length === 0) {
                return ['STRING']; // 空数组默认为 STRING 数组
            }

            const firstElement = value[0];

            if (typeof firstElement === 'object' && firstElement !== null && !Array.isArray(firstElement)) {
                // 对象数组
                return [infer(firstElement, path + '[0]')];
            } else {
                // 简单类型数组
                return [infer(firstElement, path + '[0]')];
            }
        }

        if (typeof value === 'object') {
            const result = {};
            for (const key in value) {
                result[key] = infer(value[key], path ? path + '.' + key : key);
            }
            return result;
        }

        return 'STRING'; // 默认类型
    }

    const schema = infer(data);
    return { schema, nullFields };
}
```

- [ ] **Step 4: 在浏览器控制台测试推断函数**

打开浏览器控制台，运行:
```javascript
inferSchema({id: 1, name: null, created: "2024-01-01 10:30:00"})
```
预期输出:
```javascript
{
  schema: {id: "INT", name: "STRING", created: "TIMESTAMP"},
  nullFields: Set(1) {"name"}
}
```

运行:
```javascript
inferSchema({large: 3000000000, float: 3.14, tags: ["a", "b"]})
```
预期输出:
```javascript
{
  schema: {large: "LONG", float: "DOUBLE", tags: ["STRING"]},
  nullFields: Set(0) {}
}
```

- [ ] **Step 5: 提交 schema 推断算法**

```bash
git add docs/json-to-schema.html
git commit -m "feat: 实现 schema 推断算法，支持类型推断和 null 值处理"
```

---

### Task 5: 实现高亮显示功能

**Files:**
- Modify: `docs/json-to-schema.html` (在 `<script>` 标签内，schema 推断模块后)

- [ ] **Step 1: 编写 schema 格式化函数（带高亮标记）**

在 schema 推断模块后添加：

```javascript
// 格式化 schema 为带高亮的 JSON 字符串
function formatSchemaWithHighlight(schema, nullFields, parentPath = '') {
    if (typeof schema === 'string') {
        const fullPath = parentPath;
        if (nullFields.has(fullPath)) {
            return '<span class="null-highlight">' + schema + '</span>';
        }
        return schema;
    }

    if (Array.isArray(schema)) {
        const formattedElements = schema.map((item, index) => {
            const itemPath = parentPath + '[' + index + ']';
            return formatSchemaWithHighlight(item, nullFields, itemPath);
        });
        return '[' + formattedElements.join(', ') + ']';
    }

    if (typeof schema === 'object') {
        const entries = [];
        for (const key in schema) {
            const valuePath = parentPath ? parentPath + '.' + key : key;
            const formattedValue = formatSchemaWithHighlight(schema[key], nullFields, valuePath);
            entries.push('  "' + key + '": ' + formattedValue);
        }
        return '{\n' + entries.join(',\n') + '\n}';
    }

    return String(schema);
}
```

- [ ] **Step 2: 编写纯文本 schema 格式化函数（用于复制）**

继续添加：

```javascript
// 格式化 schema 为纯文本 JSON（用于复制）
function formatSchemaPlain(schema) {
    return JSON.stringify(schema, null, 2);
}
```

- [ ] **Step 3: 在浏览器控制台测试高亮格式化**

打开浏览器控制台，运行:
```javascript
const result = inferSchema({id: 1, name: null});
formatSchemaWithHighlight(result.schema, result.nullFields)
```
预期输出: 包含 `<span class="null-highlight">STRING</span>` 的字符串

- [ ] **Step 4: 提交高亮显示功能**

```bash
git add docs/json-to-schema.html
git commit -m "feat: 实现 schema 高亮显示功能，标记 null 值字段"
```

---

### Task 6: 实现 UI 交互功能

**Files:**
- Modify: `docs/json-to-schema.html` (在 `<script>` 标签内，高亮模块后)

- [ ] **Step 1: 编写转换按钮事件处理**

在高亮模块后添加：

```javascript
// UI 交互模块

// 全局变量：保存当前生成的 schema（用于复制）
let currentSchema = null;

// 转换按钮点击事件
function handleConvert() {
    clearError();

    const jsonInput = document.getElementById('jsonInput').value;
    const parseResult = parseJsonInput(jsonInput);

    if (!parseResult.success) {
        showError(parseResult.error);
        return;
    }

    const { schema, nullFields } = inferSchema(parseResult.data);
    currentSchema = schema; // 保存用于复制

    // 显示带高亮的 schema（使用 HTML 格式）
    const highlightedSchema = formatSchemaWithHighlight(schema, nullFields);

    // 由于 textarea 不支持 HTML，我们创建一个自定义显示区域
    // 这里简化处理：直接在 textarea 中显示纯文本，提示用户查看高亮信息
    const plainSchema = formatSchemaPlain(schema);
    const outputTextarea = document.getElementById('schemaOutput');

    if (nullFields.size > 0) {
        outputTextarea.value = plainSchema + '\n\n--- 提示 ---\n以下字段从 null 值推断，需要确认类型:\n' +
            Array.from(nullFields).join(', ');
    } else {
        outputTextarea.value = plainSchema;
    }
}
```

- [ ] **Step 2: 编写复制按钮事件处理**

继续添加：

```javascript
// 复制按钮点击事件
function handleCopy() {
    if (!currentSchema) {
        showError('请先转换 JSON 数据');
        return;
    }

    const plainSchema = formatSchemaPlain(currentSchema);

    // 使用 Clipboard API
    if (navigator.clipboard) {
        navigator.clipboard.writeText(plainSchema).then(() => {
            alert('Schema 已复制到剪贴板');
        }).catch(err => {
            showError('复制失败: ' + err.message);
        });
    } else {
        // 降级方案：使用传统方法
        const textarea = document.getElementById('schemaOutput');
        textarea.select();
        try {
            document.execCommand('copy');
            alert('Schema 已复制到剪贴板');
        } catch (err) {
            showError('复制失败: ' + err.message);
        }
    }
}
```

- [ ] **Step 3: 编写清空按钮事件处理**

继续添加：

```javascript
// 清空按钮点击事件
function handleClear() {
    document.getElementById('jsonInput').value = '';
    document.getElementById('schemaOutput').value = '';
    clearError();
    currentSchema = null;
}
```

- [ ] **Step 4: 绑定按钮事件监听器**

继续添加：

```javascript
// 初始化：绑定按钮事件
document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('convertBtn').addEventListener('click', handleConvert);
    document.getElementById('copyBtn').addEventListener('click', handleCopy);
    document.getElementById('clearBtn').addEventListener('click', handleClear);
});
```

- [ ] **Step 5: 在浏览器中测试完整交互流程**

1. 在浏览器中打开 `docs/json-to-schema.html`
2. 在左侧输入框粘贴测试数据：`{"id": 1, "name": null, "created": "2024-01-01 10:30:00"}`
3. 点击"转换"按钮
4. 验证右侧显示生成的 schema，包含提示信息
5. 点击"复制"按钮
6. 验证剪贴板内容为格式化的 JSON schema
7. 点击"清空"按钮
8. 验证所有内容被清空

预期: 所有功能正常工作

- [ ] **Step 6: 提交 UI 交互功能**

```bash
git add docs/json-to-schema.html
git commit -m "feat: 实现 UI 交互功能，支持转换、复制和清空操作"
```

---

### Task 7: 改进高亮显示（使用 HTML 显示区域）

**Files:**
- Modify: `docs/json-to-schema.html` (HTML 结构和 JavaScript)

- [ ] **Step 1: 添加 HTML 显示区域（支持 HTML 内容）**

修改 HTML 结构，将输出区域的 textarea 替换为带 HTML 显示的容器：

找到 `<div class="output-section">` 部分，替换为：

```html
<div class="output-section">
    <h2>Schema 输出</h2>
    <div id="schemaDisplay" class="schema-display"></div>
    <textarea id="schemaOutput" placeholder="可编辑 schema..." style="height: 150px;"></textarea>
</div>
```

并在 CSS 中添加新样式：

```css
.schema-display {
    width: 100%;
    height: 350px;
    padding: 10px;
    border: 1px solid #ddd;
    border-radius: 4px;
    background-color: white;
    font-family: 'Courier New', monospace;
    font-size: 14px;
    overflow: auto;
    white-space: pre;
}
```

- [ ] **Step 2: 修改 JavaScript 以支持 HTML 显示**

修改 `handleConvert` 函数：

```javascript
function handleConvert() {
    clearError();

    const jsonInput = document.getElementById('jsonInput').value;
    const parseResult = parseJsonInput(jsonInput);

    if (!parseResult.success) {
        showError(parseResult.error);
        return;
    }

    const { schema, nullFields } = inferSchema(parseResult.data);
    currentSchema = schema;

    // HTML 显示区域（带高亮）
    const highlightedSchema = formatSchemaWithHighlight(schema, nullFields);
    const schemaDisplay = document.getElementById('schemaDisplay');
    schemaDisplay.innerHTML = highlightedSchema;

    // 可编辑 textarea（纯文本）
    const plainSchema = formatSchemaPlain(schema);
    document.getElementById('schemaOutput').value = plainSchema;
}
```

- [ ] **Step 3: 修改复制功能（从 textarea 复制）**

修改 `handleCopy` 函数：

```javascript
function handleCopy() {
    const schemaText = document.getElementById('schemaOutput').value;

    if (!schemaText) {
        showError('请先转换 JSON 数据');
        return;
    }

    if (navigator.clipboard) {
        navigator.clipboard.writeText(schemaText).then(() => {
            alert('Schema 已复制到剪贴板');
        }).catch(err => {
            showError('复制失败: ' + err.message);
        });
    } else {
        const textarea = document.getElementById('schemaOutput');
        textarea.select();
        try {
            document.execCommand('copy');
            alert('Schema 已复制到剪贴板');
        } catch (err) {
            showError('复制失败: ' + err.message);
        }
    }
}
```

- [ ] **Step 4: 修改清空功能（清空两个输出区域）**

修改 `handleClear` 函数：

```javascript
function handleClear() {
    document.getElementById('jsonInput').value = '';
    document.getElementById('schemaDisplay').innerHTML = '';
    document.getElementById('schemaOutput').value = '';
    clearError();
    currentSchema = null;
}
```

- [ ] **Step 5: 在浏览器中测试高亮显示**

1. 在浏览器中打开文件
2. 输入测试数据：`{"id": 1, "name": null, "age": null}`
3. 点击"转换"
4. 验证上方显示区域中 `STRING` 类型有黄色背景高亮
5. 验证下方 textarea 可编辑
6. 点击"复制"，验证复制的是 textarea 内容

预期: 高亮显示正常，编辑和复制功能正常

- [ ] **Step 6: 提交改进的高亮显示**

```bash
git add docs/json-to-schema.html
git commit -m "feat: 改进 schema 高亮显示，使用 HTML 显示区域和可编辑 textarea"
```

---

### Task 8: 完整功能测试和验证

**Files:**
- Modify: `docs/json-to-schema.html` (无修改，仅测试)

- [ ] **Step 1: 测试简单类型推断**

在浏览器中测试：
- 输入：`{"id": 123, "name": "John", "age": 30, "created": "2024-01-01 10:30:00"}`
- 点击"转换"
- 验证输出：`{"id": "INT", "name": "STRING", "age": "INT", "created": "TIMESTAMP"}`

预期: 类型推断正确，无高亮标记

- [ ] **Step 2: 测试数字边界值**

测试：
- 输入：`{"small": 100, "intMax": 2147483647, "large": 3000000000, "float": 3.14, "negative": -100}`
- 点击"转换"
- 验证输出类型：`INT`、`INT`、`LONG`、`DOUBLE`、`INT`

预期: 数字类型边界判断正确

- [ ] **Step 3: 测试 null 值高亮**

测试：
- 输入：`{"id": 1, "name": null, "email": null}`
- 点击"转换"
- 验证输出区域：`STRING` 类型有黄色背景高亮
- 验证 textarea 提示信息

预期: null 值字段被高亮显示

- [ ] **Step 4: 测试数组类型**

测试：
- 输入：`{"tags": ["a", "b", "c"], "scores": [1, 2, 3]}`
- 点击"转换"
- 验证输出：`{"tags": ["STRING"], "scores": ["INT"]}`

预期: 数组类型推断正确

- [ ] **Step 5: 测试嵌套对象**

测试：
- 输入：`{"address": {"city": "Beijing", "zip": 100000, "street": null}}`
- 点击"转换"
- 验证输出：嵌套对象结构正确，`street` 字段高亮

预期: 嵌套对象推断正确，null 字段高亮

- [ ] **Step 6: 测试对象数组**

测试：
- 输入：`{"friends": [{"name": "Tom", "age": 25}, {"name": "Jerry", "age": 30}]}`
- 点击"转换"
- 验证输出：`{"friends": [{"name": "STRING", "age": "INT"}]}`

预期: 对象数组推断正确

- [ ] **Step 7: 测试错误处理**

测试：
- 输入：`{invalid json}`
- 点击"转换"
- 验证显示错误提示

预期: 错误提示正确显示

- [ ] **Step 8: 测试复制和清空功能**

测试：
- 执行转换后，点击"复制"
- 验证剪贴板内容
- 点击"清空"
- 验证所有区域被清空

预期: 复制和清空功能正常

- [ ] **Step 9: 最终提交（如果所有测试通过）**

```bash
git add docs/json-to-schema.html
git commit -m "feat: 完成 JSON to Schema Converter 工具开发和测试验证"
```

---

## 验证标准

工具完成后，应满足以下标准：

1. **功能完整性**：
   - JSON 输入解析正确
   - 类型推断准确（STRING、INT、LONG、DOUBLE、BOOLEAN、TIMESTAMP）
   - 支持 ARRAY 和 OBJECT 类型
   - null 值处理正确（默认 STRING + 高亮）

2. **用户体验**：
   - 布局清晰，三列结构合理
   - 高亮显示明显，易于识别
   - 错误提示清晰
   - 复制和清空功能流畅

3. **技术质量**：
   - 单文件实现，无外部依赖
   - 代码结构清晰，模块化设计
   - 离线可用，浏览器兼容性良好

4. **测试覆盖**：
   - 所有类型推断场景测试通过
   - 边界值测试通过
   - 错误处理测试通过

---

## 后续优化建议（不在当前实现范围）

1. 支持更多日期格式（ISO 8601、Unix 时间戳）
2. 提供 DECIMAL 类型推断
3. 支持批量 JSON 样本处理
4. 导出为 .json 文件功能
5. 可配置的推断规则