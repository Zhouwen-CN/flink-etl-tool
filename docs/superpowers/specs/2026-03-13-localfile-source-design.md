# LocalFile Source 设计文档

## 概述

实现本地文件 Source 插件，支持通过通配符匹配文件、按文件分片并行读取、SPI 扩展多种数据格式。

## 需求

1. 路径配置支持通配符，配置项控制是否递归
2. 一个文件对应一个分片
3. 格式解析使用 SPI 机制，配置中显式指定格式
4. 统一输出 Flink Row 类型
5. 先实现 CSV 格式，架构方便扩展

## 模块结构

```
flink-etl-source/
└── flink-etl-source-localfile/          # 新增模块
    ├── pom.xml
    └── src/main/java/com/etl/source/localfile/
        ├── LocalFileSourcePlugin.java    # SourcePlugin 实现
        ├── LocalFileSource.java          # AbstractSplitSource 实现
        ├── LocalFileSplit.java           # 文件分片
        ├── LocalFileSplitEnumerator.java # 分片枚举器
        ├── LocalFileSourceReader.java    # SourceReader
        ├── LocalFileSplitReader.java     # 分片读取器
        └── format/                       # 格式解析子包
            ├── FileFormatPlugin.java     # 格式解析 SPI 接口
            └── CsvFormatPlugin.java      # CSV 格式实现
```

## 配置格式

### 基础配置

```json
{
  "source": {
    "type": "localfile",
    "config": {
      "path": "/data/input/**/*.csv",
      "format": "csv",
      "recursive": true,
      "encoding": "UTF-8",
      "delimiter": ",",
      "header": true
    }
  }
}
```

### 无文件头模式

```json
{
  "source": {
    "type": "localfile",
    "config": {
      "path": "/data/input/**/*.csv",
      "format": "csv",
      "recursive": true,
      "encoding": "UTF-8",
      "delimiter": ",",
      "header": false,
      "columns": ["id", "name", "age", "email"]
    }
  }
}
```

### 配置项说明

| 配置项 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| path | 是 | - | 文件路径，支持通配符 `*` 和 `**` |
| format | 是 | - | 文件格式，如 `csv`、`excel` |
| recursive | 否 | false | 是否递归匹配子目录 |
| encoding | 否 | UTF-8 | 文件编码 |
| delimiter | 否 | , | 字段分隔符（CSV 专用） |
| header | 否 | true | 是否从文件头获取列名（CSV 专用） |
| columns | header=false 时必填 | - | 字段名列表（CSV 专用） |

## 核心接口设计

### FileFormatPlugin

```java
package com.etl.source.localfile.format;

import com.etl.core.config.SourceConfig;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.util.List;

/**
 * 文件格式解析插件接口
 */
public interface FileFormatPlugin {

    /**
     * 获取格式类型标识
     */
    String getType();

    /**
     * 获取字段名列表
     * header=true 时从文件头解析，header=false 时从配置获取
     *
     * @param config 配置
     * @param firstFile 第一个文件的输入流（用于解析文件头）
     *                  调用方负责打开和关闭此流
     * @return 字段名列表
     */
    List<String> resolveFields(SourceConfig config, InputStream firstFile);

    /**
     * 解析文件内容，返回 Row 迭代器
     *
     * @param config 配置
     * @param inputStream 文件输入流
     *                    调用方负责打开，实现方负责在迭代完成后关闭
     * @param fields 字段名列表
     * @return Row 迭代器
     */
    Iterable<Row> parse(SourceConfig config, InputStream inputStream, List<String> fields);
}
```

### LocalFileSplit

```java
package com.etl.source.localfile;

import com.etl.core.source.BaseSourceSplit;
import lombok.Getter;

import java.io.Serializable;

/**
 * 文件分片
 * 一个文件对应一个分片
 */
@Getter
public class LocalFileSplit implements BaseSourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String splitId;      // 分片 ID = 文件路径
    private final String filePath;      // 文件绝对路径
    private final String fileName;      // 文件名

    public LocalFileSplit(String filePath) {
        this.filePath = filePath;
        this.fileName = new java.io.File(filePath).getName();
        this.splitId = filePath;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
```

### LocalFileSplitState

- 继承 `BaseSplitState<LocalFileSplit>`
- 参考 `RangeSplitState` 实现
- 用于追踪分片读取状态，支持断点续传

### LocalFileEnumCheckpoint

- 继承 `BaseEnumCheckpoint<LocalFileSplit>`
- 参考 `RangeEnumCheckpoint` 实现
- 用于保存枚举器状态，支持 Checkpoint 恢复

## 数据流程

```
┌─────────────────────────────────────────────────────────────────┐
│                        LocalFileSourcePlugin                     │
│  1. 根据 format 类型通过 SPI 加载 FileFormatPlugin               │
│  2. 创建 LocalFileSource                                         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        LocalFileSource                           │
│  1. 创建 LocalFileSplitEnumerator                                │
│  2. 创建 LocalFileSourceReader                                   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   LocalFileSplitEnumerator                       │
│  1. 扫描文件系统，匹配通配符路径                                   │
│  2. 每个文件创建一个 LocalFileSplit                               │
│  3. 分配分片给 Reader                                             │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LocalFileSourceReader                         │
│  1. 接收分片，创建 LocalFileSplitReader                           │
│  2. 管理分片状态                                                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LocalFileSplitReader                          │
│  1. 打开文件输入流                                                │
│  2. 调用 FileFormatPlugin.parse() 解析内容                        │
│  3. 返回 Row 记录                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 类设计

### LocalFileSourcePlugin

- 实现 `SourcePlugin` 接口
- 添加 `@AutoService(SourcePlugin.class)` 注解
- 职责：根据 format 配置加载 FileFormatPlugin，创建 LocalFileSource

### LocalFileSource

- 继承 `AbstractSplitSource<Row, LocalFileSplit, LocalFileEnumCheckpoint>`
- 职责：
  - 创建 SplitEnumerator 和 SourceReader
  - 管理格式插件实例

### LocalFileSplitEnumerator

- 继承 `BaseSplitEnumerator<LocalFileSplit, LocalFileEnumCheckpoint>`
- 职责：
  - 扫描文件系统，匹配通配符路径
  - 调用 `FileFormatPlugin.resolveFields()` 解析字段名（仅一次，使用第一个文件）
  - 创建文件分片并分配给 Reader

### LocalFileSourceReader

- 继承 `BaseSourceReader<Row, Row, LocalFileSplit, LocalFileSplitState>`
- 职责：管理分片状态，创建 SplitReader

### LocalFileSplitReader

- 实现 `BaseSplitReader<Row, LocalFileSplit>` 接口
- 职责：
  - 打开文件输入流
  - 调用 `FileFormatPlugin.parse()` 解析数据
  - 返回 Row 记录

### CsvFormatPlugin

- 实现 `FileFormatPlugin` 接口
- 添加 `@AutoService(FileFormatPlugin.class)` 注解
- 职责：
  - 解析 CSV 文件头获取字段名（header=true）
  - 解析 CSV 内容为 Row

## 文件通配符匹配

使用 Java NIO 的 `PathMatcher` 实现：

- `*` 匹配单个目录下的文件名
- `**` 跨目录层级匹配（需要 recursive=true）

示例：
- `/data/*.csv` - 匹配 /data 目录下所有 CSV 文件
- `/data/**/*.csv` - 递归匹配 /data 及子目录下所有 CSV 文件

## 错误处理

1. **无匹配文件**：启动时检查，若无任何文件匹配路径，抛出明确异常 `NoFilesFoundException`
2. **文件不存在**：启动时检查，抛出明确异常
3. **文件读取失败**：记录错误日志，跳过该文件继续处理其他文件
4. **格式解析失败**：记录错误日志，抛出异常终止任务
5. **编码错误**：使用配置的编码，解析失败时抛出异常

## 扩展性

添加新格式只需：

1. 创建 `FileFormatPlugin` 实现类
2. 添加 `@AutoService(FileFormatPlugin.class)` 注解
3. 在 client 模块添加依赖

无需修改 LocalFileSource 代码。