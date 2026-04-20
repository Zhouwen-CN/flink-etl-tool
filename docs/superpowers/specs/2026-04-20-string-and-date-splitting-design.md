# 字符串和日期分片功能设计

**日期：** 2026-04-20
**状态：** 设计阶段
**影响模块：** flink-etl-connector/connector-jdbc

## 一、概述

### 目标

为 JDBC Source 添加字符串和日期类型的分片支持，实现并行读取能力。

### 背景

当前 JDBC Source 仅支持数值类型分片（INT、BIGINT 等），字符串和日期类型字段无法分片，只能全表扫描。参考 SeaTunnel 的实现，扩展分片策略。

### 实现范围

本次实现**基础版本**，不包含高级特性（数据倾斜处理、采样分片等）：
- **字符串分片**：Hash Mod 方式（`WHERE HASH(column) % N = i`）
- **日期分片**：动态粒度（根据数据天数和并行度自动计算每个分片天数）
- **重要变更**：所有分片类型统一使用开区间边界（`>= start AND < end`）

### 支持的 JDBC 类型

| 策略 | 支持的 JDBC 类型 |
|------|-----------------|
| NUMERIC | TINYINT, SMALLINT, INT, BIGINT, DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE |
| STRING_HASH | CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR |
| DATE_RANGE | DATE, TIMESTAMP |

---

## 二、架构设计

### 2.1 Splitter 抽象层

引入 `ChunkSplitter` 抽象类，实现策略模式：

```
ChunkSplitter (抽象基类)
    ├── NumericSplitter        # 数值分片（迁移现有逻辑）
    ├── StringHashSplitter     # 字符串 hash 分片
    ├── DateSplitter           # 日期动态粒度分片
    └── FullTableScanSplitter  # 全表扫描（单分片）
```

**核心接口：**
```java
public abstract class ChunkSplitter {
    protected final JdbcSourceConfig config;
    protected final int parallelism;

    public ChunkSplitter(JdbcSourceConfig config, int parallelism) {
        this.config = config;
        this.parallelism = parallelism;
    }

    public abstract List<RangeSplit> generateSplits();
}
```

**参数说明：**
- `JdbcSourceConfig`：已包含所有必要信息（URL、表名、分片列、方言等）
- `parallelism`：期望的并行度（从 `SplitEnumeratorContext` 获取）

### 2.2 文件结构

```
flink-etl-connector/connector-jdbc/src/main/java/com/etl/connector/jdbc/source/
├── splitter/
│   ├── ChunkSplitter.java          # 抽象基类
│   ├── NumericSplitter.java        # 数值分片器
│   ├── StringHashSplitter.java     # 字符串 hash 分片器
│   ├── DateSplitter.java           # 日期动态粒度分片器
│   └── FullTableScanSplitter.java  # 全表扫描分片器
├── JdbcSplitEnumerator.java        # 重构：在 start() 中选择 Splitter
├── JdbcSplitHelper.java            # 重构：职责缩小为辅助工具类
├── RangeSplit.java                 # 保持不变
├── enums/
│   └── SplitStrategy.java          # 添加 STRING_HASH 和 DATE_RANGE
```

### 2.3 职责分离

| 组件 | 职责 |
|------|------|
| `JdbcSplitEnumerator` | 选择策略 → 创建 Splitter → 分配分片给 Reader |
| `ChunkSplitter` | 计算分片边界 → 生成 SQL → 返回 RangeSplit 列表 |
| `JdbcDialect` | 提供 hash 函数表达式、构建日期范围查询 SQL |
| `JdbcSplitHelper` | 辅助静态方法：查询 MIN/MAX、获取列类型、选择最优主键 |
| `RangeSplit` | 存储完整 SQL（splitId + querySql） |

---

## 三、构件设计

### 3.1 SplitStrategy 枚举扩展

```java
public enum SplitStrategy {
    NUMERIC("数值范围分片", new int[]{
        Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
        Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL
    }),

    STRING_HASH("字符串 Hash Mod 分片", new int[]{
        Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR
    }),

    DATE_RANGE("日期动态粒度分片", new int[]{
        Types.DATE, Types.TIMESTAMP
    }),

    FULL_TABLE_SCAN("全表扫描", new int[]{});

    // 现有方法保持不变：fromJdbcType(), supports(), getSupportedTypeNames()
}
```

**类型识别逻辑：**
- `JdbcSource.inferSplitKey()` 调用 `SplitStrategy.fromJdbcType(jdbcType)`
- 返回对应的策略枚举值
- JDBC 类型不匹配 → 返回 null → 使用 FULL_TABLE_SCAN

### 3.2 JdbcDialect 接口扩展

新增两个方法，由各方言实现：

```java
public interface JdbcDialect {
    // 现有方法保持不变...

    /**
     * 生成字符串列的 hash mod 表达式
     *
     * @param columnName 列名（已转义）
     * @param modulus 模数（分片数量）
     * @return hash mod 表达式（如 "MD5(column) % 4"）
     */
    String hashModExpression(String columnName, int modulus);

    /**
     * 构建日期范围查询 SQL（开区间）
     *
     * @param baseQuery 基础查询（SELECT * FROM table）
     * @param columnName 列名（已转义）
     * @param startDate 起始日期（null 表示第一个分片）
     * @param endDate 结束日期（null 表示最后一个分片）
     * @return 完整查询 SQL（使用 >= AND < 开区间）
     */
    String buildDateRangeQuery(String baseQuery, String columnName,
                                String startDate, String endDate);
}
```

**各方言实现：**

| 方言 | hashModExpression | buildDateRangeQuery |
|------|-------------------|---------------------|
| MySQL | `CAST(MD5(column) AS UNSIGNED) % N` | 标准日期格式：`'yyyy-MM-dd'` |
| PostgreSQL | `hashtext(column) % N` | 标准日期格式：`'yyyy-MM-dd'` |
| Oracle | `ORA_HASH(column, N-1)` | 使用 `TO_DATE` 函数 |
| H2 | `MOD(HASH(column), N)` | 标准日期格式（测试用） |

### 3.3 NumericSplitter 实现

**核心逻辑：**
1. 查询 MIN/MAX 数值范围
2. 计算分片数量和步长
3. 生成开区间 SQL：`WHERE column >= start AND column < end+1`

**关键变更：**
- 从闭区间 `BETWEEN start AND end` 改为开区间
- SQL 格式：`WHERE splitKey >= 0 AND splitKey < 100`（而非 `BETWEEN 0 AND 99`）

**空表处理：**
- MIN/MAX 为 NULL → 返回空列表 `Collections.emptyList()`

### 3.4 StringHashSplitter 实现

**核心逻辑：**
1. 不查询数据范围（假设 hash 函数均匀分布）
2. 按并行度生成 N 个分片
3. 每个 分片 hash 值 = 分片编号（0 到 N-1）

**SQL 格式：**
```sql
SELECT * FROM table WHERE CAST(MD5(column) AS UNSIGNED) % 10 = 0
SELECT * FROM table WHERE CAST(MD5(column) AS UNSIGNED) % 10 = 1
...
SELECT * FROM table WHERE CAST(MD5(column) AS UNSIGNED) % 10 = 9
```

**特点：**
- 不依赖数据分布，直接按 hash 值分片
- 假设 hash 函数足够均匀（MD5、hashtext 等）

### 3.5 DateSplitter 实现

**核心逻辑：**
1. 查询 MIN/MAX 日期范围（支持 DATE 和 TIMESTAMP）
2. 计算总天数：`ChronoUnit.DAYS.between(minDate, maxDate) + 1`
3. 动态决定每个分片天数：`daysPerSplit = totalDays / splitCount`
4. 生成开区间 SQL：`WHERE date >= startDate AND date < endDate+1`

**日期处理：**
- 使用 `LocalDate` 计算（Java 8 时间 API）
- TIMESTAMP 类型：查询时转为 DATE（忽略时分秒）
- 格式化：`DateTimeFormatter.ISO_LOCAL_DATE`（`yyyy-MM-dd`）

**SQL 格式：**
```sql
-- 第一个分片
SELECT * FROM table WHERE date_column >= '2020-01-01' AND date_column < '2020-02-01'

-- 中间分片
SELECT * FROM table WHERE date_column >= '2020-02-01' AND date_column < '2020-03-01'

-- 最后一个分片
SELECT * FROM table WHERE date_column >= '2020-12-01' AND date_column < '2021-01-01'
```

**边界处理：**
- 跨年分片：`LocalDate.plusDays()` 自动处理
- 单日数据：生成 1 个分片
- 空表：返回空列表

### 3.6 JdbcSplitHelper 重构

**职责缩小：**
- 从"分片计算中心"变为"辅助工具类"
- 删除：`calculateNumericSplits()`、`buildSplitQuery()`、`createFullTableScanSplits()`
- 保留：查询 MIN/MAX、获取列类型、选择最优主键等静态方法

**新增方法：**
```java
public static Pair<Date, Date> queryDateMinMax(
    JdbcDialect dialect, String url, String username, String password,
    String table, String sql, String splitColumn);
```

**查询逻辑：**
- 构建 SQL：`SELECT MIN(column), MAX(column) FROM table`
- 使用 `ResultSet.getDate()` 获取 DATE 类型
- TIMESTAMP 自动转为 DATE

### 3.7 JdbcSplitEnumerator 重构

**在 start() 方法中的逻辑：**
```java
@Override
public void start() {
    int parallelism = context.currentParallelism();
    SplitStrategy strategy = config.getSplitStrategy();

    // 1. 创建对应的 Splitter
    ChunkSplitter splitter = createSplitter(strategy, config, parallelism);

    // 2. 生成分片
    List<RangeSplit> splits = splitter.generateSplits();

    // 3. 分配给 Reader（轮询分配）
    for (RangeSplit split : splits) {
        context.assignSplit(split, getSplitOwner(split.splitId()));
    }

    // 4. 标记完成
    context.signalNoMoreSplits();
}
```

**分片分配策略：**
- 轮询分配：`splitId % parallelism`（替代原有的 hash 分配）
- 更均匀，避免 hash 冲突

---

## 四、数据流和执行流程

### 4.1 分片生成流程

```
JdbcSource.inferSplitKey()
    → SplitStrategy.fromJdbcType(jdbcType)
    → 返回策略（NUMERIC / STRING_HASH / DATE_RANGE）

JdbcSplitEnumerator.start()
    → createSplitter(strategy, config, parallelism)
    → splitter.generateSplits()
        → NumericSplitter: 查询 MIN/MAX → 计算步长 → 生成开区间 SQL
        → StringHashSplitter: 生成 hash mod SQL（N 个分片）
        → DateSplitter: 查询 MIN/MAX 日期 → 计算天数 → 生成开区间 SQL
    → 返回 List<RangeSplit>

分配分片给 Reader
    → 轮询分配（splitId % parallelism）
    → context.assignSplit(split, readerId)
```

### 4.2 SQL 生成示例

**数值分片（开区间）：**
```sql
-- 原有：BETWEEN 0 AND 99
-- 新版：>= 0 AND < 100
SELECT * FROM users WHERE id >= 0 AND id < 100
SELECT * FROM users WHERE id >= 100 AND id < 200
```

**字符串 hash 分片：**
```sql
-- MySQL
SELECT * FROM users WHERE CAST(MD5(username) AS UNSIGNED) % 10 = 0
SELECT * FROM users WHERE CAST(MD5(username) AS UNSIGNED) % 10 = 1

-- PostgreSQL
SELECT * FROM users WHERE hashtext(username) % 10 = 0

-- Oracle
SELECT * FROM users WHERE ORA_HASH(username, 9) = 0
```

**日期动态粒度分片：**
```sql
-- 365 天数据，10 个并行度 → 每个分片约 36 天
SELECT * FROM orders WHERE order_date >= '2020-01-01' AND order_date < '2020-02-06'
SELECT * FROM orders WHERE order_date >= '2020-02-06' AND order_date < '2020-03-14'
```

---

## 五、错误处理和边界情况

### 5.1 空表处理

**统一方式：**
- 所有 Splitter 查询 MIN/MAX 时，如果返回 NULL → 空表
- 返回 `Collections.emptyList()`
- Enumerator 不分配分片，任务正常结束

**日志：**
```
WARN: 表为空，不创建分片
INFO: 生成 0 个分片
```

### 5.2 异常处理

**数据库查询异常：**
- `queryMinMax()` 失败 → 抛出 RuntimeException，任务失败
- 错误信息包含上下文：`"查询日期范围失败: Connection refused (url=jdbc:mysql://...)"`

**配置异常：**
- 无主键且未配置 splitKey → `NoPrimaryKeyException`（现有逻辑）
- 不支持的 JDBC 类型 → `IllegalArgumentException`（现有逻辑）

### 5.3 边界情况

**数值边界：**
- `min > max` → 数据为空，返回空列表
- `totalRecords < parallelism` → 自动减少分片数量

**日期边界：**
- 跨年分片：`LocalDate` 自动处理（如 2020-12-31 → 2021-01-01）
- 单日数据：`totalDays = 1` → 生成 1 个分片
- TIMESTAMP 精度：查询时转为 DATE，忽略时分秒

**字符串 hash：**
- 空字符串：hash 函数支持（MD5("") 有结果）
- hash 均匀性：假设足够均匀，不处理倾斜

### 5.4 方言兼容性

**不支持 hash 的方言：**
- H2（测试数据库）：使用 `MOD(HASH(column), N)` 或抛出异常
- 方言未实现方法 → Splitter 调用时抛出 `UnsupportedOperationException`

---

## 六、实现要点

### 6.1 关键重构点

1. **创建 splitter 包**：所有 Splitter 类放入独立包
2. **迁移数值分片逻辑**：从 `JdbcSplitHelper.calculateNumericSplits()` 到 `NumericSplitter.generateSplits()`
3. **修改 SQL 生成**：所有分片使用开区间边界
4. **扩展 JdbcDialect**：各方言实现 `hashModExpression()` 和 `buildDateRangeQuery()`
5. **重构 JdbcSplitEnumerator**：从直接调用工具类改为策略模式

### 6.2 代码复用

**JdbcSplitHelper 辅助方法：**
- `queryNumericMinMax()` - 查询数值范围（NumericSplitter 调用）
- `queryDateMinMax()` - 查询日期范围（DateSplitter 调用）
- `getColumnType()` - 获取 JDBC 类型（JdbcSource.inferSplitKey() 调用）
- `selectOptimalSplitKey()` - 选择最优主键（JdbcSource.inferSplitKey() 调用）

**Splitter 内部方法：**
- `buildBaseQuery()` - 构建基础查询（SELECT * FROM table）
- 所有 Splitter 都需要此方法，可提取到基类或工具类

### 6.3 测试策略

**单元测试：**
- `NumericSplitterTest`：验证开区间 SQL 生成、空表处理、边界情况
- `StringHashSplitterTest`：验证 hash SQL 格式、分片数量
- `DateSplitterTest`：验证日期计算、动态粒度、开区间 SQL
- `JdbcDialectTest`：验证 hash 表达式、日期范围查询

**集成测试：**
- 使用 H2 内存数据库测试完整流程
- 测试不同数据分布：均匀、倾斜、空表
- 验证并行度对分片数量的影响

---

## 七、后续迭代方向

本次实现基础版本，后续可扩展：

1. **Dynamic Splitter**：数据分布自适应（分布因子、采样分片、动态边界查询）
2. **Charset Based 字符串分片**：将字符串编码为数值，按数值方式分片（精确边界）
3. **多表并行读取**：支持 `tables[]` 配置，Enumerator 维护 `pendingTables` 队列
4. **TIMESTAMP 精细粒度**：按小时、分钟分片（适用于高精度时间戳）

---

## 八、影响范围

### 受影响的类

| 类 | 变更类型 |
|------|---------|
| `JdbcSplitEnumerator` | 重构（选择 Splitter） |
| `JdbcSplitHelper` | 重构（职责缩小） |
| `SplitStrategy` | 扩展（添加枚举值） |
| `JdbcDialect` 及各方言 | 扩展（添加接口方法） |
| `NumericSplitter` | 新增（迁移现有逻辑） |
| `StringHashSplitter` | 新增 |
| `DateSplitter` | 新增 |
| `FullTableScanSplitter` | 新增 |

### 不受影响的类

- `JdbcSource`：只在 `inferSplitKey()` 中识别新策略
- `JdbcSourceReader`：保持不变（只执行 SQL）
- `JdbcSplitReader`：保持不变（只读取数据）
- `RangeSplit`：保持不变（存储完整 SQL）
- `JdbcSourceConfig`：保持不变（已包含所有字段）

---

## 九、设计决策记录

### 为什么选择 Hash Mod 而非 Charset Based？

- **Hash Mod 更简单**：不需要方言特定的编码实现
- **Charset Based 复杂**：需要了解数据库的 collation sequence，实现难度高
- **数据均匀性假设**：基础版本假设数据均匀，不处理倾斜

### 为什么使用开区间而非闭区间？

- **避免边界重叠**：闭区间 `BETWEEN 0 AND 99` 和 `BETWEEN 100 AND 199` 清晰，但日期类型容易出现边界混淆
- **日期边界更精确**：`>= '2020-01-01' AND < '2020-02-01'` 避免 1月31日 vs 2月1日的歧义
- **统一逻辑**：所有分片类型使用相同边界方式，易于理解

### 为什么不创建 SplitContext 类？

- **JdbcSourceConfig 已包含所有信息**：避免重复包装
- **减少类的数量**：保持简洁
- **parallelism 从 context 获取**：传递参数而非创建新类

### 为什么选择轮询分配而非 hash 分配？

- **更均匀**：避免 hash 冲突导致的分配不均
- **简单可靠**：`splitId % parallelism` 直接计算
- **SeaTunnel 的教训**：hash 分配可能不均匀

---

## 十、验收标准

实现完成后应满足：

1. **功能完整性**：
   - 字符串和日期类型可以自动识别分片策略
   - 生成的 SQL 使用开区间边界
   - 支持空表、单日数据等边界情况

2. **兼容性**：
   - 现有数值分片功能保持正常（只是 SQL 格式变更）
   - 无主键降级为全表扫描（保持现有逻辑）
   - 配置文件无需修改（自动推断策略）

3. **代码质量**：
   - Splitter 职责清晰，每个类只处理一种类型
   - 方言扩展遵循现有模式
   - JdbcSplitHelper 职责明确

4. **测试覆盖**：
   - 单元测试覆盖所有 Splitter
   - 集成测试验证完整流程
   - 边界情况测试（空表、单日、跨年）