# Schema 类型系统设计

## 概述

为 ETL 框架设计一套 Schema 类型系统，支持在 Source 级别配置数据类型，实现类型转换和 Flink 类型系统集成。

## 背景

### 问题描述

当前 CSV Source 读取的所有字段都是 String 类型，无法指定实际数据类型。这导致：
1. 下游无法获得正确的类型信息
2. 数据处理需要额外进行类型转换
3. 与 JDBC Source（自动获取数据库类型）行为不一致

### 目标

1. 提供统一的 Schema 定义方式
2. 在 Source 级别完成类型转换
3. 将类型信息传递给 Flink 下游
4. 复用到所有 Source 插件

## 设计决策

| 决策项 | 选择 | 理由 |
|--------|------|------|
| Schema 位置 | Source 级别 | 类型转换在 Source 完成，传递给下游 |
| Schema 定义方式 | 简单列表形式 | 简洁易用，类似 SeaTunnel |
| 支持类型 | 精简集（8种） | 覆盖大部分场景，减少复杂度 |
| Schema 可选性 | CSV 必填，JDBC 可选 | CSV 无类型信息，JDBC 可从数据库获取 |
| 架构方案 | 抽象层实现 | 复用到所有 Source 插件 |

## 类型系统

### 支持的类型

| 类型名 | Java 类型 | Flink LogicalType |
|--------|-----------|-------------------|
| `string` | String | VarCharType |
| `boolean` | Boolean | BooleanType |
| `int` | Integer | IntType |
| `long` | Long | BigIntType |
| `double` | Double | DoubleType |
| `decimal` | BigDecimal | DecimalType |
| `timestamp` | LocalDateTime | TimestampType |
| `bytes` | byte[] | VarBinaryType |

### 配置格式

```json
{
  "source": {
    "type": "localfile",
    "localFileSourceConfig": {
      "path": "/data/users.csv",
      "format": "csv",
      "skipHeader": true,
      "schema": {
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "age", "type": "int"},
          {"name": "price", "type": "double"},
          {"name": "created_at", "type": "timestamp"},
          {"name": "is_active", "type": "boolean"},
          {"name": "data", "type": "bytes"}
        ]
      }
    }
  }
}
```

**CSV 配置参数说明：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `schema` | object | **是** | - | 字段定义，包含字段名和类型 |
| `skipHeader` | boolean | 否 | `true` | 是否跳过 CSV 第一行（头行） |
| `delimiter` | string | 否 | `,` | CSV 分隔符 |
| `encoding` | string | 否 | `UTF-8` | 文件编码 |

## 模块设计

### 包结构

```
com.etl.core.schema/
├── EtlFieldType.java        # 类型枚举
├── EtlField.java            # 字段定义（name + type）
├── EtlSchema.java           # Schema 容器
├── SchemaParser.java        # 从 SourceConfig 解析 Schema
├── FlinkTypeConverter.java  # EtlSchema → Flink RowType
└── TypeConverter.java       # 原始值 → 目标类型转换
```

### 核心类设计

#### EtlFieldType

```java
public enum EtlFieldType {
    STRING,
    BOOLEAN,
    INT,
    LONG,
    DOUBLE,
    DECIMAL,
    TIMESTAMP,
    BYTES;

    public static EtlFieldType fromString(String typeName) {
        // 解析类型字符串，支持大小写不敏感
    }
}
```

#### EtlField

```java
@Data
@AllArgsConstructor
public class EtlField implements Serializable {
    private String name;
    private EtlFieldType type;
}
```

#### EtlSchema

```java
@Data
public class EtlSchema implements Serializable {
    private List<EtlField> fields;

    public EtlField getField(int index) {
        return fields.get(index);
    }

    public EtlField getField(String name) {
        return fields.stream()
            .filter(f -> f.getName().equals(name))
            .findFirst()
            .orElse(null);
    }

    public List<String> getFieldNames() {
        return fields.stream().map(EtlField::getName).collect(Collectors.toList());
    }
}
```

#### SchemaParser

```java
public class SchemaParser {

    @SuppressWarnings("unchecked")
    public static EtlSchema parse(Object schemaConfig) {
        if (schemaConfig == null) {
            return null;
        }

        // 类型校验
        if (!(schemaConfig instanceof Map)) {
            throw new SchemaConfigException("schema 必须是一个对象");
        }

        Map<String, Object> schemaMap = (Map<String, Object>) schemaConfig;
        Object fieldsObj = schemaMap.get("fields");

        if (fieldsObj == null) {
            throw new SchemaConfigException("schema 缺少 'fields' 字段");
        }

        if (!(fieldsObj instanceof List)) {
            throw new SchemaConfigException("'fields' 必须是数组");
        }

        List<Map<String, Object>> fieldsConfig = (List<Map<String, Object>>) fieldsObj;

        List<EtlField> fields = new ArrayList<>();
        for (int i = 0; i < fieldsConfig.size(); i++) {
            Map<String, Object> fieldConfig = fieldsConfig.get(i);

            Object nameObj = fieldConfig.get("name");
            if (nameObj == null) {
                throw new SchemaConfigException("字段[" + i + "] 缺少 'name'");
            }
            if (!(nameObj instanceof String)) {
                throw new SchemaConfigException("字段[" + i + "] 的 'name' 必须是字符串");
            }

            Object typeObj = fieldConfig.get("type");
            if (typeObj == null) {
                throw new SchemaConfigException("字段[" + i + "] 缺少 'type'");
            }
            if (!(typeObj instanceof String)) {
                throw new SchemaConfigException("字段[" + i + "] 的 'type' 必须是字符串");
            }

            String name = (String) nameObj;
            String typeName = (String) typeObj;
            EtlFieldType type = EtlFieldType.fromString(typeName);
            if (type == null) {
                throw new SchemaConfigException(
                    "字段[" + i + "] '" + name + "' 的类型 '" + typeName + "' 不支持");
            }

            fields.add(new EtlField(name, type));
        }

        return new EtlSchema(fields);
    }
}
```

#### SchemaConfigException

```java
public class SchemaConfigException extends RuntimeException {
    public SchemaConfigException(String message) {
        super("Schema 配置错误: " + message);
    }
}
```

#### TypeConverter

```java
public class TypeConverter {
    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将原始值转换为目标类型
     *
     * @param value 原始值（通常是 String）
     * @param fieldName 字段名（用于错误信息）
     * @param targetType 目标类型
     * @return 转换后的值
     * @throws TypeConversionException 转换失败时抛出
     */
    public static Object convert(Object value, String fieldName, EtlFieldType targetType) {
        if (value == null) {
            return null;
        }

        // 如果已经是目标类型或兼容类型，直接返回
        if (isCompatibleType(value, targetType)) {
            return value;
        }

        String strValue = String.valueOf(value).trim();
        if (strValue.isEmpty()) {
            return null;
        }

        try {
            switch (targetType) {
                case STRING:
                    return strValue;
                case BOOLEAN:
                    return parseBoolean(strValue);
                case INT:
                    return Integer.parseInt(strValue);
                case LONG:
                    return Long.parseLong(strValue);
                case DOUBLE:
                    return Double.parseDouble(strValue);
                case DECIMAL:
                    return new BigDecimal(strValue);
                case TIMESTAMP:
                    return LocalDateTime.parse(strValue, DEFAULT_TIMESTAMP_FORMAT);
                case BYTES:
                    return parseBytes(value, strValue);
                default:
                    throw new IllegalArgumentException("不支持的类型: " + targetType);
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new TypeConversionException(fieldName, strValue, targetType, e);
        }
    }

    private static boolean isCompatibleType(Object value, EtlFieldType targetType) {
        switch (targetType) {
            case STRING:
                return value instanceof String;
            case BOOLEAN:
                return value instanceof Boolean;
            case INT:
                return value instanceof Integer;
            case LONG:
                return value instanceof Long;
            case DOUBLE:
                return value instanceof Double;
            case DECIMAL:
                return value instanceof BigDecimal;
            case TIMESTAMP:
                return value instanceof LocalDateTime || value instanceof java.sql.Timestamp;
            case BYTES:
                return value instanceof byte[];
            default:
                return false;
        }
    }

    private static Boolean parseBoolean(String value) {
        // 支持多种布尔值表示
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)) {
            return false;
        }
        throw new NumberFormatException("无法解析为布尔值: " + value);
    }

    private static byte[] parseBytes(Object value, String strValue) {
        // 如果已经是字节数组，直接返回
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        // 字符串转字节数组
        return strValue.getBytes(StandardCharsets.UTF_8);
    }
}
```

#### FlinkTypeConverter

```java
public class FlinkTypeConverter {
    public static RowType toRowType(EtlSchema schema) {
        List<RowType.RowField> fields = schema.getFields().stream()
            .map(f -> new RowType.RowField(f.getName(), toLogicalType(f.getType())))
            .collect(Collectors.toList());
        return new RowType(fields);
    }

    private static LogicalType toLogicalType(EtlFieldType type) {
        switch (type) {
            case STRING:
                return new VarCharType(VarCharType.MAX_LENGTH);
            case BOOLEAN:
                return new BooleanType();
            case INT:
                return new IntType();
            case LONG:
                return new BigIntType();
            case DOUBLE:
                return new DoubleType();
            case DECIMAL:
                return new DecimalType(38, 18); // 默认精度
            case TIMESTAMP:
                return new TimestampType(9); // 纳秒精度
            case BYTES:
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            default:
                throw new IllegalArgumentException("不支持的类型: " + type);
        }
    }
}
```

### 抽象层改造

#### SourceConfig 扩展

```java
// SourceConfig 新增方法
public EtlSchema getSchema() {
    if (localFileSourceConfig == null) {
        return null;
    }
    return SchemaParser.parse(localFileSourceConfig.get("schema"));
}
```

#### AbstractSplitSource 扩展

```java
public abstract class AbstractSplitSource<T, SplitT extends SourceSplit, CheckpointT>
        implements Source<T, SplitT, CheckpointT>, ResultTypeQueryable<T> {

    protected EtlSchema schema;

    @Override
    public TypeInformation<T> getProducedType() {
        if (schema != null) {
            RowType rowType = FlinkTypeConverter.toRowType(schema);
            return (TypeInformation<T>) RowTypeInfo.of(rowType);
        }
        return null;
    }
}
```

## 插件适配

### Row 类型规范

**统一使用位置访问方式**：
- 使用 `new Row(size)` 创建 Row
- 使用 `row.setField(index, value)` 设置字段值
- 与 Flink Source 抽象层设计一致

### LocalFile Source (CSV)

**要求**：必须配置 schema

**CSV 参数变更：**

| 旧参数 | 新参数 | 说明 |
|--------|--------|------|
| `header=true` | `skipHeader=true` | 语义调整：从"是否有头"改为"是否跳过头行" |
| `header=false` + `columns` | 删除 | 字段名从 schema 获取，无需 columns |
| - | `schema`（必填） | 新增：定义字段名和类型 |

**字段数量匹配规则**：
- CSV 列数 < schema 字段数：缺少字段设为 null，记录警告日志
- CSV 列数 > schema 字段数：多余列忽略，记录警告日志
- CSV 列数 = schema 字段数：正常处理

**CsvFormatPlugin 改造**：

```java
@Slf4j
@AutoService(FileFormatPlugin.class)
public class CsvFormatPlugin implements FileFormatPlugin {

    @Override
    public String getType() {
        return "csv";
    }

    @Override
    public List<String> resolveFields(SourceConfig localFileSourceConfig, InputStream firstFile) {
        // 字段名从 schema 获取
        EtlSchema schema = localFileSourceConfig.getSchema();
        if (schema == null) {
            throw new SchemaConfigException("CSV Source 必须配置 schema");
        }
        return schema.getFieldNames();
    }

    @Override
    public Iterable<Row> parse(SourceConfig localFileSourceConfig, InputStream inputStream, List<String> fields) {
        EtlSchema schema = localFileSourceConfig.getSchema();
        if (schema == null) {
            throw new SchemaConfigException("CSV Source 必须配置 schema");
        }

        String encoding = localFileSourceConfig.getString("encoding");
        Charset charset = encoding != null ? Charset.forName(encoding) : StandardCharsets.UTF_8;

        String delimiter = localFileSourceConfig.getString("delimiter");
        char delim = delimiter != null ? delimiter.charAt(0) : ',';

        // skipHeader: 是否跳过 CSV 第一行（默认 true）
        boolean skipHeader = localFileSourceConfig.getBoolean("skipHeader", true);

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setDelimiter(delim)
                .build();

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charset));
            CSVParser parser = csvFormat.parse(reader);

            return new CsvRowIterable(parser, schema, reader, inputStream, skipHeader);

        } catch (IOException e) {
            throw new RuntimeException("解析 CSV 文件失败: " + e.getMessage(), e);
        }
    }

    /**
     * CSV Row 迭代器封装
     * 确保在迭代完成后关闭输入流
     */
    private static class CsvRowIterable implements Iterable<Row> {

        private final CSVParser parser;
        private final EtlSchema schema;
        private final BufferedReader reader;
        private final InputStream inputStream;
        private final boolean skipHeader;
        private volatile boolean closed = false;

        CsvRowIterable(CSVParser parser, EtlSchema schema, BufferedReader reader,
                       InputStream inputStream, boolean skipHeader) {
            this.parser = parser;
            this.schema = schema;
            this.reader = reader;
            this.inputStream = inputStream;
            this.skipHeader = skipHeader;
        }

        @Override
        public Iterator<Row> iterator() {
            return new Iterator<Row>() {
                private final Iterator<CSVRecord> csvIterator = parser.iterator();
                private boolean headerSkipped = false;

                @Override
                public boolean hasNext() {
                    if (closed) {
                        return false;
                    }
                    // 跳过头部行（如果配置了 skipHeader=true）
                    if (skipHeader && !headerSkipped && csvIterator.hasNext()) {
                        csvIterator.next(); // 跳过头部
                        headerSkipped = true;
                    }
                    boolean hasNext = csvIterator.hasNext();
                    if (!hasNext) {
                        closeQuietly();
                    }
                    return hasNext;
                }

                @Override
                public Row next() {
                    CSVRecord record = csvIterator.next();
                    int schemaSize = schema.getFields().size();
                    int recordSize = record.size();
                    Row row = new Row(schemaSize);

                    for (int i = 0; i < schemaSize; i++) {
                        Object value;
                        if (i < recordSize) {
                            value = record.get(i);
                        } else {
                            log.warn("CSV 行缺少字段 '{}', 已设为 null", schema.getField(i).getName());
                            value = null;
                        }

                        EtlField field = schema.getField(i);
                        Object converted = TypeConverter.convert(value, field.getName(), field.getType());
                        row.setField(i, converted);
                    }

                    // 检查是否有多余列
                    if (recordSize > schemaSize) {
                        log.warn("CSV 行有 {} 个多余列被忽略", recordSize - schemaSize);
                    }

                    return row;
                }

                private void closeQuietly() {
                    if (closed) {
                        return;
                    }
                    closed = true;
                    try {
                        parser.close();
                    } catch (Exception e) {
                        log.warn("关闭 CSV 解析器失败", e);
                    }
                    try {
                        reader.close();
                    } catch (Exception e) {
                        log.warn("关闭 BufferedReader 失败", e);
                    }
                    try {
                        inputStream.close();
                    } catch (Exception e) {
                        log.warn("关闭输入流失败", e);
                    }
                }
            };
        }
    }
}
```

### JDBC Source

**要求**：schema 可选

**Schema 配置优先级**：
- **有 schema 配置**：使用配置的类型，忽略数据库元数据推断
- **无 schema 配置**：从 ResultSetMetaData 自动推断类型

**字段名匹配规则**（有 schema 配置时）：
- schema 中的字段名必须与数据库列名一致（或使用别名）
- 字段按位置匹配，不按名称匹配
- 建议配置 schema 时明确所有字段，避免遗漏

**MySQLDialect 改造**：

```java
@Override
public Row createRow(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    // 使用位置访问方式
    Row row = new Row(columnCount);
    for (int i = 1; i <= columnCount; i++) {
        row.setField(i - 1, rs.getObject(i));
    }
    return row;
}

// 新增：从 ResultSetMetaData 推断 EtlSchema
public EtlSchema inferSchema(ResultSetMetaData metaData) throws SQLException {
    List<EtlField> fields = new ArrayList<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
        String name = metaData.getColumnLabel(i);
        EtlFieldType type = inferFieldType(metaData.getColumnType(i));
        fields.add(new EtlField(name, type));
    }
    return new EtlSchema(fields);
}

private EtlFieldType inferFieldType(int sqlType) {
    switch (sqlType) {
        case Types.BIT:
        case Types.BOOLEAN:
            return EtlFieldType.BOOLEAN;
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
            return EtlFieldType.INT;
        case Types.BIGINT:
            return EtlFieldType.LONG;
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
            return EtlFieldType.DOUBLE;
        case Types.DECIMAL:
        case Types.NUMERIC:
            return EtlFieldType.DECIMAL;
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
            return EtlFieldType.TIMESTAMP;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return EtlFieldType.BYTES;
        default:
            return EtlFieldType.STRING;
    }
}
```

**JdbcSource 改造**：

```java
public class JdbcSource extends AbstractRangeSplitSource<Row> {

    public JdbcSource(SourceConfig localFileSourceConfig, JdbcDialect dialect) {
        super(localFileSourceConfig.getString("splitColumn"));
        // ... 其他初始化

        // 解析 schema（可选）
        this.schema = localFileSourceConfig.getSchema();
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
        // 如果没有配置 schema，尝试从数据库推断
        if (schema == null) {
            schema = inferSchemaFromDatabase();
        }
        // ... 其余逻辑
    }

    private EtlSchema inferSchemaFromDatabase() {
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(dialect.buildSampleQuery(table, sql))) {
            return dialect.inferSchema(rs.getMetaData());
        } catch (SQLException e) {
            throw new RuntimeException("从数据库推断 Schema 失败", e);
        }
    }
}
```

## 错误处理

### 类型转换失败

当类型转换失败时（如将 "abc" 转换为 int），抛出 `TypeConversionException`：

```java
public class TypeConversionException extends RuntimeException {
    private final String fieldName;
    private final String rawValue;
    private final EtlFieldType targetType;

    public TypeConversionException(String fieldName, String rawValue,
                                   EtlFieldType targetType, Throwable cause) {
        super(String.format("字段 '%s' 类型转换失败: 值 '%s' 无法转换为 %s",
              fieldName, rawValue, targetType), cause);
        this.fieldName = fieldName;
        this.rawValue = rawValue;
        this.targetType = targetType;
    }
}
```

### Schema 缺失

- CSV Source：抛出异常，提示必须配置 schema
- JDBC Source：自动从数据库元数据推断

## 测试策略

### 单元测试

1. **SchemaParser 测试**
   - 解析有效配置
   - 解析无效配置（缺少 name/type）
   - 解析未知类型
   - 缺少 fields 字段
   - fields 不是数组
   - 空字段列表

2. **TypeConverter 测试**
   - 各类型正常转换
   - null 值处理
   - 空字符串处理
   - 转换失败场景（非数字转 int、无效日期格式等）
   - 数值边界值测试（int 最大值/最小值）
   - 数值溢出测试
   - 布尔值多种格式（true/false/1/0/yes/no）
   - bytes 类型处理（字符串和字节数组输入）

3. **FlinkTypeConverter 测试**
   - EtlSchema 到 RowType 的正确映射
   - 所有类型的 LogicalType 验证

### 集成测试

1. **CSV Source 集成测试**
   - 配置 schema 读取 CSV 文件
   - 验证输出 Row 的字段类型
   - CSV 列数与 schema 字段数不一致
   - CSV 列数少于 schema 字段数
   - CSV 列数多于 schema 字段数
   - `skipHeader=true` 跳过头行
   - `skipHeader=false` 不跳过头行
   - schema 缺失时抛出异常

2. **JDBC Source 集成测试**
   - 无 schema 配置时自动推断
   - 有 schema 配置时使用配置类型
   - schema 字段名与数据库列名一致
   - 类型推断正确性验证

## 后续扩展

1. **timestamp 格式配置**
   - 支持自定义日期时间格式

2. **nullable 配置**
   - 支持 nullable 字段配置
   - null 值处理策略

3. **默认值配置**
   - 支持字段默认值

4. **复杂类型支持**
   - array、map 等复杂类型