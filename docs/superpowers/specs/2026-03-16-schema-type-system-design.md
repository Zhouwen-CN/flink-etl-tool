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
    "config": {
      "path": "/data/users.csv",
      "format": "csv",
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

        Map<String, Object> schemaMap = (Map<String, Object>) schemaConfig;
        List<Map<String, Object>> fieldsConfig = (List<Map<String, Object>>) schemaMap.get("fields");

        List<EtlField> fields = fieldsConfig.stream()
            .map(f -> new EtlField(
                (String) f.get("name"),
                EtlFieldType.fromString((String) f.get("type"))
            ))
            .collect(Collectors.toList());

        return new EtlSchema(fields);
    }
}
```

#### TypeConverter

```java
public class TypeConverter {
    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static Object convert(Object value, EtlFieldType targetType) {
        if (value == null) {
            return null;
        }

        String strValue = String.valueOf(value).trim();
        if (strValue.isEmpty()) {
            return null;
        }

        switch (targetType) {
            case STRING:
                return strValue;
            case BOOLEAN:
                return Boolean.parseBoolean(strValue);
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
                return strValue.getBytes(StandardCharsets.UTF_8);
            default:
                throw new IllegalArgumentException("不支持的类型: " + targetType);
        }
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
    if (config == null) {
        return null;
    }
    return SchemaParser.parse(config.get("schema"));
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

### LocalFile Source (CSV)

**要求**：必须配置 schema

**CsvFormatPlugin 改造**：

```java
@Override
public Iterable<Row> parse(SourceConfig config, InputStream inputStream, List<String> fields) {
    EtlSchema schema = config.getSchema();
    if (schema == null) {
        throw new RuntimeException("CSV Source 必须配置 schema");
    }

    // ... 解析逻辑

    @Override
    public Row next() {
        CSVRecord record = csvIterator.next();
        Row row = new Row(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            String value = i < record.size() ? record.get(i) : null;
            EtlField field = schema.getField(i);
            Object converted = TypeConverter.convert(value, field.getType());
            row.setField(i, converted);
        }

        return row;
    }
}
```

### JDBC Source

**要求**：schema 可选

**无 schema 时**：从 ResultSetMetaData 自动推断类型

**MySQLDialect 改造**：

```java
@Override
public Row createRow(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    Row row = Row.withNames();
    for (int i = 1; i <= columnCount; i++) {
        String fieldName = metaData.getColumnLabel(i);
        row.setField(fieldName, rs.getObject(i));
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

2. **TypeConverter 测试**
   - 各类型正常转换
   - null 值处理
   - 空字符串处理
   - 转换失败场景

3. **FlinkTypeConverter 测试**
   - EtlSchema 到 RowType 的正确映射

### 集成测试

1. **CSV Source 集成测试**
   - 配置 schema 读取 CSV 文件
   - 验证输出 Row 的字段类型

2. **JDBC Source 集成测试**
   - 无 schema 配置时自动推断
   - 有 schema 配置时使用配置类型

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