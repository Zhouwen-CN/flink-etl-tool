package com.etl.connector.cdc.mysql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import lombok.Getter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * MySQL CDC 序列化器
 * 将 Debezium JSON 转换为带 RowKind 的 Row
 * 动态从数据库获取表 Schema
 */
public class MySqlCdcDeserializer implements DebeziumDeserializationSchema<Row> {

    private final String hostname;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final String table;

    @Getter
    private RowType rowType;  // Schema 信息（延迟初始化）

    private ObjectMapper objectMapper;  // 延迟初始化（不可序列化）

    private boolean initialized = false;  // 标记是否已初始化

    /**
     * 构造函数：接收数据库连接参数
     */
    public MySqlCdcDeserializer(
            String hostname, int port, String database,
            String username, String password, String table) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.table = table;
    }

    /**
     * 延迟初始化：第一次调用时从数据库获取表 Schema
     */
    private void initialize() throws Exception {
        if (initialized) {
            return;
        }

        // 初始化 ObjectMapper（不可序列化）
        objectMapper = new ObjectMapper();

        // 构建 JDBC URL（处理 H2 内存数据库特殊情况）
        String jdbcUrl;
        String schemaPattern;  // schema 名称（用于 getColumns 查询）
        String tableNamePattern;  // 表名匹配模式
        if (hostname.startsWith("mem:") || hostname.startsWith("file:")) {
            // H2 内存数据库 URL 格式：jdbc:h2:mem:testdb
            jdbcUrl = "jdbc:h2:" + hostname;
            // H2 默认 schema 是 PUBLIC
            schemaPattern = "PUBLIC";
            // H2 默认将表名转换为大写，使用大写表名匹配
            tableNamePattern = table.toUpperCase();
        } else {
            // MySQL URL 格式：jdbc:mysql://host:port/database
            jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + database;
            schemaPattern = database;
            tableNamePattern = table;
        }

        // 使用 try-with-resources 确保 Connection 和 ResultSet 自动关闭
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet columns = metaData.getColumns(null, schemaPattern, tableNamePattern, null)) {
                // 构建 RowType（只包含指定表的列）
                List<RowType.RowField> fields = new ArrayList<>();
                while (columns.next()) {
                    String tableNameFromRs = columns.getString("TABLE_NAME");
                    // 确保只处理指定表的列（getColumns 可能返回多个表）
                    if (!tableNameFromRs.equalsIgnoreCase(tableNamePattern)) {
                        continue;
                    }

                    String columnName = columns.getString("COLUMN_NAME");
                    String columnType = columns.getString("TYPE_NAME");
                    LogicalType logicalType = convertJdbcTypeToFlinkType(columnType);
                    fields.add(new RowType.RowField(columnName, logicalType));
                }

                rowType = new RowType(fields);
            }
        }

        // 验证 Schema 是否获取成功
        if (rowType.getFieldCount() == 0) {
            throw new IllegalStateException("表 '" + table + "' 不存在或没有字段");
        }

        initialized = true;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Row> out) throws Exception {
        // 延迟初始化（第一次调用时执行）
        initialize();

        // 从 SourceRecord 中提取 value（Debezium JSON）
        byte[] valueBytes = (byte[]) record.value();
        JsonNode jsonNode = objectMapper.readTree(valueBytes);

        // 验证必需字段 'op'
        if (!jsonNode.has("op")) {
            throw new IOException("Debezium JSON 缺少必需字段 'op'");
        }

        // 解析 Debezium op 字段
        String op = jsonNode.get("op").asText();
        RowKind rowKind = parseRowKind(op);

        // 提取业务数据（after/before 字段）并进行验证
        JsonNode dataNode;
        if (op.equals("d")) {
            // DELETE 操作使用 before 字段
            if (!jsonNode.has("before") || jsonNode.get("before").isNull()) {
                throw new IOException("DELETE 操作缺少 'before' 字段");
            }
            dataNode = jsonNode.get("before");
        } else {
            // INSERT/UPDATE 操作使用 after 字段
            if (!jsonNode.has("after") || jsonNode.get("after").isNull()) {
                throw new IOException("INSERT/UPDATE 操作缺少 'after' 字段");
            }
            dataNode = jsonNode.get("after");
        }

        // 构建 Row（带 RowKind）
        Row row = extractRow(dataNode, rowKind);

        // 发送到下游
        out.collect(row);
    }

    private RowKind parseRowKind(String op) {
        switch (op) {
            case "c":  // create
            case "r":  // read（快照读取）
                return RowKind.INSERT;
            case "u":  // update
                return RowKind.UPDATE_AFTER;
            case "d":  // delete
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException("不支持的 op 类型: " + op);
        }
    }

    private Row extractRow(JsonNode dataNode, RowKind rowKind) {
        int fieldCount = rowType.getFieldCount();
        Row row = Row.withPositions(rowKind, fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = rowType.getFieldNames().get(i);
            LogicalType fieldType = rowType.getTypeAt(i);

            JsonNode fieldValue = dataNode.get(fieldName);
            Object value = convertJsonNodeToValue(fieldValue, fieldType);
            row.setField(i, value);
        }

        return row;
    }

    private Object convertJsonNodeToValue(JsonNode node, LogicalType type) {
        if (node == null || node.isNull()) {
            return null;
        }

        // 根据 LogicalType 类型转换
        if (type instanceof IntType) {
            return node.asInt();
        } else if (type instanceof BigIntType) {
            return node.asLong();
        } else if (type instanceof VarCharType) {
            return node.asText();
        } else if (type instanceof DoubleType) {
            return node.asDouble();
        } else if (type instanceof DecimalType) {
            return new BigDecimal(node.asText());
        } else if (type instanceof TimestampType) {
            return Timestamp.valueOf(node.asText());
        } else if (type instanceof DateType) {
            return Date.valueOf(node.asText());
        } else if (type instanceof BooleanType) {
            return node.asBoolean();
        }

        throw new UnsupportedOperationException("不支持的字段类型: " + type);
    }

    private LogicalType convertJdbcTypeToFlinkType(String jdbcType) {
        // JDBC 类型 → Flink LogicalType 映射
        switch (jdbcType.toUpperCase()) {
            case "INT":
            case "INTEGER":
            case "SMALLINT":
            case "TINYINT":
                return new IntType();
            case "BIGINT":
                return new BigIntType();
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
            case "CHARACTER VARYING":
            case "CHARACTER":
            case "CLOB":
                return new VarCharType();
            case "DOUBLE":
            case "FLOAT":
            case "REAL":
            case "DOUBLE PRECISION":
                return new DoubleType();
            case "DECIMAL":
            case "NUMERIC":
                return new DecimalType();
            case "TIMESTAMP":
            case "DATETIME":
            case "TIMESTAMP WITH TIME ZONE":
                return new TimestampType();
            case "DATE":
                return new DateType();
            case "BOOLEAN":
            case "BIT":
            case "BOOL":
                return new BooleanType();
            default:
                throw new UnsupportedOperationException("不支持的 JDBC 类型: " + jdbcType);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}