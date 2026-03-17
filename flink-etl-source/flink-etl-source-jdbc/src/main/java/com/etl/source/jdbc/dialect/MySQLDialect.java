package com.etl.source.jdbc.dialect;

import com.etl.core.schema.EtlField;
import com.etl.core.schema.EtlFieldType;
import com.etl.core.schema.EtlSchema;
import com.etl.source.jdbc.JdbcDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * MySQL 数据库方言实现
 */
@Slf4j
public class MySQLDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    /** 用反引号包裹标识符并转义内部的反引号，防止 SQL 注入 */
    private static String quoteIdentifier(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    @Override
    public String buildRangeQuery(String table, String sql, String splitColumn) {
        String quotedColumn = quoteIdentifier(splitColumn);
        String query;
        if (table != null) {
            // 表名模式
            query = String.format("SELECT MIN(%s), MAX(%s) FROM %s",
                    quotedColumn, quotedColumn, quoteIdentifier(table));
        } else {
            // 自定义 SQL 模式
            query = String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t",
                    quotedColumn, quotedColumn, sql);
        }
        log.debug("构建范围查询 SQL: {}", query);
        return query;
    }

    @Override
    public String buildSplitQuery(String table, String sql, String splitColumn, long start, long end) {
        String quotedColumn = quoteIdentifier(splitColumn);
        String query;
        if (table != null) {
            // 表名模式
            query = String.format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d",
                    quoteIdentifier(table), quotedColumn, start, end);
        } else {
            // 自定义 SQL 模式
            query = String.format("SELECT * FROM (%s) AS t WHERE %s BETWEEN %d AND %d",
                    sql, quotedColumn, start, end);
        }
        log.debug("构建分片查询 SQL: {}", query);
        return query;
    }

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

    @Override
    public EtlSchema inferSchema(ResultSetMetaData metaData) throws SQLException {
        List<EtlField> fields = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String name = metaData.getColumnLabel(i);
            EtlFieldType type = inferFieldType(metaData.getColumnType(i));
            fields.add(new EtlField(name, type));
        }
        EtlSchema schema = new EtlSchema();
        schema.setFields(fields);
        return schema;
    }

    @Override
    public String buildSampleQuery(String table, String sql) {
        if (table != null) {
            return "SELECT * FROM " + quoteIdentifier(table) + " WHERE 1=0";
        } else {
            return "SELECT * FROM (" + sql + ") AS t WHERE 1=0";
        }
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
}