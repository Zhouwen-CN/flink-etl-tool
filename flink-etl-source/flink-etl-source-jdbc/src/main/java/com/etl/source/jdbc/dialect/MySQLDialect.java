package com.etl.source.jdbc.dialect;

import com.etl.source.jdbc.JdbcDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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

        // 使用位置访问方式创建 Row（兼容性好）
        Row row = new Row(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            row.setField(i - 1, rs.getObject(i));
        }
        return row;
    }

    @Override
    public String buildSampleQuery(String table, String sql) {
        if (table != null) {
            return "SELECT * FROM " + quoteIdentifier(table) + " WHERE 1=0";
        } else {
            return "SELECT * FROM (" + sql + ") AS t WHERE 1=0";
        }
    }
}