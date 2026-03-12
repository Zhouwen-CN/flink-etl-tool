package com.etl.source.jdbc.dialect;

import com.etl.source.jdbc.JdbcDialect;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * MySQL 数据库方言实现
 */
public class MySQLDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(MySQLDialect.class);

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
        logger.debug("构建范围查询 SQL: {}", query);
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
        logger.debug("构建分片查询 SQL: {}", query);
        return query;
    }

    @Override
    public Row createRow(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        String[] fieldNames = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            fieldNames[i - 1] = metaData.getColumnLabel(i);
        }

        Row row = Row.withNames();
        for (int i = 1; i <= columnCount; i++) {
            row.setField(fieldNames[i - 1], rs.getObject(i));
        }
        return row;
    }
}