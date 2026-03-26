package com.etl.core.utils;

import com.etl.core.schema.TypeConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * SQL 工具类
 * 提供标识符转义、Schema 推断等通用功能
 */
public final class SqlUtils {
    private SqlUtils() {}

    /**
     * 转义 SQL 标识符，防止 SQL 注入
     * 支持常见数据库：MySQL(`), PostgreSQL/SQLite("), SQL Server([)
     *
     * @param name 标识符名称
     * @param jdbcUrl JDBC 连接 URL
     * @return 转义后的标识符
     */
    public static String quoteIdentifier(String name, String jdbcUrl) {
        if (jdbcUrl.contains(":mysql")) {
            return "`" + name + "`";
        } else if (jdbcUrl.contains(":postgresql") || jdbcUrl.contains(":sqlite")) {
            return "\"" + name + "\"";
        } else if (jdbcUrl.contains(":sqlserver") || jdbcUrl.contains(":microsoft")) {
            return "[" + name + "]";
        }
        // 默认使用双引号（SQL 标准）
        return "\"" + name + "\"";
    }


    /**
     * 根据 metadata 构建 flink type
     */
    public static TypeInformation<Row> inferRowType(
            String table,
            String sql,
            String url,
            String username,
            String password
    ) {
        String sampleQuery;
        if (table != null) {
            sampleQuery = "SELECT * FROM " + table + " WHERE 1=0";
        } else {
            sampleQuery = "SELECT * FROM (" + sql + ") AS t WHERE 1=0";
        }

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleQuery)) {

            // 从 ResultSetMetaData 推断类型
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            String[] names = new String[columnCount];
            TypeInformation<?>[] types = new TypeInformation<?>[columnCount];

            for (int i = 1; i <= columnCount; i++) {
                int index = i - 1;
                names[index] = metaData.getColumnLabel(i);
                types[index] = TypeConverter.fromSqlType(metaData.getColumnType(i));
            }

            return Types.ROW_NAMED(names, types);
        } catch (SQLException e) {
            throw new RuntimeException("从数据库推断 Schema 失败: " + e.getMessage(), e);
        }
    }


    /**
     * 根据表名和字段名，生成插入语句
     *
     * @param url     数据库连接 url
     * @param table   表名
     * @param columns 字段名
     * @return insert sql
     */
    public static String getInsertSql(String url, String table, String[] columns) {
        String colList = Arrays.stream(columns)
                .map(c -> SqlUtils.quoteIdentifier(c, url))
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                SqlUtils.quoteIdentifier(table, url), colList, placeholders);
    }

    /**
     * 获取指定列的 JDBC 类型
     *
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param columnName  列名
     * @param url         数据库连接 URL
     * @param username    用户名
     * @param password    密码
     * @return JDBC 类型常量（来自 java.sql.Types）
     * @throws RuntimeException 如果列不存在或查询失败
     */
    public static int getColumnType(String table, String sql, String columnName,
                                    String url, String username, String password) {
        // 构建查询语句
        String sampleQuery;
        if (table != null) {
            sampleQuery = "SELECT " + quoteIdentifier(columnName, url) + " FROM " + table + " WHERE 1=0";
        } else {
            sampleQuery = "SELECT " + quoteIdentifier(columnName, url) + " FROM (" + sql + ") AS t WHERE 1=0";
        }

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleQuery)) {

            ResultSetMetaData metaData = rs.getMetaData();
            if (metaData.getColumnCount() < 1) {
                throw new RuntimeException("无法获取列 '" + columnName + "' 的类型信息");
            }
            return metaData.getColumnType(1);

        } catch (SQLException e) {
            throw new RuntimeException("获取列 '" + columnName + "' 的类型失败: " + e.getMessage(), e);
        }
    }
}