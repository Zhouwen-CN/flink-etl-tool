package com.etl.connector.jdbc.utils;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * JDBC 分片工具类
 * 提供分片计算和 SQL 构建的静态方法
 */
@Slf4j
public final class JdbcSplitHelper {

    private JdbcSplitHelper() {
        // 工具类不允许实例化
    }

    private static final Map<Integer, String> JDBC_TYPE_NAMES = new HashMap<>();

    static {
        // 数值类型
        JDBC_TYPE_NAMES.put(Types.TINYINT, "TINYINT");
        JDBC_TYPE_NAMES.put(Types.SMALLINT, "SMALLINT");
        JDBC_TYPE_NAMES.put(Types.INTEGER, "INTEGER");
        JDBC_TYPE_NAMES.put(Types.BIGINT, "BIGINT");
        JDBC_TYPE_NAMES.put(Types.FLOAT, "FLOAT");
        JDBC_TYPE_NAMES.put(Types.REAL, "REAL");
        JDBC_TYPE_NAMES.put(Types.DOUBLE, "DOUBLE");
        JDBC_TYPE_NAMES.put(Types.NUMERIC, "NUMERIC");
        JDBC_TYPE_NAMES.put(Types.DECIMAL, "DECIMAL");
        // 字符串类型
        JDBC_TYPE_NAMES.put(Types.CHAR, "CHAR");
        JDBC_TYPE_NAMES.put(Types.VARCHAR, "VARCHAR");
        JDBC_TYPE_NAMES.put(Types.LONGVARCHAR, "LONGVARCHAR");
        JDBC_TYPE_NAMES.put(Types.NCHAR, "NCHAR");
        JDBC_TYPE_NAMES.put(Types.NVARCHAR, "NVARCHAR");
        // 日期类型
        JDBC_TYPE_NAMES.put(Types.DATE, "DATE");
        JDBC_TYPE_NAMES.put(Types.TIMESTAMP, "TIMESTAMP");
    }


    /**
     * 获取指定列的 JDBC 类型
     *
     * @param url        数据库连接 URL
     * @param table      表名（可能为 null）
     * @param sql        自定义 SQL（可能为 null）
     * @param columnName 列名
     * @param username   用户名
     * @param password   密码
     * @return JDBC 类型常量（来自 java.sql.Types）
     * @throws RuntimeException 如果列不存在或查询失败
     */
    public static int getColumnType(JdbcDialect dialect, String url, String table, String sql, String columnName,
                                    String username, String password) {
        // 构建查询语句
        String sampleQuery;
        if (table != null) {
            sampleQuery = "SELECT " + dialect.quoteIdentifier(columnName) + " FROM " + table + " WHERE 1=0";
        } else {
            sampleQuery = "SELECT " + dialect.quoteIdentifier(columnName) + " FROM (" + sql + ") AS t WHERE 1=0";
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


    /**
     * 获取 JDBC 类型名称（用于日志输出）
     *
     * @param jdbcType JDBC 类型常量
     * @return 类型名称
     */
    public static String getJdbcTypeName(int jdbcType) {
        return JDBC_TYPE_NAMES.getOrDefault(jdbcType, "未知类型(" + jdbcType + ")");
    }
}