package com.etl.connector.jdbc.utils;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.core.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * JDBC 分片工具类
 * 提供分片计算和 JDBC 类型查询的静态方法
 */
@Slf4j
public final class JdbcSplitHelper {

    private static final Map<Integer, String> JDBC_TYPE_NAMES = new HashMap<>();

    static {
        JDBC_TYPE_NAMES.put(Types.TINYINT, "TINYINT");
        JDBC_TYPE_NAMES.put(Types.SMALLINT, "SMALLINT");
        JDBC_TYPE_NAMES.put(Types.INTEGER, "INTEGER");
        JDBC_TYPE_NAMES.put(Types.BIGINT, "BIGINT");
        JDBC_TYPE_NAMES.put(Types.FLOAT, "FLOAT");
        JDBC_TYPE_NAMES.put(Types.REAL, "REAL");
        JDBC_TYPE_NAMES.put(Types.DOUBLE, "DOUBLE");
        JDBC_TYPE_NAMES.put(Types.NUMERIC, "NUMERIC");
        JDBC_TYPE_NAMES.put(Types.DECIMAL, "DECIMAL");
        JDBC_TYPE_NAMES.put(Types.CHAR, "CHAR");
        JDBC_TYPE_NAMES.put(Types.VARCHAR, "VARCHAR");
        JDBC_TYPE_NAMES.put(Types.LONGVARCHAR, "LONGVARCHAR");
        JDBC_TYPE_NAMES.put(Types.NCHAR, "NCHAR");
        JDBC_TYPE_NAMES.put(Types.NVARCHAR, "NVARCHAR");
        JDBC_TYPE_NAMES.put(Types.DATE, "DATE");
        JDBC_TYPE_NAMES.put(Types.TIMESTAMP, "TIMESTAMP");
    }

    private JdbcSplitHelper() {
        // 工具类不允许实例化
    }

    /**
     * 获取指定列的 JDBC 类型
     * 委托 JdbcUtil 执行，由调用方负责标识符转义
     */
    public static int getColumnType(JdbcDialect dialect, String url, String table, String sql, String columnName,
                                    String username, String password) {
        // 构建查询语句（需要转义标识符）
        String sampleQuery;
        if (table != null) {
            sampleQuery = "SELECT " + dialect.quoteIdentifier(columnName)
                    + " FROM " + dialect.quoteIdentifier(table) + " WHERE 1=0";
        } else {
            sampleQuery = "SELECT " + dialect.quoteIdentifier(columnName)
                    + " FROM (" + sql + ") AS t WHERE 1=0";
        }

        return JdbcUtil.getColumnType(
                dialect.driverClassName(), url, sampleQuery, username, password, columnName);
    }

    /**
     * 获取 JDBC 类型名称（用于日志输出）
     */
    public static String getJdbcTypeName(int jdbcType) {
        return JDBC_TYPE_NAMES.getOrDefault(jdbcType, "未知类型(" + jdbcType + ")");
    }
}