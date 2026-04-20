package com.etl.connector.jdbc.source.utils;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
     * 从主键列中选择最优的可用分片列
     * 优先级：BIGINT > INTEGER > SMALLINT > TINYINT > DECIMAL/NUMERIC > FLOAT/REAL/DOUBLE
     *
     * @param primaryKeys 主键 Map<列名, JDBC类型>
     * @return 最优的 splitKey 列名，null 表示无可用类型
     */
    public static String selectOptimalSplitKey(Map<String, Integer> primaryKeys) {
        // 定义类型优先级（数值越大优先级越高）
        Map<Integer, Integer> typePriority = new HashMap<>();
        typePriority.put(java.sql.Types.BIGINT, 6);
        typePriority.put(java.sql.Types.INTEGER, 5);
        typePriority.put(java.sql.Types.SMALLINT, 4);
        typePriority.put(java.sql.Types.TINYINT, 3);
        typePriority.put(java.sql.Types.DECIMAL, 2);
        typePriority.put(java.sql.Types.NUMERIC, 2);
        typePriority.put(java.sql.Types.FLOAT, 1);
        typePriority.put(java.sql.Types.REAL, 1);
        typePriority.put(java.sql.Types.DOUBLE, 1);

        String selectedKey = null;
        int selectedPriority = 0;

        for (Map.Entry<String, Integer> entry : primaryKeys.entrySet()) {
            Integer priority = typePriority.get(entry.getValue());
            if (priority != null && priority > selectedPriority) {
                selectedKey = entry.getKey();
                selectedPriority = priority;
            }
        }

        return selectedKey;
    }

    /**
     * 获取 JDBC 类型名称（用于日志输出）
     *
     * @param jdbcType JDBC 类型常量
     * @return 类型名称
     */
    public static String getJdbcTypeName(int jdbcType) {
        switch (jdbcType) {
            case java.sql.Types.BIGINT: return "BIGINT";
            case java.sql.Types.INTEGER: return "INTEGER";
            case java.sql.Types.SMALLINT: return "SMALLINT";
            case java.sql.Types.TINYINT: return "TINYINT";
            case java.sql.Types.DECIMAL: return "DECIMAL";
            case java.sql.Types.NUMERIC: return "NUMERIC";
            case java.sql.Types.FLOAT: return "FLOAT";
            case java.sql.Types.REAL: return "REAL";
            case java.sql.Types.DOUBLE: return "DOUBLE";
            default: return String.valueOf(jdbcType);
        }
    }


}