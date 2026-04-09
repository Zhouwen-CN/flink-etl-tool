package com.etl.source.jdbc.utils;

import com.etl.core.dialect.JdbcDialect;
import com.etl.source.jdbc.RangeSplit;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
     * 计算数值范围分片（从数据库查询范围并计算分片）
     *
     * @param dialect     数据库方言
     * @param url         数据库连接 URL
     * @param username    用户名
     * @param password    密码
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param parallelism 并行度（期望的分片数量）
     * @return 分片列表
     */
    public static List<RangeSplit> calculateNumericSplits(JdbcDialect dialect, String url, String username, String password,
                                                          String table, String sql, String splitColumn,
                                                          int parallelism) {
        // 1. 查询分片列范围
        String rangeQuery = buildRangeQuery(
                dialect,
                table,
                sql,
                splitColumn
        );
        log.info("查询分片范围: {}", rangeQuery);

        long min = 0L;
        long max = 0L;
        boolean hasData = false;

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(rangeQuery)) {

            if (rs.next()) {
                // 检查是否为 NULL（空表时 MIN/MAX 返回 NULL）
                hasData = rs.getObject(1) != null;
                if (hasData) {
                    min = rs.getLong(1);
                    max = rs.getLong(2);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("获取分片范围失败: " + e.getMessage(), e);
        }

        // 空表检查
        if (!hasData) {
            log.warn("表为空，不创建分片");
            return new ArrayList<>();
        }

        log.info("分片列范围: [{}, {}]", min, max);

        // 2. 计算分片
        List<RangeSplit> splits = new ArrayList<>();

        if (min > max) {
            log.warn("数据范围为空，不创建分片");
            return splits;
        }

        long totalRecords = max - min + 1;
        int actualSplitCount = (int) Math.min(parallelism, totalRecords);

        if (actualSplitCount < parallelism) {
            log.info("数据量({})小于并行度({})，实际分片数调整为 {}",
                    totalRecords, parallelism, actualSplitCount);
        }

        long splitSize = (totalRecords + actualSplitCount - 1) / actualSplitCount;

        long currentStart = min;
        for (int i = 0; i < actualSplitCount && currentStart <= max; i++) {
            long currentEnd = Math.min(currentStart + splitSize - 1, max);

            // 生成该分片的查询 SQL
            String querySql = buildSplitQuery(dialect, table, sql, splitColumn, currentStart, currentEnd);
            String splitId = splitColumn + "_" + currentStart + "_" + currentEnd;
            splits.add(new RangeSplit(splitId, querySql));

            currentStart = currentEnd + 1;
        }

        log.info("共计算出 {} 个分片", splits.size());
        return splits;
    }

    /**
     * 构建范围查询 SQL（获取分片列的 MIN 和 MAX 值）
     *
     * @param dialect     数据库方言
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @return 查询 SQL
     */
    private static String buildRangeQuery(JdbcDialect dialect, String table, String sql, String splitColumn) {
        splitColumn = dialect.quoteIdentifier(splitColumn);
        if (table != null) {
            table = dialect.quoteIdentifier(table);
            return String.format("SELECT MIN(%s), MAX(%s) FROM %s", splitColumn, splitColumn, table);
        } else {
            return String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t", splitColumn, splitColumn, sql);
        }
    }

    /**
     * 构建分片数据查询 SQL
     *
     * @param dialect     数据库方言
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param start       起始值
     * @param end         结束值
     * @return 查询 SQL
     */
    public static String buildSplitQuery(JdbcDialect dialect, String table, String sql, String splitColumn, long start, long end) {
        splitColumn = dialect.quoteIdentifier(splitColumn);
        if (table != null) {
            table = dialect.quoteIdentifier(table);
            return String.format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d", table, splitColumn, start, end);
        } else {
            return String.format("SELECT * FROM (%s) AS t WHERE %s BETWEEN %d AND %d", sql, splitColumn, start, end);
        }
    }

    /**
     * 创建全表扫描分片
     *
     * @param dialect 数据库方言
     * @param table   表名（可能为 null）
     * @param sql     自定义 SQL（可能为 null）
     * @return 包含单个全表扫描分片的列表
     */
    public static List<RangeSplit> createFullTableScanSplits(JdbcDialect dialect, String table, String sql) {
        String querySql;
        if (table != null) {
            querySql = "SELECT * FROM " + dialect.quoteIdentifier(table);
        } else {
            querySql = "SELECT * FROM (" + sql + ") AS t";
        }
        return Collections.singletonList(new RangeSplit("full_table_scan", querySql));
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
        typePriority.put(Types.BIGINT, 6);
        typePriority.put(Types.INTEGER, 5);
        typePriority.put(Types.SMALLINT, 4);
        typePriority.put(Types.TINYINT, 3);
        typePriority.put(Types.DECIMAL, 2);
        typePriority.put(Types.NUMERIC, 2);
        typePriority.put(Types.FLOAT, 1);
        typePriority.put(Types.REAL, 1);
        typePriority.put(Types.DOUBLE, 1);

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
            case Types.BIGINT: return "BIGINT";
            case Types.INTEGER: return "INTEGER";
            case Types.SMALLINT: return "SMALLINT";
            case Types.TINYINT: return "TINYINT";
            case Types.DECIMAL: return "DECIMAL";
            case Types.NUMERIC: return "NUMERIC";
            case Types.FLOAT: return "FLOAT";
            case Types.REAL: return "REAL";
            case Types.DOUBLE: return "DOUBLE";
            default: return String.valueOf(jdbcType);
        }
    }
}