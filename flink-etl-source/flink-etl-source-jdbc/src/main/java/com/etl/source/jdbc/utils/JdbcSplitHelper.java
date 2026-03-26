package com.etl.source.jdbc.utils;

import com.etl.core.utils.SqlUtils;
import com.etl.source.jdbc.RangeSplit;
import com.etl.source.jdbc.SplitStrategy;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
     * @param url         数据库连接 URL
     * @param username    用户名
     * @param password    密码
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param parallelism 并行度（期望的分片数量）
     * @return 分片列表
     */
    public static List<RangeSplit> calculateNumericSplits(String url, String username, String password,
                                                          String table, String sql, String splitColumn,
                                                          int parallelism) {
        // 1. 查询分片列范围
        String rangeQuery = buildRangeQuery(
                url,
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
            String querySql = buildSplitQuery(url, table, sql, splitColumn, currentStart, currentEnd);
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
     * @param url         数据库连接 url
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @return 查询 SQL
     */
    private static String buildRangeQuery(String url, String table, String sql, String splitColumn) {
        splitColumn = SqlUtils.quoteIdentifier(splitColumn, url);
        if (table != null) {
            table = SqlUtils.quoteIdentifier(table, url);
            return String.format("SELECT MIN(%s), MAX(%s) FROM %s", splitColumn, splitColumn, table);
        } else {
            return String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t", splitColumn, splitColumn, sql);
        }
    }

    /**
     * 构建分片数据查询 SQL
     *
     * @param url         数据库连接 url
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param start       起始值
     * @param end         结束值
     * @return 查询 SQL
     */
    public static String buildSplitQuery(String url, String table, String sql, String splitColumn, long start, long end) {
        splitColumn = SqlUtils.quoteIdentifier(splitColumn, url);
        if (table != null) {
            table = SqlUtils.quoteIdentifier(table, url);
            return String.format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d", table, splitColumn, start, end);
        } else {
            return String.format("SELECT * FROM (%s) AS t WHERE %s BETWEEN %d AND %d", sql, splitColumn, start, end);
        }
    }

    /**
     * 创建全表扫描分片
     *
     * @param url    数据库连接 URL（用于标识符转义）
     * @param table  表名（可能为 null）
     * @param sql    自定义 SQL（可能为 null）
     * @return 包含单个全表扫描分片的列表
     */
    public static List<RangeSplit> createFullTableScanSplits(String url, String table, String sql) {
        String querySql;
        if (table != null) {
            querySql = "SELECT * FROM " + SqlUtils.quoteIdentifier(table, url);
        } else {
            querySql = "SELECT * FROM (" + sql + ") AS t";
        }
        return Collections.singletonList(new RangeSplit("full_table_scan", querySql));
    }

    /**
     * 校验分片列类型是否支持分片
     *
     * @param url         数据库连接 URL
     * @param username    用户名
     * @param password    密码
     * @param table       表名（可能为 null）
     * @param sql         自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param strategy    分片策略
     * @throws IllegalArgumentException 如果分片列类型不支持
     */
    public static void validateSplitColumnType(String url, String username, String password,
                                                String table, String sql, String splitColumn,
                                                SplitStrategy strategy) {
        int jdbcType = SqlUtils.getColumnType(table, sql, splitColumn, url, username, password);

        if (!strategy.supports(jdbcType)) {
            throw new IllegalArgumentException(
                    String.format("分片列 '%s' 的类型不支持 %s。支持的类型: %s",
                            splitColumn,
                            strategy.getDescription(),
                            strategy.getSupportedTypeNames())
            );
        }

        log.info("分片列 '{}' 类型校验通过，使用策略: {}", splitColumn, strategy.getDescription());
    }
}