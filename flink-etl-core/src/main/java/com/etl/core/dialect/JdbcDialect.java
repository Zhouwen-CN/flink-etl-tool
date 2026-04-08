package com.etl.core.dialect;

import java.io.Serializable;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JDBC 数据库方言接口
 * 提供数据库特定的 SQL 生成和 URL 处理能力
 */
public interface JdbcDialect extends Serializable {

    /**
     * 获取数据库类型标识
     * @return 类型标识，如 "mysql", "postgresql"
     */
    String getName();

    /**
     * 判断 URL 是否匹配此数据库类型
     * @param url JDBC 连接 URL
     * @return 是否匹配
     */
    boolean acceptsUrl(String url);

    /**
     * 包装 JDBC URL，添加必要的参数，比如mysql
     * @param url 原始 URL
     * @return 包装后的 URL
     */
    default String wrapUrl(String url) {
        return url;
    }

    /**
     * 转义 SQL 标识符
     * @param identifier 标识符名称
     * @return 转义后的标识符
     */
    String quoteIdentifier(String identifier);

    /**
     * 生成 INSERT SQL
     * @param table 表名
     * @param columns 列名数组
     * @return INSERT SQL
     */
    default String getInsertSql(String table, String[] columns) {
        String colList = Arrays.stream(columns)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = String.join(", ", Collections.nCopies(columns.length, "?"));

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                quoteIdentifier(table), colList, placeholders);
    }

    /**
     * 生成 UPSERT SQL（存在则更新，不存在则插入）
     * @param table 表名
     * @param columns 所有列名数组
     * @param keyFields 主键/唯一键字段列表
     * @return UPSERT SQL
     */
    String getUpsertSql(String table, String[] columns, List<String> keyFields);


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
    default int getColumnType(String url, String table, String sql, String columnName,
                              String username, String password) {
        // 构建查询语句
        String sampleQuery;
        if (table != null) {
            sampleQuery = "SELECT " + this.quoteIdentifier(columnName) + " FROM " + table + " WHERE 1=0";
        } else {
            sampleQuery = "SELECT " + this.quoteIdentifier(columnName) + " FROM (" + sql + ") AS t WHERE 1=0";
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