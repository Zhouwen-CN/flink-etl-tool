package com.etl.connector.jdbc.dialect;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JDBC 数据库方言接口
 * 提供数据库特定的 SQL 生成和 URL 处理能力
 * 因为 jdbc source 和 jdbc sink都需要，所以放在了 core 模块
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
     * 生成 UPSERT SQL（存在则更新，不存在则插入）
     * @param table 表名
     * @param columns 所有列名数组
     * @param keyFields 主键/唯一键字段列表
     * @return UPSERT SQL
     */
    String getUpsertSql(String table, String[] columns, List<String> keyFields);

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
     * 生成 DELETE SQL
     * @param table 表名
     * @param keyFields 主键/唯一键字段列表（用于 WHERE 条件）
     * @return DELETE SQL
     */
    default String getDeleteSql(String table, List<String> keyFields){
        // DELETE FROM table WHERE key1=? AND key2=?

        String whereClause = keyFields.stream()
                .map(key -> quoteIdentifier(key) + " = ?")
                .collect(Collectors.joining(" AND "));

        return String.format("DELETE FROM %s WHERE %s",
                quoteIdentifier(table), whereClause);
    }

    /**
     * 生成字符串列的 hash mod 表达式
     *
     * @param columnName 列名（已转义）
     * @param modulus 模数（分片数量）
     * @return hash mod 表达式（如 "MD5(column) % 4"）
     */
    String hashModExpression(String columnName, int modulus);

    /**
     * 构建日期范围查询 SQL（开区间）
     * 默认实现使用字符串字面量拼接，Oracle 等需要特殊日期函数的方言应覆盖此方法
     *
     * @param baseQuery 基础查询（SELECT * FROM table）
     * @param columnName 列名（已转义）
     * @param startDate 起始日期（null 表示第一个分片）
     * @param endDate 结束日期（null 表示最后一个分片）
     * @return 完整查询 SQL（使用 >= AND < 开区间）
     */
    default String buildDateRangeQuery(String baseQuery, String columnName,
                                String startDate, String endDate) {
        if (startDate == null && endDate == null) {
            return baseQuery;
        } else if (startDate == null) {
            return String.format("%s WHERE %s < '%s'", baseQuery, columnName, endDate);
        } else if (endDate == null) {
            return String.format("%s WHERE %s >= '%s'", baseQuery, columnName, startDate);
        } else {
            return String.format("%s WHERE %s >= '%s' AND %s < '%s'",
                baseQuery, columnName, startDate, columnName, endDate);
        }
    }
}