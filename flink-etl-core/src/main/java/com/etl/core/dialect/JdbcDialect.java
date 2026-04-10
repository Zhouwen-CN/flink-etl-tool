package com.etl.core.dialect;

import java.io.Serializable;
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
     * 生成 UPDATE SQL
     * @param table 表名
     * @param columns 所有列名数组
     * @param keyFields 主键/唯一键字段列表（用于 WHERE 条件）
     * @return UPDATE SQL
     */
    default String getUpdateSql(String table, String[] columns, List<String> keyFields){
        // UPDATE table SET col1=?, col2=? WHERE key1=? AND key2=?

        String setClause = Arrays.stream(columns)
                .filter(col -> !keyFields.contains(col))
                .map(col -> quoteIdentifier(col) + " = ?")
                .collect(Collectors.joining(", "));

        String whereClause = keyFields.stream()
                .map(key -> quoteIdentifier(key) + " = ?")
                .collect(Collectors.joining(" AND "));

        return String.format("UPDATE %s SET %s WHERE %s",
                quoteIdentifier(table), setClause, whereClause);
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
}