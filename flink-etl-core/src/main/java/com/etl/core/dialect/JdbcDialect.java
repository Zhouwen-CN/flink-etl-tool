package com.etl.core.dialect;

import java.io.Serializable;
import java.util.List;

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
     * 包装 JDBC URL，添加必要的参数
     * @param url 原始 URL
     * @return 包装后的 URL
     */
    String wrapUrl(String url);

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
    String getInsertSql(String table, String[] columns);

    /**
     * 生成 UPSERT SQL（存在则更新，不存在则插入）
     * @param table 表名
     * @param columns 所有列名数组
     * @param keyFields 主键/唯一键字段列表
     * @return UPSERT SQL
     */
    String getUpsertSql(String table, String[] columns, List<String> keyFields);

    /**
     * 是否支持 UPSERT
     * @return 是否支持
     */
    default boolean supportsUpsert() {
        return true;
    }
}