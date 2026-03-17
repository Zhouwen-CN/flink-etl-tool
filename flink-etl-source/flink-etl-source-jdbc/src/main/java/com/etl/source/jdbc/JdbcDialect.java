package com.etl.source.jdbc;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * JDBC 数据库方言接口
 * 定义数据库特定的 SQL 构建和类型转换方法
 */
public interface JdbcDialect extends Serializable {

    /**
     * 获取 JDBC 驱动类名
     *
     * @return 驱动类名
     */
    String getDriverClassName();

    /**
     * 构建分片范围查询 SQL
     * 用于查询分片列的 MIN 和 MAX 值
     *
     * @param table 表名（可能为 null）
     * @param sql 自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @return 查询 SQL
     */
    String buildRangeQuery(String table, String sql, String splitColumn);

    /**
     * 构建分片数据查询 SQL
     *
     * @param table 表名（可能为 null）
     * @param sql 自定义 SQL（可能为 null）
     * @param splitColumn 分片列名
     * @param start 起始值
     * @param end 结束值
     * @return 查询 SQL
     */
    String buildSplitQuery(String table, String sql, String splitColumn, long start, long end);

    /**
     * 从 ResultSet 创建 Flink Row
     * 处理数据库特定的类型转换
     *
     * @param rs ResultSet
     * @return Flink Row
     * @throws SQLException SQL 异常
     */
    Row createRow(ResultSet rs) throws SQLException;

    /**
     * 从 ResultSetMetaData 推断 Schema
     *
     * @param metaData ResultSet 元数据
     * @return 推断的 EtlSchema
     * @throws SQLException SQL 异常
     */
    EtlSchema inferSchema(ResultSetMetaData metaData) throws SQLException;

    /**
     * 构建示例查询（用于推断 Schema）
     *
     * @param table 表名（可能为 null）
     * @param sql 自定义 SQL（可能为 null）
     * @return 示例查询 SQL，返回空结果集
     */
    String buildSampleQuery(String table, String sql);
}