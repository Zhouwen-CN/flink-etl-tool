package com.etl.core.utils;

import com.etl.core.dialect.JdbcDialect;
import com.etl.core.schema.SqlTypeConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SQL 工具类
 * 提供标识符转义、Schema 推断等通用功能
 */
public final class SqlUtils {
    private SqlUtils() {}

    /**
     * 根据 metadata 构建 flink type
     */
    public static TypeInformation<Row> inferRowType(
            String table,
            String sql,
            String url,
            String username,
            String password
    ) {
        String sampleQuery;
        if (table != null) {
            sampleQuery = "SELECT * FROM " + table + " WHERE 1=0";
        } else {
            sampleQuery = "SELECT * FROM (" + sql + ") AS t WHERE 1=0";
        }

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleQuery)) {

            // 从 ResultSetMetaData 推断类型
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            String[] names = new String[columnCount];
            TypeInformation<?>[] types = new TypeInformation<?>[columnCount];

            for (int i = 1; i <= columnCount; i++) {
                int index = i - 1;
                names[index] = metaData.getColumnLabel(i);
                types[index] = SqlTypeConverter.fromSqlType(metaData.getColumnType(i));
            }

            return Types.ROW_NAMED(names, types);
        } catch (SQLException e) {
            throw new RuntimeException("从数据库推断 Schema 失败: " + e.getMessage(), e);
        }
    }


    /**
     * 获取指定列的 JDBC 类型
     *
     * @param dialect    数据库方言
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
     * 从数据库获取表的主键信息
     *
     * @param url      数据库连接 URL
     * @param table    表名
     * @param username 用户名（可为 null）
     * @param password 密码（可为 null）
     * @return LinkedHashMap<列名, JDBC类型>，按 KEY_SEQ 顺序排列
     * @throws RuntimeException 如果表没有主键或获取失败
     */
    public static LinkedHashMap<String, Integer> getPrimaryKey(
            String url, String table, String username, String password) {

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            // 自动获取 catalog 和 schema
            String catalog = conn.getCatalog();
            String schema = conn.getSchema();

            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getPrimaryKeys(catalog, schema, table);

            // 按 KEY_SEQ 收集主键列名
            Map<Integer, String> keySeqColumnName = new HashMap<>();
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int keySeq = rs.getInt("KEY_SEQ");
                keySeqColumnName.put(keySeq - 1, columnName); // KEY_SEQ 是 1-based
            }
            rs.close();

            // 构建有序的 LinkedHashMap
            LinkedHashMap<String, Integer> result = new LinkedHashMap<>();
            if (keySeqColumnName.isEmpty()) {
                throw new RuntimeException(
                    String.format("表 '%s' 没有主键，无法使用 UPSERT 模式。请使用 INSERT 模式或为表添加主键", table));
            }

            // 获取每个主键列的 JDBC 类型
            for (int i = 0; i < keySeqColumnName.size(); i++) {
                String columnName = keySeqColumnName.get(i);

                // 使用 DatabaseMetaData.getColumns() 获取列类型
                ResultSet colRs = metaData.getColumns(catalog, schema, table, columnName);
                if (colRs.next()) {
                    int jdbcType = colRs.getInt("DATA_TYPE");
                    result.put(columnName, jdbcType);
                } else {
                    throw new RuntimeException(
                        String.format("无法获取表 '%s' 列 '%s' 的类型信息", table, columnName));
                }
                colRs.close();
            }

            return result;

        } catch (SQLException e) {
            throw new RuntimeException("从数据库获取主键失败: " + e.getMessage(), e);
        }
    }
}