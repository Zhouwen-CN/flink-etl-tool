package com.etl.connector.jdbc.utils;

import com.etl.core.exception.NoPrimaryKeyException;
import com.etl.core.schema.SqlTypeConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL 工具类
 * 提供标识符转义、Schema 推断等通用功能
 */
public final class SqlUtils {
    private SqlUtils() {
    }

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
     * 从数据库获取表的主键信息
     *
     * @param url      数据库连接 URL
     * @param table    表名
     * @param username 用户名（可为 null）
     * @param password 密码（可为 null）
     * @return List<Pair<String, Integer>>，使用 List 保证复合主键按 KEY_SEQ 顺序排列
     * @throws NoPrimaryKeyException 如果表没有主键
     * @throws RuntimeException      如果获取主键失败
     */
    public static List<Pair<String, Integer>> getPrimaryKey(
            String url, String table, String username, String password) {

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            // 自动获取 catalog 和 schema
            String catalog = conn.getCatalog();
            String schema = conn.getSchema();

            DatabaseMetaData metaData = conn.getMetaData();

            // 按 KEY_SEQ 收集主键列名
            Map<Integer, String> keySeqColumnName = new HashMap<>();
            try(ResultSet rs = metaData.getPrimaryKeys(catalog, schema, table);) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    int keySeq = rs.getInt("KEY_SEQ");
                    Preconditions.checkState(!keySeqColumnName.containsKey(keySeq - 1), "The field(s) of primary key must be from the same table.");
                    keySeqColumnName.put(keySeq - 1, columnName);
                }
            }

            if (keySeqColumnName.isEmpty()) {
                throw new NoPrimaryKeyException(table);
            }

            List<Pair<String, Integer>> primaryKeyList = new ArrayList<>();

            for (Map.Entry<Integer, String> entry : keySeqColumnName.entrySet()) {
                Integer index = entry.getKey();
                String columnName = entry.getValue();

                // 使用 DatabaseMetaData.getColumns() 获取列类型
                try (ResultSet colRs = metaData.getColumns(catalog, schema, table, columnName)) {
                    if (colRs.next()) {
                        int jdbcType = colRs.getInt("DATA_TYPE");
                        primaryKeyList.set(index, Pair.of(columnName, jdbcType));
                    } else {
                        throw new RuntimeException(
                                String.format("无法获取表 '%s' 列 '%s' 的类型信息", table, columnName));
                    }
                }
            }

            return primaryKeyList;
        } catch (SQLException e) {
            throw new RuntimeException("从数据库获取主键失败: " + e.getMessage(), e);
        }
    }
}