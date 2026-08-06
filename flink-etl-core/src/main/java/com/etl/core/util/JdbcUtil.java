package com.etl.core.util;

import com.etl.core.exception.NoPrimaryKeyException;
import com.etl.core.schema.convert.SqlTypeConverter;
import lombok.extern.slf4j.Slf4j;
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
 * JDBC 通用工具类
 * 提供连接创建和查询模板方法，消除各处的 DriverManager 样板代码
 */
@Slf4j
public final class JdbcUtil {

    private JdbcUtil() {
    }

    /**
     * 创建数据库连接
     * 自动加载驱动类（如果指定）
     *
     * @param url         JDBC URL
     * @param username    用户名（可为 null）
     * @param password    密码（可为 null）
     * @param driverClass 驱动类名（可为 null，不加载）
     * @return 数据库连接
     */
    public static Connection getConnection(String url, String username, String password, String driverClass) {
        try {
            if (driverClass != null && !driverClass.isEmpty()) {
                Class.forName(driverClass);
            }
            Connection conn = (username != null && !username.isEmpty())
                    ? DriverManager.getConnection(url, username, password)
                    : DriverManager.getConnection(url);
            log.debug("JDBC 连接创建成功: url={}", url);
            return conn;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC 驱动加载失败: " + driverClass, e);
        } catch (SQLException e) {
            throw new RuntimeException("JDBC 连接创建失败: " + url, e);
        }
    }

    /**
     * 创建数据库连接（无需驱动类）
     */
    public static Connection getConnection(String url, String username, String password) {
        return getConnection(url, username, password, null);
    }

    /**
     * 执行查询的模板方法
     * 自动管理 Connection、Statement、ResultSet 生命周期
     *
     * @param url         JDBC URL
     * @param username    用户名（可为 null）
     * @param password    密码（可为 null）
     * @param driverClass 驱动类名（可为 null）
     * @param sql         查询 SQL
     * @param function    查询处理函数
     * @param <T>         返回值类型
     * @return 查询结果
     */
    public static <T> T query(String url, String username, String password, String driverClass,
                              String sql, QueryFunction<T> function) {
        try (Connection conn = getConnection(url, username, password, driverClass);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return function.apply(conn, stmt, rs);
        } catch (SQLException e) {
            throw new RuntimeException("JDBC 查询失败: " + sql, e);
        }
    }

    /**
     * 检查连接是否有效
     *
     * @param conn 连接
     * @return true 连接有效
     */
    public static boolean isValid(Connection conn) {
        try {
            if (conn == null || conn.isClosed()) {
                return false;
            }
            conn.getMetaData();
            return conn.isValid(2);
        } catch (SQLException e) {
            log.warn("连接有效性检查失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 根据 metadata 构建 flink type
     */
    public static TypeInformation<Row> inferRowType(
            String table, String sql,
            String url, String username, String password) {

        String sampleQuery;
        if (table != null) {
            sampleQuery = "SELECT * FROM " + table + " WHERE 1=0";
        } else {
            sampleQuery = "SELECT * FROM (" + sql + ") AS t WHERE 1=0";
        }

        return query(url, username, password, null, sampleQuery, (conn, stmt, rs) -> {
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
        });
    }

    /**
     * 从数据库获取表的主键信息
     *
     * @param url      数据库连接 URL
     * @param table    表名
     * @param username 用户名（可为 null）
     * @param password 密码（可为 null）
     * @return List<Pair<String, Integer>>，使用 List 保证复合主键按 KEY_SEQ 顺序排列
     */
    public static List<Pair<String, Integer>> getPrimaryKey(
            String url, String table, String username, String password) {

        return query(url, username, password, null,
                "SELECT * FROM " + table + " WHERE 1=0", (conn, stmt, rs) -> {
                    String catalog = conn.getCatalog();
                    String schema = conn.getSchema();

                    DatabaseMetaData metaData = conn.getMetaData();

                    // 按 KEY_SEQ 收集主键列名
                    Map<Integer, String> keySeqColumnName = new HashMap<>();
                    try (ResultSet rs2 = metaData.getPrimaryKeys(catalog, schema, table)) {
                        while (rs2.next()) {
                            String columnName = rs2.getString("COLUMN_NAME");
                            int keySeq = rs2.getInt("KEY_SEQ");
                            Preconditions.checkState(!keySeqColumnName.containsKey(keySeq - 1), "The field(s) of primary key must be from the same table.");
                            keySeqColumnName.put(keySeq - 1, columnName);
                        }
                    }

                    if (keySeqColumnName.isEmpty()) {
                        throw new NoPrimaryKeyException(table);
                    }

                    // 主键名称排序
                    String[] pkFields = new String[keySeqColumnName.size()];
                    keySeqColumnName.forEach((index, columnName) -> pkFields[index] = columnName);

                    // 主键名 + jdbc类型列表
                    List<Pair<String, Integer>> primaryKeyList = new ArrayList<>();
                    for (String columnName : pkFields) {
                        try (ResultSet colRs = metaData.getColumns(catalog, schema, table, columnName)) {
                            if (colRs.next()) {
                                int jdbcType = colRs.getInt("DATA_TYPE");
                                primaryKeyList.add(Pair.of(columnName, jdbcType));
                            } else {
                                throw new RuntimeException(
                                        String.format("无法获取表 '%s' 列 '%s' 的类型信息", table, columnName));
                            }
                        }
                    }

                    return primaryKeyList;
                });
    }

    /**
     * 获取指定列的 JDBC 类型
     * 底层版本，接受已构建好的查询 SQL，不依赖 JdbcDialect
     */
    public static int getColumnType(String driverClassName, String url, String sampleQuery,
                                    String username, String password, String columnName) {
        return query(url, username, password, driverClassName, sampleQuery, (conn, stmt, rs) -> {
            ResultSetMetaData metaData = rs.getMetaData();
            if (metaData.getColumnCount() < 1) {
                throw new RuntimeException("无法获取列的类型信息: " + columnName);
            }
            return metaData.getColumnType(1);
        });
    }

    /**
     * 查询函数接口
     */
    @FunctionalInterface
    public interface QueryFunction<T> {
        T apply(Connection conn, Statement stmt, ResultSet rs) throws SQLException;
    }
}
