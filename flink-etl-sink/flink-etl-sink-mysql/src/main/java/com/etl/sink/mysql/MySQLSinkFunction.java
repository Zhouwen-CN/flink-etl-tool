package com.etl.sink.mysql;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL Sink Function
 * 支持批量写入和 upsert 模式，列名从 Row 字段名中动态获取
 */
@Slf4j
public class MySQLSinkFunction extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final int batchSize;
    private final String writeMode;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient int pendingCount;
    private transient String[] columns;

    public MySQLSinkFunction(String url, String username, String password,
                              String table, int batchSize, String writeMode) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.table = table;
        this.batchSize = batchSize;
        this.writeMode = writeMode;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(url, username, password);
        connection.setAutoCommit(false);
        pendingCount = 0;
        // columns 和 statement 在第一条记录到来时初始化
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        if (statement == null) {
            Set<String> fieldNames = row.getFieldNames(true);
            if (fieldNames == null || fieldNames.isEmpty()) {
                throw new IllegalStateException(
                    "Row 没有字段名信息，请使用 Row.withNames() 或确保 RowType 包含字段名");
            }
            columns = fieldNames.toArray(new String[0]);
            statement = connection.prepareStatement(buildSql());
            log.info("MySQL Sink 已连接: table={}, mode={}, batchSize={}, columns={}",
                    table, writeMode, batchSize, Arrays.toString(columns));
        }

        for (int i = 0; i < columns.length; i++) {
            statement.setObject(i + 1, row.getField(columns[i]));
        }
        statement.addBatch();
        pendingCount++;

        if (pendingCount >= batchSize) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (statement != null && pendingCount > 0) {
                flush();
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void flush() throws SQLException {
        statement.executeBatch();
        connection.commit();
        log.debug("已写入 {} 条记录到 {}", pendingCount, table);
        pendingCount = 0;
    }

    private String buildSql() {
        String colList = Arrays.stream(columns)
                .map(c -> "`" + c + "`")
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(columns)
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        if ("upsert".equalsIgnoreCase(writeMode)) {
            String updateClause = Arrays.stream(columns)
                    .map(c -> "`" + c + "` = VALUES(`" + c + "`)")
                    .collect(Collectors.joining(", "));
            return String.format("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                    table, colList, placeholders, updateClause);
        } else {
            return String.format("INSERT INTO `%s` (%s) VALUES (%s)",
                    table, colList, placeholders);
        }
    }
}