package com.etl.sink.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * MySQL Sink Function
 * 支持批量写入和 upsert 模式
 */
public class MySQLSinkFunction extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(MySQLSinkFunction.class);

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String[] columns;
    private final int batchSize;
    private final String writeMode;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient int pendingCount;

    public MySQLSinkFunction(String url, String username, String password,
                              String table, String[] columns, int batchSize, String writeMode) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.table = table;
        this.columns = columns;
        this.batchSize = batchSize;
        this.writeMode = writeMode;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(url, username, password);
        connection.setAutoCommit(false);
        statement = connection.prepareStatement(buildSql());
        pendingCount = 0;
        logger.info("MySQL Sink 已连接: table={}, mode={}, batchSize={}", table, writeMode, batchSize);
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        for (int i = 0; i < columns.length; i++) {
            statement.setObject(i + 1, row.getField(i));
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
        logger.debug("已写入 {} 条记录到 {}", pendingCount, table);
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
            // MySQL INSERT ... ON DUPLICATE KEY UPDATE
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
