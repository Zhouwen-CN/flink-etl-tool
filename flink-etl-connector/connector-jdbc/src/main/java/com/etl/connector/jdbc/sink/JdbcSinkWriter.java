package com.etl.connector.jdbc.sink;

import com.etl.connector.jdbc.dialect.WriteMode;
import com.etl.core.sink.AbstractSinkWriter;
import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC Sink Writer 实现
 * 管理数据库连接和批量写入
 */
@Slf4j
public class JdbcSinkWriter extends AbstractSinkWriter<JdbcSinkConfig> {

    private static final String FIELD_FILTER_PREFIX = "__";
    private final transient Connection connection;
    private transient String[] columns;

    /**
     * INSERT/UPSERT 模式专用字段
     */
    private transient PreparedStatement normalStatement;

    /** CDC 模式专用字段 */
    private transient Map<RowKind, PreparedStatement> cdcStatements;

    /** 批量大小 */
    private final int batchSize;

    /** 待写入数据计数 */
    private int pendingCount = 0;

    public JdbcSinkWriter(Sink.InitContext context, JdbcSinkConfig config) throws IOException {
        super(context, config);
        this.batchSize = config.getBatchSize();

        // 直接初始化数据库连接（不再延迟初始化）
        try {
            connection = DriverManager.getConnection(
                config.getUrl(),
                config.getUsername(),
                config.getPassword()
            );
            connection.setAutoCommit(false);

            // CDC 模式：初始化 Statement 缓存
            if (config.getMode() == WriteMode.CDC) {
                cdcStatements = new HashMap<>();
            }

            log.info("JDBC Sink 已连接: url={}, mode={}, subtaskId={}",
                config.getUrl(), config.getMode(), context.getSubtaskId());
        } catch (SQLException e) {
            throw new IOException("Failed to initialize JDBC connection", e);
        }
    }

    @Override
    public void write(Row row, Context context) throws IOException, InterruptedException {
        try {
            // 首次写入时缓存列名（过滤掉 __ 开头的隐藏字段）
            if (columns == null) {
                columns = row.getFieldNames(true).stream()
                        .filter(name -> !name.startsWith(FIELD_FILTER_PREFIX))
                    .toArray(String[]::new);
                log.debug("JDBC Sink 写入字段（已过滤隐藏字段）: {}", Arrays.toString(columns));
            }

            if (config.getMode() == WriteMode.CDC) {
                writeCdcRow(row);
            } else {
                writeNormalRow(row);
            }

            pendingCount++;

            // 达到批量大小时自动 flush
            if (pendingCount >= batchSize) {
                flush(false);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to write row", e);
        }
    }

    /**
     * 写入普通行（INSERT/UPSERT 模式）
     */
    private void writeNormalRow(Row row) throws SQLException {
        if (normalStatement == null) {
            initStatement();
        }

        for (int i = 0; i < columns.length; i++) {
            normalStatement.setObject(i + 1, row.getField(columns[i]));
        }

        normalStatement.addBatch();
    }

    private void initStatement() throws SQLException {
        String sql;
        if (config.getMode() == WriteMode.CUSTOM) {
            // CUSTOM 模式：使用用户自定义 SQL
            NamedParameterSqlParser.ParsedSql parsed = NamedParameterSqlParser.parse(config.getSql());
            sql = parsed.getPreparedSql();
            log.info("JDBC Sink CUSTOM 模式: {}", sql);
        } else {
            // INSERT/UPSERT 模式：基于 table 配置生成 SQL
            if (config.getMode() == WriteMode.UPSERT) {
                sql = config.getDialect().getUpsertSql(config.getTable(), columns, config.getKeyFields());
                log.info("JDBC Sink UPSERT 模式: table={}, keyFields={}", config.getTable(), config.getKeyFields());
            } else {
                sql = config.getDialect().getInsertSql(config.getTable(), columns);
                log.info("JDBC Sink INSERT 模式: table={}, columns={}", config.getTable(), Arrays.toString(columns));
            }
        }

        this.normalStatement = connection.prepareStatement(sql);
    }

    /**
     * 写入 CDC 行
     */
    private void writeCdcRow(Row row) throws SQLException {
        RowKind kind = row.getKind();

        // 获取或创建 PreparedStatement
        PreparedStatement stmt = getCdcStatement(kind);

        // 设置参数并 addBatch
        setCdcParameters(stmt, row, kind);
        stmt.addBatch();
    }

    /**
     * 获取或创建 CDC Statement
     */
    private PreparedStatement getCdcStatement(RowKind kind) throws SQLException {
        if (!cdcStatements.containsKey(kind)) {
            String sql = buildCdcSql(kind);
            PreparedStatement stmt = connection.prepareStatement(sql);
            cdcStatements.put(kind, stmt);
            log.info("CDC SQL 创建: kind={}, sql={}", kind, sql);
        }

        return cdcStatements.get(kind);
    }

    /**
     * 构建 CDC SQL
     */
    private String buildCdcSql(RowKind kind) {
        String table = config.getTable();
        List<String> keyFields = config.getKeyFields();

        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                // INSERT 和 UPDATE_AFTER 都使用 upsert SQL（原子操作，存在则更新，不存在则插入）
                return config.getDialect().getUpsertSql(table, columns, keyFields);
            case DELETE:
                return config.getDialect().getDeleteSql(table, keyFields);
            default:
                throw new IllegalArgumentException(
                    String.format("CDC 模式不支持 RowKind: %s，支持: INSERT, UPDATE_AFTER, DELETE", kind)
                );
        }
    }

    /**
     * 设置 CDC 参数
     */
    private void setCdcParameters(PreparedStatement stmt, Row row, RowKind kind) throws SQLException {
        List<String> keyFields = config.getKeyFields();
        int index = 1;

        switch (kind) {
            case INSERT:
                // INSERT: 设置所有字段
                for (String col : columns) {
                    stmt.setObject(index++, row.getField(col));
                }
                break;

            case UPDATE_AFTER:
                // UPDATE: SET 部分用非主键字段，WHERE 用主键字段
                for (String col : columns) {
                    if (!keyFields.contains(col)) {
                        stmt.setObject(index++, row.getField(col));
                    }
                }
                for (String key : keyFields) {
                    stmt.setObject(index++, row.getField(key));
                }
                break;

            case DELETE:
                // DELETE: 只设置主键字段（WHERE 条件）
                for (String key : keyFields) {
                    stmt.setObject(index++, row.getField(key));
                }
                break;

            default:
                throw new IllegalArgumentException("CDC 模式不支持 RowKind: " + kind);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (pendingCount == 0) {
            return;
        }

        try {
            if (config.getMode() == WriteMode.CDC) {
                // CDC 模式：遍历所有 Statement 执行 executeBatch
                for (PreparedStatement stmt : cdcStatements.values()) {
                    stmt.executeBatch();
                }
                connection.commit();
                pendingCount = 0;
            } else {
                // INSERT/UPSERT 模式：flush 正常批次
                int[] results = normalStatement.executeBatch();
                connection.commit();
                pendingCount = 0;

                log.debug("已写入 {} 条记录, subtaskId={}", results.length, this.context.getSubtaskId());
            }
        } catch (SQLException e) {
            // 回滚事务
            try {
                if (connection != null) {
                    connection.rollback();
                    log.warn("Flush 失败，已回滚事务");
                }
            } catch (SQLException rollbackEx) {
                log.error("回滚失败", rollbackEx);
            }
            throw new IOException("Failed to flush batch", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // 提交剩余数据
            flush(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while flushing during close", e);
        } finally {
            // 清理资源
            try {
                if (config.getMode() == WriteMode.CDC) {
                    // CDC 模式：关闭所有 Statement
                    if (cdcStatements != null) {
                        for (PreparedStatement stmt : cdcStatements.values()) {
                            if (stmt != null) {
                                stmt.close();
                            }
                        }
                    }
                } else {
                    if (normalStatement != null) {
                        normalStatement.close();
                    }
                }
                if (connection != null) {
                    connection.close();
                }
                log.info("JDBC Sink 资源清理完成, subtaskId={}", context.getSubtaskId());
            } catch (SQLException e) {
                throw new IOException("Failed to cleanup JDBC resources", e);
            }
        }
    }
}