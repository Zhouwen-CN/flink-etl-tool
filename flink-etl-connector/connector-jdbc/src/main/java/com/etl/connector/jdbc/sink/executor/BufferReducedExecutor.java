package com.etl.connector.jdbc.sink.executor;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Key 归并缓冲执行器
 * 用于 CDC/UPSERT 模式，按主键归并数据并分段执行
 */
@Slf4j
public class BufferReducedExecutor implements JdbcBatchStatementExecutor {

    private static final int DEFAULT_MAX_BUFFER_SIZE = 10_000;

    private final JdbcBatchStatementExecutor upsertExecutor;
    private final JdbcBatchStatementExecutor deleteExecutor;
    private final Function<Row, Row> keyExtractor;
    private final int maxBufferSize;

    // 核心缓冲：LinkedHashMap 保证流入顺序
    // Boolean: true=upsert, false=delete
    private transient LinkedHashMap<Row, Map.Entry<Boolean, Row>> buffer;

    public BufferReducedExecutor(
            JdbcDialect dialect,
            String table,
            String[] columns,
            List<String> keyFields) {
        this(dialect, table, columns, keyFields, DEFAULT_MAX_BUFFER_SIZE);
    }

    public BufferReducedExecutor(
            JdbcDialect dialect,
            String table,
            String[] columns,
            List<String> keyFields,
            int maxBufferSize) {

        this.maxBufferSize = maxBufferSize;

        // 初始化内部 Executor
        String upsertSql = dialect.getUpsertSql(table, columns, keyFields);
        String deleteSql = dialect.getDeleteSql(table, keyFields);

        this.upsertExecutor = new SimpleBufferedExecutor(upsertSql, columns);
        this.deleteExecutor = new SimpleBufferedExecutor(deleteSql, keyFields.toArray(new String[0]));

        // Key 提取函数
        this.keyExtractor = row -> {
            Object[] keyValues = new Object[keyFields.size()];
            for (int i = 0; i < keyFields.size(); i++) {
                keyValues[i] = row.getField(keyFields.get(i));
            }
            return Row.of(keyValues);
        };

        log.info("BufferReducedExecutor 初始化: table={}, keyFields={}, upsertSql={}, deleteSql={}",
                table, keyFields, upsertSql, deleteSql);
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        upsertExecutor.prepareStatements(connection);
        deleteExecutor.prepareStatements(connection);
        this.buffer = new LinkedHashMap<>();
    }

    @Override
    public void addToBatch(Row record) throws SQLException {
        RowKind kind = record.getKind();

        // CDC 模式：UPDATE_BEFORE 直接跳过
        if (kind == RowKind.UPDATE_BEFORE) {
            log.debug("跳过 UPDATE_BEFORE: {}", record);
            return;
        }

        // 提取主键
        Row key = keyExtractor.apply(record);

        // changeFlag: true=upsert(INSERT/UPDATE_AFTER), false=delete(DELETE)
        boolean changeFlag = (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER);

        // 归并入 buffer（同 key 自动覆盖，保留最终状态）
        buffer.put(key, new AbstractMap.SimpleEntry<>(changeFlag, record));

        // 防止 buffer 无限增长导致 OOM
        if (buffer.size() >= maxBufferSize) {
            log.warn("Buffer 达到上限 {}，强制执行批次", maxBufferSize);
            try {
                executeBatch();
            } catch (SQLException e) {
                throw new SQLException("Buffer 满时强制刷写失败", e);
            }
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }

        Boolean prevFlag = null;
        for (Map.Entry<Row, Map.Entry<Boolean, Row>> entry : buffer.entrySet()) {
            boolean currentFlag = entry.getValue().getKey();
            Row data = entry.getValue().getValue();

            if (currentFlag) {  // Upsert
                // 前面是 delete，先执行完 delete
                if (prevFlag != null && !prevFlag) {
                    deleteExecutor.executeBatch();
                }
                upsertExecutor.addToBatch(data);
            } else {  // Delete
                // 前面是 upsert，先执行完 upsert
                if (prevFlag != null && prevFlag) {
                    upsertExecutor.executeBatch();
                }
                deleteExecutor.addToBatch(entry.getKey());  // 只需要主键
            }
            prevFlag = currentFlag;
        }

        // 执行最后的批次
        if (prevFlag) {
            upsertExecutor.executeBatch();
        } else {
            deleteExecutor.executeBatch();
        }

        buffer.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        upsertExecutor.closeStatements();
        deleteExecutor.closeStatements();
    }
}
