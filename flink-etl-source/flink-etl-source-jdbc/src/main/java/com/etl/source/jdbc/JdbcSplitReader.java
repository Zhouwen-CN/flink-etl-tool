package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitReader;
import com.etl.source.jdbc.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

/**
 * JDBC 分片读取器
 * 实现阻塞式数据读取，配合 BaseSourceReader 使用
 *
 * <p>设计说明：
 * <ul>
 *   <li>每个分片创建独立的数据库连接</li>
 *   <li>支持分批读取，每批最多 batchSize 条记录</li>
 *   <li>支持流式读取（MySQL 需设置 batchSize=Integer.MIN_VALUE）</li>
 *   <li>直接返回 Flink Row 类型，无需额外包装</li>
 * </ul>
 */
@Slf4j
public class JdbcSplitReader implements BaseSplitReader<Row, RangeSplit> {

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final String splitColumn;
    private final int batchSize;
    private final Integer queryTimeout;

    private final Queue<RangeSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    // 当前分片读取状态
    private RangeSplit currentSplit;
    private Connection currentConnection;
    private Statement currentStatement;
    private ResultSet currentResultSet;
    private boolean hasNextRecord;
    private int currentOffset;

    public JdbcSplitReader(JdbcSourceConfig config) {
        this.url = config.getUrl();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.table = config.getTable();
        this.sql = config.getSql();
        this.splitColumn = config.getSplitColumn();
        this.batchSize = config.getBatchSize();
        this.queryTimeout = config.getQueryTimeout();
    }

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        // 如果没有当前分片，尝试开始新分片
        if (currentSplit == null) {
            RangeSplit split = pendingSplits.poll();
            if (split == null) {
                // 没有待处理的分片，返回空结果
                RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }

            // 开始新分片
            startNewSplit(split);
        }

        // 读取一批数据
        return fetchBatch();
    }

    /**
     * 开始读取新分片
     */
    private void startNewSplit(RangeSplit split) throws IOException {
        log.info("开始读取分片: {}", split.splitId());

        try {
            // 创建连接
            currentConnection = DriverManager.getConnection(url, username, password);
            currentStatement = currentConnection.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
            );

            // 设置 fetchSize
            currentStatement.setFetchSize(batchSize);
            if (queryTimeout != null) {
                currentStatement.setQueryTimeout(queryTimeout);
            }

            // 使用分片中预先构建好的查询 SQL
            String querySql = split.getQuerySql();

            log.debug("执行查询: {}", querySql);

            // 执行查询
            currentResultSet = currentStatement.executeQuery(querySql);
            hasNextRecord = currentResultSet.next();
            currentOffset = 0;
            currentSplit = split;

        } catch (SQLException e) {
            // 出错时关闭资源
            closeCurrentSplit();
            throw new IOException("读取分片失败: " + split.splitId(), e);
        }
    }

    /**
     * 读取一批数据
     */
    private RecordsWithSplitIds<Row> fetchBatch() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        try {
            int recordsInBatch = 0;
            ResultSetMetaData metaData = currentResultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 读取一批记录
            while (hasNextRecord && recordsInBatch < batchSize) {
                // 内联 createRow 逻辑
                Row row = new Row(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.setField(i - 1, currentResultSet.getObject(i));
                }

                builder.add(currentSplit.splitId(), row);
                recordsInBatch++;
                currentOffset++;

                // 读取下一条记录
                hasNextRecord = currentResultSet.next();
            }

            // 如果没有更多记录，标记分片完成
            if (!hasNextRecord) {
                finishedSplits.add(currentSplit.splitId());
                log.debug("分片 {} 读取完成，共 {} 条记录", currentSplit.splitId(), currentOffset);

                // 关闭资源
                closeCurrentSplit();
            }

        } catch (SQLException e) {
            closeCurrentSplit();
            throw new IOException("读取分片失败: " + currentSplit.splitId(), e);
        }

        return builder.build();
    }

    /**
     * 关闭当前分片的资源
     */
    private void closeCurrentSplit() {
        closeQuietly(currentResultSet, "ResultSet");
        closeQuietly(currentStatement, "Statement");
        closeQuietly(currentConnection, "Connection");

        currentResultSet = null;
        currentStatement = null;
        currentConnection = null;
        currentSplit = null;
        hasNextRecord = false;
    }

    /**
     * 安静地关闭资源（忽略异常）
     */
    private void closeQuietly(AutoCloseable resource, String resourceName) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("关闭 {} 失败", resourceName, e);
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RangeSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个新分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        closeCurrentSplit();
        log.info("JdbcSplitReader 关闭");
    }
}