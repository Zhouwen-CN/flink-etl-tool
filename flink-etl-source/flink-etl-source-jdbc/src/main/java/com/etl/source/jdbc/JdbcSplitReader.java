package com.etl.source.jdbc;

import com.etl.core.source.RangeSplit;
import com.etl.core.source.base.BaseSplitReader;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li>使用 fetch() 方法一次性读取一个分片的所有数据</li>
 *   <li>支持流式读取（通过 fetchSize 控制）</li>
 * </ul>
 */
public class JdbcSplitReader implements BaseSplitReader<JdbcRecord, RangeSplit> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSplitReader.class);

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final String splitColumn;
    private final Integer fetchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;

    private final Queue<RangeSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    public JdbcSplitReader(String url, String username, String password,
                           String table, String sql, String splitColumn,
                           Integer fetchSize, Integer queryTimeout,
                           JdbcDialect dialect) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.table = table;
        this.sql = sql;
        this.splitColumn = splitColumn;
        this.fetchSize = fetchSize;
        this.queryTimeout = queryTimeout;
        this.dialect = dialect;
    }

    @Override
    public RecordsWithSplitIds<JdbcRecord> fetch() throws IOException {
        RangeSplit split = pendingSplits.poll();

        if (split == null) {
            // 没有待处理的分片，返回空结果
            RecordsBySplits.Builder<JdbcRecord> builder = new RecordsBySplits.Builder<>();
            builder.addFinishedSplits(finishedSplits);
            return builder.build();
        }

        logger.info("开始读取分片: {}", split.splitId());

        try {
            return fetchDataForSplit(split);
        } catch (SQLException e) {
            throw new IOException("读取分片失败: " + split.splitId(), e);
        }
    }

    /**
     * 读取单个分片的数据
     */
    private RecordsWithSplitIds<JdbcRecord> fetchDataForSplit(RangeSplit split) throws SQLException {
        RecordsBySplits.Builder<JdbcRecord> builder = new RecordsBySplits.Builder<>();

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {

            // 设置 fetchSize 实现流式读取
            if (fetchSize != null) {
                stmt.setFetchSize(fetchSize);
            }
            if (queryTimeout != null) {
                stmt.setQueryTimeout(queryTimeout);
            }

            // 构建分片查询 SQL
            String querySql = dialect.buildSplitQuery(table, sql, splitColumn,
                    split.getStart(), split.getEnd());
            logger.debug("执行查询: {}", querySql);

            try (ResultSet rs = stmt.executeQuery(querySql)) {
                while (rs.next()) {
                    Row row = dialect.createRow(rs);
                    builder.add(split.splitId(), new JdbcRecord(row, split.splitId()));
                }
            }
        }

        // 标记分片完成
        finishedSplits.add(split.splitId());
        logger.info("分片 {} 读取完成", split.splitId());

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RangeSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        logger.debug("接收到 {} 个新分片", splitsChanges.splits().size());
    }

    @Override
    public void wakeUp() {
        // JDBC 读取是同步阻塞的，不需要唤醒机制
    }

    @Override
    public void close() throws Exception {
        logger.info("JdbcSplitReader 关闭");
    }
}