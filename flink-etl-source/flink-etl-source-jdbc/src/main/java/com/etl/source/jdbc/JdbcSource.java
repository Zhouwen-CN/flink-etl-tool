package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractRangeSplitSource;
import com.etl.core.source.PendingSplitsCheckpoint;
import com.etl.core.source.RangeSplit;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 */
public class JdbcSource extends AbstractRangeSplitSource<Row> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final Integer fetchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;

    public JdbcSource(SourceConfig config, JdbcDialect dialect) {
        super(config.getString("splitColumn"));  // 只传递 splitColumn
        this.url = config.getString("url");
        this.username = config.getString("username");
        this.password = config.getString("password");
        this.table = config.getString("table");
        this.sql = config.getString("sql");
        this.fetchSize = config.getInteger("fetchSize");
        this.queryTimeout = config.getInteger("queryTimeout");
        this.dialect = dialect;

        logger.info("创建 JdbcSource: table={}, sql={}, splitColumn={}",
                table, sql, splitColumn);
    }

    @Override
    protected Range<Long> getSplitColumnRange() {
        String querySql = dialect.buildRangeQuery(table, sql, splitColumn);
        logger.info("查询分片范围: {}", querySql);

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql)) {

            if (rs.next()) {
                long min = rs.getLong(1);
                long max = rs.getLong(2);
                logger.info("分片范围: [{}, {}]", min, max);
                return Range.between(min, max);
            }
            return Range.between(0L, 0L);
        } catch (SQLException e) {
            throw new RuntimeException("获取分片范围失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<RangeSplit, PendingSplitsCheckpoint<RangeSplit>>
    createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
        logger.info("创建 SplitEnumerator");
        Range<Long> range = getSplitColumnRange();
        // 从 Flink 上下文获取真实并行度
        int parallelism = enumContext.currentParallelism();
        List<RangeSplit> splits = calculateSplits(range, parallelism);
        return new JdbcSplitEnumerator(splits, enumContext);
    }

    @Override
    public SplitEnumerator<RangeSplit, PendingSplitsCheckpoint<RangeSplit>>
    restoreEnumerator(SplitEnumeratorContext<RangeSplit> enumContext,
                      PendingSplitsCheckpoint<RangeSplit> checkpoint) {
        logger.info("恢复 SplitEnumerator");
        Range<Long> range = getSplitColumnRange();
        // 从 Flink 上下文获取真实并行度
        int parallelism = enumContext.currentParallelism();
        List<RangeSplit> splits = calculateSplits(range, parallelism);
        return new JdbcSplitEnumerator(splits, enumContext);
    }

    @Override
    public SourceReader<Row, RangeSplit> createReader(SourceReaderContext readerContext) {
        logger.info("创建 SourceReader");
        return new JdbcSourceReader(url, username, password, table, sql,
                splitColumn, fetchSize, queryTimeout, dialect, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        // 使用简单的字符串序列化
        return new SimpleVersionedSerializer<RangeSplit>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(RangeSplit split) {
                return split.splitId().getBytes();
            }

            @Override
            public RangeSplit deserialize(int version, byte[] serialized) {
                // 简化实现，实际使用时会从 splitId 解析
                return new RangeSplit(splitColumn, 0, 0);
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint<RangeSplit>>
    getEnumeratorCheckpointSerializer() {
        // 简化实现
        return new SimpleVersionedSerializer<PendingSplitsCheckpoint<RangeSplit>>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(PendingSplitsCheckpoint<RangeSplit> checkpoint) {
                return new byte[0];
            }

            @Override
            public PendingSplitsCheckpoint<RangeSplit> deserialize(int version, byte[] serialized) {
                return new PendingSplitsCheckpoint<>(List.of());
            }
        };
    }
}