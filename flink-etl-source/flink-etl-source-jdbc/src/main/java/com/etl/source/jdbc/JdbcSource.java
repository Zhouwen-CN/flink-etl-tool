package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import com.etl.core.source.AbstractRangeSplitSource;
import com.etl.core.source.RangeEnumCheckpoint;
import com.etl.core.source.RangeSplit;
import com.etl.core.source.base.BaseSplitReader;
import com.etl.core.source.base.serde.DefaultCheckpointSerializer;
import com.etl.core.source.base.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.List;
import java.util.function.Supplier;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 *
 * <p>优化后使用新的抽象类：
 * <ul>
 *   <li>{@link JdbcSplitEnumerator} - 继承 BaseSplitEnumerator</li>
 *   <li>{@link JdbcSourceReader} - 继承 BaseSourceReader</li>
 *   <li>默认序列化器 - 无需手写</li>
 *   <li>直接输出 Flink Row 类型</li>
 * </ul>
 */
@Slf4j
public class JdbcSource extends AbstractRangeSplitSource<Row> {

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final Integer fetchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;

    public JdbcSource(SourceConfig config, JdbcDialect dialect) {
        super(config.getString("splitColumn"));
        this.url = config.getString("url");
        this.username = config.getString("username");
        this.password = config.getString("password");
        this.table = config.getString("table");
        this.sql = config.getString("sql");
        this.fetchSize = config.getInteger("fetchSize");
        this.queryTimeout = config.getInteger("queryTimeout");
        this.dialect = dialect;

        // 解析 schema（可选）
        this.schema = config.getSchema();

        log.info("创建 JdbcSource: table={}, sql={}, splitColumn={}",
                table, sql, splitColumn);
    }

    @Override
    protected Range<Long> getSplitColumnRange() {
        String querySql = dialect.buildRangeQuery(table, sql, splitColumn);
        log.info("查询分片范围: {}", querySql);

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql)) {

            if (rs.next()) {
                long min = rs.getLong(1);
                long max = rs.getLong(2);
                log.info("分片范围: [{}, {}]", min, max);
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
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
        log.info("创建 SplitEnumerator");

        // 如果没有配置 schema，尝试从数据库推断
        if (schema == null) {
            schema = inferSchemaFromDatabase();
        }

        Range<Long> range = getSplitColumnRange();
        int parallelism = enumContext.currentParallelism();
        List<RangeSplit> splits = calculateSplits(range, parallelism);
        return new JdbcSplitEnumerator(splits, enumContext);
    }

    /**
     * 从数据库推断 Schema
     */
    private EtlSchema inferSchemaFromDatabase() {
        String sampleQuery = dialect.buildSampleQuery(table, sql);
        log.info("推断 Schema: {}", sampleQuery);

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleQuery)) {
            return dialect.inferSchema(rs.getMetaData());
        } catch (SQLException e) {
            throw new RuntimeException("从数据库推断 Schema 失败: " + e.getMessage(), e);
        }
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    restoreEnumerator(SplitEnumeratorContext<RangeSplit> enumContext,
                      RangeEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, checkpoint);
    }

    @Override
    public SourceReader<Row, RangeSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");

        // 创建 SplitReader 供应器
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, RangeSplit>>) () ->
                new JdbcSplitReader(url, username, password, table, sql,
                        splitColumn, fetchSize, queryTimeout, dialect);

        // 创建 Reader
        return new JdbcSourceReader(
                splitReaderSupplier,
                new Configuration(),
                readerContext
        );
    }

    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        // 使用默认序列化器
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<RangeEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        // 使用默认序列化器
        return new DefaultCheckpointSerializer<>();
    }
}