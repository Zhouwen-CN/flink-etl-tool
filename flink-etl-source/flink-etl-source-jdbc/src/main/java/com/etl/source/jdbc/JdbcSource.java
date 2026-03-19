package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.source.jdbc.dialect.MySQLDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

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
public class JdbcSource extends AbstractRangeSplitSource {

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final String splitColumn;
    private final int batchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;

    public JdbcSource(SourceConfig config, JdbcDialect dialect) {
        super(config);
        String url = config.getString("url");
        Preconditions.checkNotNull(url, "url is null");

        // mysql 需要加上这个参数，batchSize 参数才能生效
        if (dialect instanceof MySQLDialect) {
            if(!url.contains("useCursorFetch=true")){
                if(url.contains("?")){
                    url += "&useCursorFetch=true";
                }else{
                    url += "?useCursorFetch=true";
                }
            }
        }
        this.url = url;
        this.username = config.getString("username");
        this.password = config.getString("password");
        this.table = config.getString("table");
        this.splitColumn = config.getString("splitColumn");
        Preconditions.checkNotNull(this.splitColumn, "splitColumn is null");
        this.sql = config.getString("sql");
        this.batchSize = config.getInteger("batchSize", 100);
        this.queryTimeout = config.getInteger("queryTimeout");
        this.dialect = dialect;

        log.info("创建 JdbcSource: table={}, sql={}, splitColumn={}", table, sql, splitColumn);
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

        Range<Long> range = getSplitColumnRange();
        int parallelism = enumContext.currentParallelism();
        List<RangeSplit> splits = calculateSplits(splitColumn, range, parallelism);
        return new JdbcSplitEnumerator(splits, enumContext);
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
                        splitColumn, batchSize, queryTimeout, dialect);

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

    @Override
    public TypeInformation<Row> getProducedType() {
        String sampleQuery = dialect.buildSampleQuery(table, sql);
        log.info("推断 Schema: {}", sampleQuery);

        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleQuery)) {
            return dialect.inferType(rs.getMetaData());
        } catch (SQLException e) {
            throw new RuntimeException("从数据库推断 Schema 失败: " + e.getMessage(), e);
        }
    }
}