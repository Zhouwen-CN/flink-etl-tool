package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.source.jdbc.dialect.MySQLDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.*;
import java.util.function.Supplier;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 *
 * <p>直接继承 AbstractSplitSource，分片逻辑由 JdbcSplitEnumerator 处理。
 */
@Slf4j
public class JdbcSource extends AbstractSplitSource<RangeSplit, RangeEnumCheckpoint> {

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
        this.batchSize = config.getInteger("batchSize", getDefaultBatchSize());
        this.queryTimeout = config.getInteger("queryTimeout");
        this.dialect = dialect;

        log.info("创建 JdbcSource: table={}, sql={}, splitColumn={}", table, sql, splitColumn);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    createEnumerator(SplitEnumeratorContext<RangeSplit> enumContext) {
        log.info("创建 SplitEnumerator");

        JdbcSplitConfig splitConfig = JdbcSplitConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .splitColumn(splitColumn)
                .dialect(dialect)
                .build();

        return new JdbcSplitEnumerator(enumContext, splitConfig);
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    restoreEnumerator(SplitEnumeratorContext<RangeSplit> enumContext,
                      RangeEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");

        JdbcSplitConfig splitConfig = JdbcSplitConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .splitColumn(splitColumn)
                .dialect(dialect)
                .build();

        return new JdbcSplitEnumerator(enumContext, checkpoint, splitConfig);
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
        // 如果配置了 schema，使用父类实现
        if (getConfig().getSchema() != null) {
            return super.getProducedType();
        }

        // 否则从数据库推断
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