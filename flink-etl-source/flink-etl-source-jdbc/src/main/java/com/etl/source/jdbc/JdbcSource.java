package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.source.jdbc.config.JdbcSourceConfig;
import com.etl.source.jdbc.dialect.JdbcDialect;
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


    private final JdbcSourceConfig jdbcSourceConfig;

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

        String username = config.getString("username");
        String password = config.getString("password");

        // 不能同时为null
        String table = config.getString("table");
        String sql = config.getString("sql");

        String splitColumn = config.getString("splitColumn");
        Preconditions.checkNotNull(splitColumn, "splitColumn is null");

        Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        Integer queryTimeout = config.getInteger("queryTimeout");

        jdbcSourceConfig = JdbcSourceConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .splitColumn(splitColumn)
                .batchSize(batchSize)
                .queryTimeout(queryTimeout)
                .dialect(dialect)
                .build();

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
        return new JdbcSplitEnumerator(enumContext, jdbcSourceConfig);
    }

    @Override
    public SplitEnumerator<RangeSplit, RangeEnumCheckpoint>
    restoreEnumerator(SplitEnumeratorContext<RangeSplit> enumContext,
                      RangeEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");

        return new JdbcSplitEnumerator(enumContext, checkpoint, jdbcSourceConfig);
    }

    @Override
    public SourceReader<Row, RangeSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");

        // 创建 SplitReader 供应器
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, RangeSplit>>) () ->
                new JdbcSplitReader(jdbcSourceConfig);

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


    /**
     * 重写父类 getProducedType，通过 metadata 推断字段名称和类型
     */
    @Override
    public TypeInformation<Row> getProducedType() {
        JdbcDialect dialect = jdbcSourceConfig.getDialect();
        String table = jdbcSourceConfig.getTable();
        String sql = jdbcSourceConfig.getSql();
        String url = jdbcSourceConfig.getUrl();
        String username = jdbcSourceConfig.getUsername();
        String password = jdbcSourceConfig.getPassword();

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