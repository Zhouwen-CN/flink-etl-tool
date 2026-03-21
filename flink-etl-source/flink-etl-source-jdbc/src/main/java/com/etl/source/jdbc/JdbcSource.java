package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.core.utils.SqlUtils;
import com.etl.source.jdbc.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.function.Supplier;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 */
@Slf4j
public class JdbcSource extends AbstractSplitSource<RangeSplit, RangeEnumCheckpoint> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSource(SourceConfig config) {
        super(config);
        String url = config.getString("url");
        Preconditions.checkNotNull(url, "url is null");

        // MySQL 需要添加 useCursorFetch 参数，使 batchSize 生效
        if (url.contains(":mysql:") && !url.contains("useCursorFetch=true")) {
            url = url.contains("?") ? url + "&useCursorFetch=true" : url + "?useCursorFetch=true";
            log.info("MySQL URL 添加 useCursorFetch 参数");
        }

        String username = config.getString("username");
        String password = config.getString("password");

        String table = config.getString("table");
        String sql = config.getString("sql");

        String splitColumn = config.getString("splitColumn");
        Preconditions.checkNotNull(splitColumn, "splitColumn is null");

        Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

        Integer queryTimeout = config.getInteger("queryTimeout");

        this.jdbcSourceConfig = JdbcSourceConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .splitColumn(splitColumn)
                .batchSize(batchSize)
                .queryTimeout(queryTimeout)
                .build();

        log.info("创建 JdbcSource: {}", this.jdbcSourceConfig);
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
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, RangeSplit>>) () ->
                new JdbcSplitReader(jdbcSourceConfig);
        return new JdbcSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<RangeEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        String table = jdbcSourceConfig.getTable();
        String sql = jdbcSourceConfig.getSql();
        String url = jdbcSourceConfig.getUrl();
        String username = jdbcSourceConfig.getUsername();
        String password = jdbcSourceConfig.getPassword();

        return SqlUtils.inferRowType(
                table,
                sql,
                url,
                username,
                password
        );
    }

}