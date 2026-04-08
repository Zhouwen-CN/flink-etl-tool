package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialectLoader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.core.utils.SqlUtils;
import com.etl.source.jdbc.config.JdbcSourceConfig;
import com.etl.source.jdbc.enums.SplitStrategy;
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
        String url = Preconditions.checkNotNull(config.getString("url"), "url is null");

        // 支持显式配置 dialect
        String dialectName = config.getString("dialect");
        JdbcDialect dialect = JdbcDialectLoader.get(dialectName, url);

        // 使用 Dialect 包装 URL
        url = dialect.wrapUrl(url);

        String username = config.getString("username");
        String password = config.getString("password");

        String table = config.getString("table");
        String sql = config.getString("sql");

        String splitColumn = config.getString("splitColumn");
        SplitStrategy splitStrategy;

        if (splitColumn == null) {
            // 未配置 splitColumn，使用全表扫描模式
            log.warn("未配置 splitColumn，将使用单分片全表扫描模式，无法并行读取。建议配置 splitColumn 以启用并行分片读取。");
            splitStrategy = SplitStrategy.FULL_TABLE_SCAN;
        } else {
            // 配置了 splitColumn，自动匹配分片策略
            int jdbcType = dialect.getColumnType(url, table, sql, splitColumn, username, password);
            splitStrategy = SplitStrategy.fromJdbcType(jdbcType);
            // 如果没有匹配的策略，抛出明确的错误
            if (splitStrategy == null) {
                throw new IllegalArgumentException(
                        String.format("分片列 '%s' 的 JDBC 类型(%d)不支持分片。支持的类型: %s",
                                splitColumn, jdbcType, SplitStrategy.NUMERIC.getSupportedTypeNames()));
            }
            log.info("分片列 '{}' 使用策略: {}", splitColumn, splitStrategy.getDescription());
        }

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
                .splitStrategy(splitStrategy)
                .batchSize(batchSize)
                .queryTimeout(queryTimeout)
                .dialect(dialect)
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
        Supplier<BaseSplitReader<Row, RangeSplit>> splitReaderSupplier = () ->
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