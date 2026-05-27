package com.etl.connector.jdbc.source;

import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.core.utils.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * JDBC Source 实现
 * 支持主键范围分片读取关系型数据库
 */
@Slf4j
public class JdbcSource extends AbstractSplitSource<JdbcSplit> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSource(SourceConfig config) {
        super(config);
        jdbcSourceConfig = JdbcSourceConfig.fromSourceConfig(config, super.getDefaultBatchSize());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<JdbcSplit, BaseEnumCheckpoint<JdbcSplit>>
    createEnumerator(SplitEnumeratorContext<JdbcSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, jdbcSourceConfig);
    }

    @Override
    public SplitEnumerator<JdbcSplit, BaseEnumCheckpoint<JdbcSplit>>
    restoreEnumerator(SplitEnumeratorContext<JdbcSplit> enumContext,
                      BaseEnumCheckpoint<JdbcSplit> checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new JdbcSplitEnumerator(enumContext, checkpoint, jdbcSourceConfig);
    }

    @Override
    public SourceReader<Row, JdbcSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<AbstractSplitReader<Row, JdbcSplit>> splitReaderSupplier = JdbcSplitReader::new;
        return new JdbcSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<JdbcSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<JdbcSplit>> getEnumeratorCheckpointSerializer() {
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
