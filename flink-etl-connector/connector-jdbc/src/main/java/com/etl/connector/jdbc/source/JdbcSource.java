package com.etl.connector.jdbc.source;

import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.BaseRecordEmitter;
import com.etl.core.source.BaseSourceReader;
import com.etl.core.util.SqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.types.Row;

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
        return new BaseSourceReader<>(JdbcSplitReader::new, new BaseRecordEmitter<>(readerContext), readerContext);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        String table = jdbcSourceConfig.getTable();
        String sql = jdbcSourceConfig.getSql();
        String url = jdbcSourceConfig.getUrl();
        String username = jdbcSourceConfig.getUsername();
        String password = jdbcSourceConfig.getPassword();

        return SqlUtil.inferRowType(
                table,
                sql,
                url,
                username,
                password
        );
    }
}
