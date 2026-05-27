package com.etl.core.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

/**
 * 支持分片的 Source 抽象基类
 * 简化 Flink FLIP-27 Source API 的实现
 *
 * @param <SplitT> 分片类型
 */
public abstract class AbstractSplitSource<SplitT extends BaseSourceSplit>
        implements Source<Row, SplitT, BaseEnumCheckpoint<SplitT>>, ResultTypeQueryable<Row> {

    protected final SourceConfig config;

    public AbstractSplitSource(SourceConfig config) {
        this.config = config;
    }

    /**
     * 所有 source 默认的 batchSize
     */
    protected int getDefaultBatchSize() {
        return 100;
    }

    @Override
    public abstract SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>>
            createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>>
            restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext,
                              BaseEnumCheckpoint<SplitT> checkpoint);

    @Override
    public abstract SourceReader<Row, SplitT> createReader(SourceReaderContext readerContext);

    @Override
    public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

    @Override
    public abstract SimpleVersionedSerializer<BaseEnumCheckpoint<SplitT>>
            getEnumeratorCheckpointSerializer();

    /**
     * 默认从 source.schema 中获取，子类可以重写
     */
    @Override
    public TypeInformation<Row> getProducedType() {
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            throw new SchemaConfigException("schema is null");
        }
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}
