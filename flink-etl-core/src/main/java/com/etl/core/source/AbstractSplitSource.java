package com.etl.core.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

/**
 * 支持分片的 Source 抽象基类
 * 简化 Flink FLIP-27 Source API 的实现
 *
 * @param <SplitT> 分片类型
 * @param <CheckpointT> 检查点类型
 */
public abstract class AbstractSplitSource<SplitT extends SourceSplit, CheckpointT>
        implements Source<Row, SplitT, CheckpointT>, ResultTypeQueryable<Row> {

    protected final SourceConfig config;

    public AbstractSplitSource(SourceConfig config) {
        this.config = config;
    }

    /**
     * 所有 source 默认的 batchSize
     * @return 批次大小
     */
    protected int getDefaultBatchSize() {
        return 100;
    }

    @Override
    public abstract SplitEnumerator<SplitT, CheckpointT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, CheckpointT> restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext, CheckpointT checkpoint);

    @Override
    public abstract SourceReader<Row, SplitT> createReader(SourceReaderContext readerContext);

    @Override
    public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

    @Override
    public abstract SimpleVersionedSerializer<CheckpointT> getEnumeratorCheckpointSerializer();

    /**
     * 默认从 source.schema 中获取，子类可以重写
     */
    @Override
    public TypeInformation<Row> getProducedType() {
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            throw new SchemaConfigException("schema is null");
        }

        // 直接使用 EtlSchema 中的字段名和类型
        return Types.ROW_NAMED(schema.getFieldNames(), schema.getFieldTypes());
    }
}