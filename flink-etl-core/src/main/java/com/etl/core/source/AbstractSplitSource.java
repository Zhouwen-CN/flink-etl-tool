package com.etl.core.source;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * 支持分片的 Source 抽象基类
 * 简化 Flink FLIP-27 Source API 的实现
 *
 * @param <T> 输出记录类型
 * @param <SplitT> 分片类型
 * @param <CheckpointT> 检查点类型
 */
public abstract class AbstractSplitSource<T, SplitT extends SourceSplit, CheckpointT>
        implements Source<T, SplitT, CheckpointT> {

    protected EtlSchema schema;

    @Override
    public abstract SplitEnumerator<SplitT, CheckpointT>
    createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, CheckpointT>
    restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext, CheckpointT checkpoint);

    @Override
    public abstract SourceReader<T, SplitT> createReader(SourceReaderContext readerContext);

    @Override
    public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

    @Override
    public abstract SimpleVersionedSerializer<CheckpointT> getEnumeratorCheckpointSerializer();
}