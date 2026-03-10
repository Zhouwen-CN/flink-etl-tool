package com.etl.core.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * 支持分片的 Source 抽象基类
 * 简化 Flink FLIP-27 Source API 的实现
 *
 * @param <T> 输出记录类型
 * @param <SplitT> 分片类型
 */
public abstract class AbstractSplitSource<T, SplitT extends SourceSplit>
        implements Source<T, SplitT, PendingSplitsCheckpoint<SplitT>> {

    @Override
    public abstract SplitEnumerator<SplitT, PendingSplitsCheckpoint<SplitT>>
    createEnumerator(SplitEnumeratorContext<SplitT> enumContext);

    @Override
    public abstract SplitEnumerator<SplitT, PendingSplitsCheckpoint<SplitT>>
    restoreEnumerator(SplitEnumeratorContext<SplitT> enumContext,
                      PendingSplitsCheckpoint<SplitT> checkpoint);

    @Override
    public abstract SourceReader<T, SplitT> createReader(SourceReaderContext readerContext);

    @Override
    public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

    @Override
    public abstract SimpleVersionedSerializer<PendingSplitsCheckpoint<SplitT>>
    getEnumeratorCheckpointSerializer();
}