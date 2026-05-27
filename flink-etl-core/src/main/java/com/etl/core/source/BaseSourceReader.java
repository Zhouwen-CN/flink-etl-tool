package com.etl.core.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.function.Supplier;

/**
 * 源阅读器基类
 * 基于 Flink 的 SingleThreadMultiplexSourceReaderBase，封装了线程模型和状态管理
 *
 * @param <SplitT> 分片类型
 * @see SingleThreadMultiplexSourceReaderBase
 */
@Slf4j
public class BaseSourceReader<SplitT extends BaseSourceSplit>
        extends SingleThreadMultiplexSourceReaderBase<Row, Row, SplitT, BaseSplitState<SplitT>> {

    public BaseSourceReader(
            Supplier<AbstractSplitReader<Row, SplitT>> splitReaderSupplier,
            RecordEmitter<Row, Row, BaseSplitState<SplitT>> recordEmitter,
            SourceReaderContext context) {
        super(splitReaderSupplier::get, recordEmitter, new Configuration(), context);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, BaseSplitState<SplitT>> finishedSplitIds) {
        log.info("分片完成: {}", finishedSplitIds.keySet());
        context.sendSplitRequest();
    }

    @Override
    public BaseSplitState<SplitT> initializedState(SplitT split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new BaseSplitState<>(split);
    }

    @Override
    protected SplitT toSplitType(String splitId, BaseSplitState<SplitT> splitState) {
        return splitState.getSplit();
    }
}
