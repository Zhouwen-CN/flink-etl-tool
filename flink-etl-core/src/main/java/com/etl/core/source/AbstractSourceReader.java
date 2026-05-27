package com.etl.core.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import java.util.Map;
import java.util.function.Supplier;

/**
 * 源阅读器抽象基类
 * 基于 Flink 的 SingleThreadMultiplexSourceReaderBase，封装了线程模型和状态管理
 *
 * @param <E>      原始记录类型（从外部系统读取的原始数据）
 * @param <T>      输出记录类型（最终输出的数据）
 * @param <SplitT> 分片类型
 * @see SingleThreadMultiplexSourceReaderBase
 */
@Slf4j
public abstract class AbstractSourceReader<E, T, SplitT extends BaseSourceSplit>
        extends SingleThreadMultiplexSourceReaderBase<E, T, SplitT, BaseSplitState<SplitT>> {

    public AbstractSourceReader(
            Supplier<AbstractSplitReader<E, SplitT>> splitReaderSupplier,
            RecordEmitter<E, T, BaseSplitState<SplitT>> recordEmitter,
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

    /**
     * 默认实现：创建 BaseSplitState
     * 子类如无特殊状态需求，无需覆盖
     */
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
