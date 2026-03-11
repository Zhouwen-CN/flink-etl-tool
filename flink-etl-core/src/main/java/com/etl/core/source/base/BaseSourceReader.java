package com.etl.core.source.base;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordEvaluator;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 源阅读器抽象基类
 * 基于 Flink 的 SingleThreadMultiplexSourceReaderBase，封装了线程模型和状态管理
 *
 * <p>该类自动处理：
 * <ul>
 *   <li>线程模型 - 阻塞操作在单独线程中执行</li>
 *   <li>状态管理 - 每个分片的状态追踪</li>
 *   <li>水印对齐 - 支持分片级别的水印</li>
 * </ul>
 *
 * <p>子类需要实现：
 * <ul>
 *   <li>{@link #initializedState(BaseSourceSplit)} - 初始化分片状态</li>
 *   <li>{@link #toSplitType(String, BaseSplitState)} - 状态转换为分片</li>
 *   <li>{@link #onSplitFinished(Map)} - 分片完成回调</li>
 * </ul>
 *
 * @param <E> 原始记录类型（从外部系统读取的原始数据）
 * @param <T> 输出记录类型（最终输出的数据）
 * @param <SplitT> 分片类型
 * @param <StateT> 分片状态类型
 *
 * @see SingleThreadMultiplexSourceReaderBase
 */
public abstract class BaseSourceReader<E, T, SplitT extends BaseSourceSplit, StateT extends BaseSplitState<SplitT>>
        extends SingleThreadMultiplexSourceReaderBase<E, T, SplitT, StateT> {

    /**
     * 构造函数
     *
     * @param splitReaderSupplier 分片读取器供应器
     * @param recordEmitter 记录发射器
     * @param config 配置
     * @param context 读取器上下文
     */
    public BaseSourceReader(
            Supplier<BaseSplitReader<E, SplitT>> splitReaderSupplier,
            RecordEmitter<E, T, StateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(splitReaderSupplier::get, recordEmitter, config, context);
    }

    /**
     * 构造函数（自定义 FetcherManager）
     *
     * @param splitFetcherManager 分片提取管理器
     * @param recordEmitter 记录发射器
     * @param config 配置
     * @param context 读取器上下文
     */
    public BaseSourceReader(
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, StateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(splitFetcherManager, recordEmitter, config, context);
    }

    /**
     * 构造函数（完整版本，支持 EOF 记录评估）
     *
     * @param splitFetcherManager 分片提取管理器
     * @param recordEmitter 记录发射器
     * @param eofRecordEvaluator EOF 记录评估器
     * @param config 配置
     * @param context 读取器上下文
     */
    public BaseSourceReader(
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, StateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context) {
        super(splitFetcherManager, recordEmitter, eofRecordEvaluator, config, context);
    }

    /**
     * 启动读取器
     * 如果没有分配分片，则请求分片
     */
    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    /**
     * 分片完成时的回调
     * 子类应该在此处理完成的分片，例如清理状态或请求新分片
     *
     * @param finishedSplitIds 完成的分片 ID 和状态映射
     */
    @Override
    protected abstract void onSplitFinished(Map<String, StateT> finishedSplitIds);

    /**
     * 初始化分片状态
     * 当新分片分配给 Reader 时调用
     *
     * @param split 分片
     * @return 初始化的状态
     */
    @Override
    public abstract StateT initializedState(SplitT split);

    /**
     * 将状态转换为分片类型
     * 用于检查点恢复
     *
     * @param splitId 分片 ID
     * @param splitState 分片状态
     * @return 分片
     */
    @Override
    protected abstract SplitT toSplitType(String splitId, StateT splitState);
}