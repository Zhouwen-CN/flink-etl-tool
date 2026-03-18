package com.etl.core.source;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;

/**
 * 分片读取器接口
 * 简化的 SplitReader 接口，子类只需关注数据读取
 *
 * <p>该接口基于 Flink 的 SplitReader，提供阻塞式数据读取能力。
 * 配合 {@link BaseSourceReader} 使用可以大大简化 SourceReader 的实现。
 *
 * @param <E> 原始记录类型（从外部系统读取的原始数据）
 * @param <SplitT> 分片类型
 *
 * @see SplitReader
 */
public interface BaseSplitReader<E, SplitT extends BaseSourceSplit> extends SplitReader<E, SplitT> {

    /**
     * 阻塞式提取数据
     * 该方法会被提取线程调用，实现应该阻塞等待数据可用
     *
     * @return 包含分片 ID 的记录集合
     * @throws IOException 读取异常
     */
    @Override
    RecordsWithSplitIds<E> fetch() throws IOException;

    /**
     * 处理分片变动
     * 当有新的分片分配给 Reader 时调用
     *
     * @param splitsChanges 分片变动
     */
    @Override
    void handleSplitsChanges(SplitsChange<SplitT> splitsChanges);

    /**
     * 唤醒阻塞的提取操作
     * 用于在关闭或检查点时唤醒阻塞中的 fetch() 操作
     */
    @Override
    default void wakeUp() {
        // 默认空实现，子类可覆盖
    }

    /**
     * 关闭读取器，释放资源
     *
     * @throws Exception 关闭异常
     */
    @Override
    default void close() throws Exception {
        // 默认空实现，子类可覆盖
    }
}