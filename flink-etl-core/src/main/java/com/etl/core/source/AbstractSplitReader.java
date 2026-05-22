package com.etl.core.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * 分片读取器抽象类
 * 简化的 SplitReader 实现，子类只需关注数据读取
 *
 * <p>该类基于 Flink 的 SplitReader，提供阻塞式数据读取能力。
 * 配合 {@link AbstractSourceReader} 使用可以大大简化 SourceReader 的实现。
 *
 * <p>父类提供：
 * <ul>
 *   <li>{@link #pendingSplits} 待处理分片队列，子类可直接访问</li>
 *   <li>{@link #handleSplitsChanges} 默认实现，将新分片加入队列</li>
 * </ul>
 *
 * @param <E> 原始记录类型（从外部系统读取的原始数据）
 * @param <SplitT> 分片类型
 *
 * @see SplitReader
 */
@Slf4j
public abstract class AbstractSplitReader<E, SplitT extends BaseSourceSplit> implements SplitReader<E, SplitT> {

    /**
     * 待处理的分片队列
     * 子类可直接访问，从中取出分片进行读取
     */
    protected final Queue<SplitT> pendingSplits = new ArrayDeque<>();

    /**
     * 阻塞式提取数据
     * 该方法会被提取线程调用，实现应该阻塞等待数据可用
     *
     * @return 包含分片 ID 的记录集合
     * @throws IOException 读取异常
     */
    @Override
    public abstract RecordsWithSplitIds<E> fetch() throws IOException;

    /**
     * 处理分片变动
     * 默认实现将新分片加入 {@link #pendingSplits} 队列，子类如有特殊需求可覆盖
     *
     * @param splitsChanges 分片变动
     */
    @Override
    public void handleSplitsChanges(SplitsChange<SplitT> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个新分片", splitsChanges.splits().size());
    }

    /**
     * 唤醒阻塞的提取操作
     * 用于在关闭或检查点时唤醒阻塞中的 fetch() 操作
     */
    @Override
    public void wakeUp() {
        // 默认空实现，子类可覆盖
    }

    /**
     * 关闭读取器，释放资源
     *
     * @throws Exception 关闭异常
     */
    @Override
    public void close() throws Exception {
        // 默认空实现，子类可覆盖
    }
}
