package com.etl.core.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 分片枚举器抽象基类
 * 封装了分片分配的通用逻辑，子类只需关注分片发现
 *
 * <p>该类自动处理：
 * <ul>
 *   <li>handleSplitRequest - 从队列中分配分片给 Reader</li>
 *   <li>addSplitsBack - Reader 失败时回收未处理的分片</li>
 *   <li>snapshotState - 将待处理分片快照为 BaseEnumCheckpoint</li>
 * </ul>
 *
 * @param <SplitT> 分片类型
 */
@Slf4j
public abstract class AbstractSplitEnumerator<SplitT extends BaseSourceSplit>
        implements SplitEnumerator<SplitT, BaseEnumCheckpoint<SplitT>> {

    /** 待分配的分片队列（线程安全） */
    protected final Queue<SplitT> pendingSplits = new ConcurrentLinkedQueue<>();

    /** 枚举器上下文 */
    protected final SplitEnumeratorContext<SplitT> context;

    public AbstractSplitEnumerator(SplitEnumeratorContext<SplitT> context) {
        this.context = context;
    }

    public AbstractSplitEnumerator(SplitEnumeratorContext<SplitT> context,
                                   BaseEnumCheckpoint<SplitT> checkpoint) {
        this(context);
        if (checkpoint != null && checkpoint.getPendingSplits() != null) {
            pendingSplits.addAll(checkpoint.getPendingSplits());
            log.info("从检查点恢复 {} 个待处理分片", pendingSplits.size());
        }
    }

    /**
     * 处理分片请求
     * 自动从队列中取出分片分配给请求的 Reader，如果队列为空则通知无更多分片
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        SplitT split = pendingSplits.poll();
        if (split != null) {
            log.debug("分配分片 {} 给 Reader {}", split.splitId(), subtaskId);
            context.assignSplit(split, subtaskId);
        } else {
            log.debug("无更多分片，通知 Reader {}", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    /**
     * 将分配失败的分片返回到待处理队列
     * 当 Reader 失败时会调用此方法
     */
    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        log.warn("Reader {} 返回 {} 个未处理的分片", subtaskId, splits.size());
        pendingSplits.addAll(splits);
    }

    /**
     * 子类可覆盖：处理 Reader 注册通知
     */
    @Override
    public void addReader(int subtaskId) {
        log.debug("Reader {} 已注册", subtaskId);
    }

    /**
     * 默认快照实现：将待处理分片打包为 BaseEnumCheckpoint
     * 子类如无特殊状态，无需覆盖
     */
    @Override
    public BaseEnumCheckpoint<SplitT> snapshotState(long checkpointId) {
        List<SplitT> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new BaseEnumCheckpoint<>(pending);
    }

    /**
     * 获取待处理分片数量
     */
    protected int getPendingSplitCount() {
        return pendingSplits.size();
    }

    /**
     * 添加分片到待处理队列
     */
    protected void addPendingSplits(List<SplitT> splits) {
        pendingSplits.addAll(splits);
        log.debug("添加 {} 个分片到待处理队列，当前队列大小: {}", splits.size(), pendingSplits.size());
    }
}
