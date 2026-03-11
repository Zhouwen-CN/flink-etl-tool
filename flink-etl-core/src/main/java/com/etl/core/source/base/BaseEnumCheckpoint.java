package com.etl.core.source.base;

import java.io.Serializable;
import java.util.Collection;

/**
 * 枚举器检查点抽象类
 * 用于保存 SplitEnumerator 的状态，支持故障恢复
 *
 * @param <SplitT> 分片类型
 */
public abstract class BaseEnumCheckpoint<SplitT extends BaseSourceSplit> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待处理的分片集合
     */
    protected Collection<SplitT> pendingSplits;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public BaseEnumCheckpoint(Collection<SplitT> pendingSplits) {
        this.pendingSplits = pendingSplits;
    }

    /**
     * 获取待处理的分片
     *
     * @return 待处理分片集合
     */
    public Collection<SplitT> getPendingSplits() {
        return pendingSplits;
    }

    @Override
    public String toString() {
        return "BaseEnumCheckpoint{" +
                "pendingSplits=" + pendingSplits +
                '}';
    }
}