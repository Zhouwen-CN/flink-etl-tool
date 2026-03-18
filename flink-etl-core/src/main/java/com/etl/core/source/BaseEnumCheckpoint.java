package com.etl.core.source;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collection;

/**
 * 枚举器检查点抽象类
 * 用于保存 SplitEnumerator 的状态，支持故障恢复
 *
 * @param <SplitT> 分片类型
 */
@Getter
public abstract class BaseEnumCheckpoint<SplitT extends BaseSourceSplit> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 待处理的分片集合 */
    protected final Collection<SplitT> pendingSplits;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public BaseEnumCheckpoint(Collection<SplitT> pendingSplits) {
        this.pendingSplits = pendingSplits;
    }

    @Override
    public String toString() {
        return "BaseEnumCheckpoint{" +
                "pendingSplits=" + pendingSplits +
                '}';
    }
}