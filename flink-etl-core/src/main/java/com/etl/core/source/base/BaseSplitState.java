package com.etl.core.source.base;

import java.io.Serializable;

/**
 * 分片状态抽象类
 * 用于跟踪分片的读取进度，支持 Checkpoint 恢复
 *
 * @param <SplitT> 分片类型
 */
public abstract class BaseSplitState<SplitT extends BaseSourceSplit> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 关联的分片
     */
    protected SplitT split;

    /**
     * 构造函数
     *
     * @param split 关联的分片
     */
    public BaseSplitState(SplitT split) {
        this.split = split;
    }

    /**
     * 获取关联的分片
     *
     * @return 分片
     */
    public SplitT getSplit() {
        return split;
    }

    /**
     * 更新关联的分片
     *
     * @param split 新的分片
     */
    public void setSplit(SplitT split) {
        this.split = split;
    }

    @Override
    public String toString() {
        return "BaseSplitState{" +
                "split=" + split +
                '}';
    }
}