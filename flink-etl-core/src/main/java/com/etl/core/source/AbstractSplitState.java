package com.etl.core.source;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 分片状态抽象类
 * 用于跟踪分片的读取进度，支持 Checkpoint 恢复
 *
 * @param <SplitT> 分片类型
 */
@Getter
@Setter
public abstract class AbstractSplitState<SplitT extends BaseSourceSplit> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 关联的分片 */
    protected SplitT split;

    /**
     * 构造函数
     *
     * @param split 关联的分片
     */
    public AbstractSplitState(SplitT split) {
        this.split = split;
    }
}