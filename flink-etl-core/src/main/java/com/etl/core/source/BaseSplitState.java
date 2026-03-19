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
public abstract class BaseSplitState<SplitT extends BaseSourceSplit> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 关联的分片 */
    protected SplitT split;

    /** 已读取的记录数 */
    protected long recordsRead;

    /**
     * 构造函数
     *
     * @param split 关联的分片
     */
    public BaseSplitState(SplitT split) {
        this.split = split;
        this.recordsRead = 0;
    }

    /**
     * 增加已读取记录数
     *
     * @param count 增加的数量
     */
    public void addRecordsRead(long count) {
        this.recordsRead += count;
    }

    @Override
    public String toString() {
        return "BaseSplitState{" +
                "split=" + split +
                ", recordsRead=" + recordsRead +
                '}';
    }
}