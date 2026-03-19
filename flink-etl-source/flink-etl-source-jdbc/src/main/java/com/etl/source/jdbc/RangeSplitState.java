package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;
import lombok.Setter;

/**
 * 范围分片状态
 * 用于跟踪范围分片的读取进度
 *
 * <p>继承自 BaseSplitState，复用 recordsRead 计数功能
 */
@Getter
@Setter
public class RangeSplitState extends BaseSplitState<RangeSplit> {

    private static final long serialVersionUID = 1L;

    /** 当前读取位置，用于断点续读 */
    private long currentPosition;

    /**
     * 构造函数
     *
     * @param split 范围分片
     */
    public RangeSplitState(RangeSplit split) {
        super(split);
        this.currentPosition = split.getStart();
    }

    @Override
    public String toString() {
        return "RangeSplitState{" +
                "split=" + getSplit() +
                ", currentPosition=" + currentPosition +
                ", recordsRead=" + getRecordsRead() +
                '}';
    }
}