package com.etl.core.source;

import com.etl.core.source.base.BaseEnumCheckpoint;

import java.util.Collection;

/**
 * 范围分片枚举器检查点
 * 用于保存 JdbcSplitEnumerator 的状态
 */
public class RangeEnumCheckpoint extends BaseEnumCheckpoint<RangeSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public RangeEnumCheckpoint(Collection<RangeSplit> pendingSplits) {
        super(pendingSplits);
    }
}