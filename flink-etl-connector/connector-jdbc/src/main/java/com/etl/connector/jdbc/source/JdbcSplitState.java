package com.etl.connector.jdbc.source;

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
public class JdbcSplitState extends BaseSplitState<JdbcSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param split 范围分片
     */
    public JdbcSplitState(JdbcSplit split) {
        super(split);
    }

    @Override
    public String toString() {
        return "RangeSplitState{" +
                "split=" + getSplit() +
                ", recordsRead=" + getRecordsRead() +
                '}';
    }
}