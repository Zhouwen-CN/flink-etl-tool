package com.etl.connector.localfile.source;

import com.etl.core.source.AbstractSplitState;
import lombok.Getter;
import lombok.Setter;

/**
 * 文件分片状态
 * 用于跟踪文件分片的读取进度
 *
 * <p>继承自 AbstractSplitState，复用 recordsRead 计数功能
 */
@Getter
@Setter
public class LocalFileSplitState extends AbstractSplitState<LocalFileSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param split 文件分片
     */
    public LocalFileSplitState(LocalFileSplit split) {
        super(split);
    }

    @Override
    public String toString() {
        return "LocalFileSplitState{" +
                "split=" + getSplit() +
                ", recordsRead=" + getRecordsRead() +
                '}';
    }
}