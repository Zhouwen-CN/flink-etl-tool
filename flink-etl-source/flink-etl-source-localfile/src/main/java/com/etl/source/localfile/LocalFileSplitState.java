package com.etl.source.localfile;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;
import lombok.Setter;

/**
 * 文件分片状态
 * 用于跟踪文件分片的读取进度
 */
@Getter
@Setter
public class LocalFileSplitState extends BaseSplitState<LocalFileSplit> {

    private static final long serialVersionUID = 1L;

    /** 已读取的记录数 */
    private long recordsRead;

    /**
     * 构造函数
     *
     * @param split 文件分片
     */
    public LocalFileSplitState(LocalFileSplit split) {
        super(split);
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
        return "LocalFileSplitState{" +
                "split=" + getSplit() +
                ", recordsRead=" + recordsRead +
                '}';
    }
}