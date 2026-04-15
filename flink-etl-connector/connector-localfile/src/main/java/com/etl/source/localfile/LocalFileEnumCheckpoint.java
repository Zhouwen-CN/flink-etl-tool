package com.etl.source.localfile;

import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;

import java.util.Collection;

/**
 * 文件分片枚举器检查点
 * 用于保存 LocalFileSplitEnumerator 的状态
 */
public class LocalFileEnumCheckpoint extends BaseEnumCheckpoint<LocalFileSplit> {

    private static final long serialVersionUID = DefaultCheckpointSerializer.VERSION;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public LocalFileEnumCheckpoint(Collection<LocalFileSplit> pendingSplits) {
        super(pendingSplits);
    }
}