package com.etl.connector.localfile.source;

import com.etl.core.source.BaseEnumCheckpoint;

import java.util.Collection;

/**
 * 文件分片枚举器检查点
 * 用于保存 LocalFileSplitEnumerator 的状态
 */
public class LocalFileEnumCheckpoint extends BaseEnumCheckpoint<LocalFileSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public LocalFileEnumCheckpoint(Collection<LocalFileSplit> pendingSplits) {
        super(pendingSplits);
    }
}