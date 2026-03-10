package com.etl.core.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * 待处理分片的检查点状态
 * 用于保存 Source 枚举器的状态
 *
 * @param <SplitT> 分片类型
 */
@Public
public class PendingSplitsCheckpoint<SplitT extends SourceSplit> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Collection<SplitT> splits;

    /**
     * 创建检查点
     *
     * @param splits 待处理的分片集合
     */
    public PendingSplitsCheckpoint(Collection<SplitT> splits) {
        this.splits = splits != null ? splits : Collections.emptyList();
    }

    /**
     * 获取待处理的分片
     *
     * @return 待处理分片集合
     */
    public Collection<SplitT> getSplits() {
        return splits;
    }

    @Override
    public String toString() {
        return "PendingSplitsCheckpoint{" +
                "splits=" + splits +
                '}';
    }
}