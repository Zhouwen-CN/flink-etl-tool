package com.etl.core.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/**
 * 分片接口
 * 所有分片类型的基础接口，继承 Flink 的 SourceSplit
 *
 * @see SourceSplit
 */
public interface BaseSourceSplit extends SourceSplit, Serializable {

    /**
     * 获取分片 ID
     * 用于唯一标识一个分片
     *
     * @return 分片 ID
     */
    @Override
    String splitId();
}