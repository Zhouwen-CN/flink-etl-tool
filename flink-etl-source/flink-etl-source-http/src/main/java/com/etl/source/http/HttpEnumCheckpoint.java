package com.etl.source.http;

import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;

import java.util.Collection;

/**
 * HTTP 分片枚举器检查点
 */
public class HttpEnumCheckpoint extends BaseEnumCheckpoint<HttpSplit> {

    private static final long serialVersionUID = DefaultCheckpointSerializer.VERSION;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public HttpEnumCheckpoint(Collection<HttpSplit> pendingSplits) {
        super(pendingSplits);
    }
}