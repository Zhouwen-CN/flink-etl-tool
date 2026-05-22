package com.etl.connector.http.source;

import com.etl.core.source.AbstractEnumCheckpoint;

import java.util.Collection;

/**
 * HTTP 分片枚举器检查点
 */
public class HttpEnumCheckpoint extends AbstractEnumCheckpoint<HttpSplit> {

    private static final long serialVersionUID = 1L;
    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public HttpEnumCheckpoint(Collection<HttpSplit> pendingSplits) {
        super(pendingSplits);
    }
}