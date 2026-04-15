package com.etl.connector.http.source;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;
import lombok.Setter;

/**
 * HTTP 分片状态
 */
@Getter
@Setter
public class HttpSplitState extends BaseSplitState<HttpSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param split HTTP 分片
     */
    public HttpSplitState(HttpSplit split) {
        super(split);
    }

    @Override
    public String toString() {
        return "HttpSplitState{" +
                "split=" + getSplit() +
                ", recordsRead=" + getRecordsRead() +
                '}';
    }
}