package com.etl.connector.http.source;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.core.source.BaseSourceSplit;
import lombok.Getter;

/**
 * HTTP 分片
 * 单分片模式，包含完整的请求配置
 */
@Getter
public class HttpSplit implements BaseSourceSplit {

    private static final long serialVersionUID = 1L;

    /** 分片 ID */
    private final String splitId;

    /** HTTP 配置 */
    private final HttpSourceConfig config;

    /**
     * 构造函数
     *
     * @param splitId 分片 ID
     * @param config  HTTP 配置
     */
    public HttpSplit(String splitId, HttpSourceConfig config) {
        this.splitId = splitId;
        this.config = config;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "HttpSplit{" +
                "splitId='" + splitId + '\'' +
                ", url='" + config.getUrl() + '\'' +
                '}';
    }
}