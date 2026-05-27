package com.etl.connector.http.source;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.Collections;

/**
 * HTTP 分片枚举器
 * 单分片模式，创建一个包含完整配置的分片
 */
@Slf4j
public class HttpSplitEnumerator extends AbstractSplitEnumerator<HttpSplit> {

    private final HttpSourceConfig httpSourceConfig;

    public HttpSplitEnumerator(
            SplitEnumeratorContext<HttpSplit> context,
            HttpSourceConfig httpSourceConfig) {
        super(context);
        this.httpSourceConfig = httpSourceConfig;
    }

    public HttpSplitEnumerator(
            SplitEnumeratorContext<HttpSplit> context,
            BaseEnumCheckpoint<HttpSplit> checkpoint,
            HttpSourceConfig httpSourceConfig) {
        super(context, checkpoint);
        this.httpSourceConfig = httpSourceConfig;
    }

    @Override
    public void start() {
        log.info("HttpSplitEnumerator 启动，URL: {}", httpSourceConfig.getUrl());

        HttpSplit split = new HttpSplit("http-split-0", httpSourceConfig);
        addPendingSplits(Collections.singletonList(split));
        log.info("创建 HTTP 分片: {}", split);
    }

    @Override
    public void close() throws IOException {
        log.info("HttpSplitEnumerator 关闭");
    }
}
