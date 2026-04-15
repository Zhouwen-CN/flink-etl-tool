package com.etl.connector.http.source;

import com.etl.core.source.BaseSplitEnumerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * HTTP 分片枚举器
 * 单分片模式，创建一个包含完整配置的分片
 */
@Slf4j
public class HttpSplitEnumerator extends BaseSplitEnumerator<HttpSplit, HttpEnumCheckpoint> {

    private final HttpSourceConfig httpSourceConfig;

    /**
     * 构造函数
     *
     * @param context           枚举器上下文
     * @param httpSourceConfig  HTTP 配置
     */
    public HttpSplitEnumerator(
            SplitEnumeratorContext<HttpSplit> context,
            HttpSourceConfig httpSourceConfig) {
        super(context);
        this.httpSourceConfig = httpSourceConfig;
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context           枚举器上下文
     * @param checkpoint        检查点
     * @param httpSourceConfig  HTTP 配置
     */
    public HttpSplitEnumerator(
            SplitEnumeratorContext<HttpSplit> context,
            HttpEnumCheckpoint checkpoint,
            HttpSourceConfig httpSourceConfig) {
        super(context, checkpoint);
        this.httpSourceConfig = httpSourceConfig;
    }

    @Override
    public void start() {
        log.info("HttpSplitEnumerator 启动，URL: {}", httpSourceConfig.getUrl());

        // 创建单分片
        HttpSplit split = new HttpSplit("http-split-0", httpSourceConfig);

        // 添加到待处理队列
        addPendingSplits(Collections.singletonList(split));
        log.info("创建 HTTP 分片: {}", split);
    }

    @Override
    public HttpEnumCheckpoint snapshotState(long checkpointId) {
        List<HttpSplit> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new HttpEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("HttpSplitEnumerator 关闭");
    }
}