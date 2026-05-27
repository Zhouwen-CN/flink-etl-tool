package com.etl.connector.http.source;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.BaseRecordEmitter;
import com.etl.core.source.BaseSourceReader;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.flink.types.Row;

/**
 * HTTP Source 实现
 * 支持 GET/POST 请求获取 JSON 数据
 */
@Slf4j
public class HttpSource extends AbstractSplitSource<HttpSplit> {

    private final HttpSourceConfig httpSourceConfig;

    public HttpSource(SourceConfig config) {
        super(config);
        httpSourceConfig = HttpSourceConfig.fromSourceConfig(config);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<HttpSplit, BaseEnumCheckpoint<HttpSplit>> createEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new HttpSplitEnumerator(enumContext, httpSourceConfig);
    }

    @Override
    public SplitEnumerator<HttpSplit, BaseEnumCheckpoint<HttpSplit>> restoreEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext,
            BaseEnumCheckpoint<HttpSplit> checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new HttpSplitEnumerator(enumContext, checkpoint, httpSourceConfig);
    }

    @Override
    public SourceReader<Row, HttpSplit> createReader(SourceReaderContext readerContext) {
        return new BaseSourceReader<>(HttpSplitReader::new, new BaseRecordEmitter<>(readerContext), readerContext);
    }
}
