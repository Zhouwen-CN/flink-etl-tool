package com.etl.source.http;

import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.BaseSourceReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * HTTP Source Reader
 * 继承 BaseSourceReader，自动处理线程模型和状态管理
 */
@Slf4j
public class HttpSourceReader extends BaseSourceReader<Row, Row, HttpSplit, HttpSplitState> {

    public HttpSourceReader(
            Supplier<BaseSplitReader<Row, HttpSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new HttpRecordEmitter(), new Configuration(), context);
    }

    @Override
    public HttpSplitState initializedState(HttpSplit split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new HttpSplitState(split);
    }

    @Override
    protected HttpSplit toSplitType(String splitId, HttpSplitState splitState) {
        return splitState.getSplit();
    }
}