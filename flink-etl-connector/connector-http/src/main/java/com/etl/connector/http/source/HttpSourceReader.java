package com.etl.connector.http.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.BaseRecordEmitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * HTTP Source Reader
 * 继承 AbstractSourceReader，自动处理线程模型和状态管理
 */
@Slf4j
public class HttpSourceReader extends AbstractSourceReader<Row, Row, HttpSplit, HttpSplitState> {

    public HttpSourceReader(
            Supplier<AbstractSplitReader<Row, HttpSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new BaseRecordEmitter<>(context), context);
    }

    @Override
    public HttpSplitState initializedState(HttpSplit split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new HttpSplitState(split);
    }
}