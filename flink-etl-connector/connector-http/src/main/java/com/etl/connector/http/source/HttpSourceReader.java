package com.etl.connector.http.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.BaseRecordEmitter;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * HTTP Source Reader
 * 继承 AbstractSourceReader，自动处理线程模型和状态管理
 */
public class HttpSourceReader extends AbstractSourceReader<Row, Row, HttpSplit> {

    public HttpSourceReader(
            Supplier<AbstractSplitReader<Row, HttpSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new BaseRecordEmitter<>(context), context);
    }
}
