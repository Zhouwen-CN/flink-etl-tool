package com.etl.connector.mock.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.BaseRecordEmitter;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Mock Source 阅读器
 * 包装 MockSplitReader，处理分片状态
 */
public class MockSourceReader extends AbstractSourceReader<Row, Row, MockSplit> {

    public MockSourceReader(
            Supplier<AbstractSplitReader<Row, MockSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new BaseRecordEmitter<>(context), context);
    }
}
