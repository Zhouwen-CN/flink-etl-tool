package com.etl.connector.mock.source;

import com.etl.core.source.BaseSourceReader;
import com.etl.core.source.BaseSplitReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Mock Source 阅读器
 * 包装 MockSplitReader，处理分片状态
 */
public class MockSourceReader
        extends BaseSourceReader<Row, Row, MockSplit, MockSplitState> {

    public MockSourceReader(
            Supplier<BaseSplitReader<Row, MockSplit>> splitReaderSupplier,
            SourceReaderContext context) {
        super(splitReaderSupplier, new MockRecordEmitter(), context);
    }

    @Override
    public MockSplitState initializedState(MockSplit split) {
        return new MockSplitState(split);
    }

    @Override
    protected MockSplit toSplitType(String splitId, MockSplitState splitState) {
        return splitState.getSplit();
    }
}