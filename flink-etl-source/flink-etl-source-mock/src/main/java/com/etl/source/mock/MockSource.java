package com.etl.source.mock;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

/**
 * Mock Source 占位实现
 * TODO: Task 10 将完整实现此类的分片逻辑、Enumerator 和 Reader
 */
@Slf4j
public class MockSource extends AbstractSplitSource<MockSplit, MockCheckpoint> {

    public MockSource(SourceConfig config) {
        super(config);
        log.warn("MockSource 当前为占位实现，完整功能将在 Task 10 中实现");
    }

    @Override
    public Boundedness getBoundedness() {
        // Mock Source 默认为有界数据源（批处理模式）
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<MockSplit, MockCheckpoint> createEnumerator(SplitEnumeratorContext<MockSplit> enumContext) {
        throw new UnsupportedOperationException("MockSource 尚未实现，请等待 Task 10 完成");
    }

    @Override
    public SplitEnumerator<MockSplit, MockCheckpoint> restoreEnumerator(SplitEnumeratorContext<MockSplit> enumContext, MockCheckpoint checkpoint) {
        throw new UnsupportedOperationException("MockSource 尚未实现，请等待 Task 10 完成");
    }

    @Override
    public SourceReader<Row, MockSplit> createReader(SourceReaderContext readerContext) {
        throw new UnsupportedOperationException("MockSource 尚未实现，请等待 Task 10 完成");
    }

    @Override
    public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
        throw new UnsupportedOperationException("MockSource 尚未实现，请等待 Task 10 完成");
    }

    @Override
    public SimpleVersionedSerializer<MockCheckpoint> getEnumeratorCheckpointSerializer() {
        throw new UnsupportedOperationException("MockSource 尚未实现，请等待 Task 10 完成");
    }
}