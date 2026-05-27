package com.etl.connector.mock.source;

import com.etl.connector.mock.source.config.MockSourceConfig;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

/**
 * Mock Source 分片枚举器
 * 在 start() 时创建单个 MockSplit 并分配到队列
 */
@Slf4j
public class MockSplitEnumerator extends AbstractSplitEnumerator<MockSplit> {

    private final MockSourceConfig mockConfig;

    public MockSplitEnumerator(
            SplitEnumeratorContext<MockSplit> context,
            MockSourceConfig mockConfig) {
        super(context);
        this.mockConfig = mockConfig;
    }

    public MockSplitEnumerator(
            SplitEnumeratorContext<MockSplit> context,
            BaseEnumCheckpoint<MockSplit> checkpoint,
            MockSourceConfig mockConfig) {
        super(context, checkpoint);
        this.mockConfig = mockConfig;
    }

    @Override
    public void start() {
        MockSplit split = new MockSplit(mockConfig);
        pendingSplits.add(split);
        log.info("Mock Source 创建单分片: {}", split.splitId());
    }

    @Override
    public void close() {
        log.info("MockSplitEnumerator 关闭");
    }
}
