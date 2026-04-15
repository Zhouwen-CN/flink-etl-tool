package com.etl.source.mock;

import com.etl.core.source.BaseSplitEnumerator;
import com.etl.source.mock.config.MockSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock Source 分片枚举器
 * 在 start() 时创建单个 MockSplit 并分配到队列
 */
@Slf4j
public class MockSplitEnumerator
        extends BaseSplitEnumerator<MockSplit, MockEnumCheckpoint> {

    private final MockSourceConfig mockConfig;

    public MockSplitEnumerator(
            SplitEnumeratorContext<MockSplit> context,
            MockSourceConfig mockConfig) {
        super(context);
        this.mockConfig = mockConfig;
    }

    public MockSplitEnumerator(
            SplitEnumeratorContext<MockSplit> context,
            MockEnumCheckpoint checkpoint,
            MockSourceConfig mockConfig) {
        super(context, checkpoint);
        this.mockConfig = mockConfig;
    }

    @Override
    public void start() {
        // 创建固定的单分片
        MockSplit split = new MockSplit(mockConfig);

        // 添加到待分配队列
        pendingSplits.add(split);

        log.info("Mock Source 创建单分片: {}", split.splitId());
    }

    @Override
    public MockEnumCheckpoint snapshotState(long checkpointId) {
        List<MockSplit> pending = new ArrayList<>(pendingSplits);
        return new MockEnumCheckpoint(pending);
    }

    @Override
    public void close() {
        log.info("MockSplitEnumerator 关闭");
    }
}