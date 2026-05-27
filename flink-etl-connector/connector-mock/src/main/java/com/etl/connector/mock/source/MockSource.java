package com.etl.connector.mock.source;

import com.etl.connector.mock.source.config.MockSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Mock Source 主类
 * <p>
 * 始终以 CONTINUOUS_UNBOUNDED 模式运行，行为由用户配置决定：
 * <ul>
 *   <li>配置了 data 或 numRows：数据读取完毕后程序自然停止</li>
 *   <li>未配置 data 和 numRows：按 intervalMs（默认 1000ms）持续生成数据</li>
 * </ul>
 */
@Slf4j
public class MockSource extends AbstractSplitSource<MockSplit> {

    private final MockSourceConfig mockConfig;
    private final boolean bounded;

    public MockSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        super(config);
        this.mockConfig = MockSourceConfig.fromSourceConfig(config, runtimeMode);
        this.bounded = runtimeMode == RuntimeExecutionMode.BATCH;
    }

    @Override
    public Boundedness getBoundedness() {
        if (bounded) {
            return Boundedness.BOUNDED;
        }
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MockSplit, BaseEnumCheckpoint<MockSplit>> createEnumerator(
            SplitEnumeratorContext<MockSplit> enumContext) {
        log.info("创建 MockSplitEnumerator");
        return new MockSplitEnumerator(enumContext, mockConfig);
    }

    @Override
    public SplitEnumerator<MockSplit, BaseEnumCheckpoint<MockSplit>> restoreEnumerator(
            SplitEnumeratorContext<MockSplit> enumContext,
            BaseEnumCheckpoint<MockSplit> checkpoint) {
        log.info("从检查点恢复 MockSplitEnumerator");
        return new MockSplitEnumerator(enumContext, checkpoint, mockConfig);
    }

    @Override
    public SourceReader<Row, MockSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 MockSourceReader");

        Supplier<AbstractSplitReader<Row, MockSplit>> splitReaderSupplier = MockSplitReader::new;

        return new MockSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<BaseEnumCheckpoint<MockSplit>> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
