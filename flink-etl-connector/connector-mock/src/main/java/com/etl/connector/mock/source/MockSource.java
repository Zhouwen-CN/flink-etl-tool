package com.etl.connector.mock.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.core.utils.JsonUtils;
import com.etl.connector.mock.source.config.MockSourceConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

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
public class MockSource extends AbstractSplitSource<MockSplit, MockEnumCheckpoint> {

    private final MockSourceConfig mockConfig;
    private final boolean bounded;

    public MockSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        super(config);
        this.bounded = runtimeMode == RuntimeExecutionMode.BATCH;

        // 1. Schema 校验
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");

        // 2. 检查是否存在复杂类型
        if (schema.hasComplexType()) {
            throw new SchemaConfigException("Mock Source 只支持简单类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
        }

        // 3. 解析用户配置
        JsonNode data = parseDataConfig(config);
        Integer numRows = config.getInteger("numRows", 10);
        Long intervalMs = config.getLong("intervalMs", 1000L);

        // 4. 封装配置对象
        this.mockConfig = MockSourceConfig.builder()
                .bounded(bounded)
                .schema(schema)
                .data(data)
                .numRows(numRows)
                .intervalMs(intervalMs)
                .build();

        log.info("创建 MockSource: bounded={}, data={}, numRows={}, intervalMs={}",
            bounded, data != null ? data.size() : null, mockConfig.getNumRows(), intervalMs);
    }

    private JsonNode parseDataConfig(SourceConfig config) {
        if (!config.contains("data")) {
            return null;
        }

        Object dataObj = config.get("data");
        JsonNode data = JsonUtils.valueToTree(dataObj);
        if (!data.isArray()) {
            throw new IllegalArgumentException("配置项 'data' 必须是数组类型");
        }
        return data;
    }

    @Override
    public Boundedness getBoundedness() {
        if (bounded) {
            return Boundedness.BOUNDED;
        }
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MockSplit, MockEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<MockSplit> enumContext) {
        log.info("创建 MockSplitEnumerator");
        return new MockSplitEnumerator(enumContext, mockConfig);
    }

    @Override
    public SplitEnumerator<MockSplit, MockEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<MockSplit> enumContext,
            MockEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 MockSplitEnumerator");
        return new MockSplitEnumerator(enumContext, checkpoint, mockConfig);
    }

    @Override
    public SourceReader<Row, MockSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 MockSourceReader");

        Supplier<BaseSplitReader<Row, MockSplit>> splitReaderSupplier = () ->
            new MockSplitReader(mockConfig);

        return new MockSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<MockEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
