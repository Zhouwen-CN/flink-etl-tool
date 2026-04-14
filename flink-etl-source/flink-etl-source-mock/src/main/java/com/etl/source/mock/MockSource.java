package com.etl.source.mock;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.source.mock.config.MockSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Mock Source 主类
 * 支持固定数据和随机生成两种模式
 */
@Slf4j
public class MockSource extends AbstractSplitSource<MockSplit, MockEnumCheckpoint> {

    private final MockSourceConfig mockConfig;

    public MockSource(SourceConfig config) {
        super(config);

        // 1. 获取运行模式（从 job.mode 传入）
        MockSourceConfig.RunMode runMode = getRunModeFromJobConfig(config);

        // 2. Schema 校验
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");
        validateSimpleTypesOnly(schema);

        // 3. 配置参数获取
        List<MockSourceConfig.RowData> rows = parseRowsConfig(config);
        Integer numRows = config.getInteger("numRows", 10);
        Long intervalMs = config.getLong("intervalMs", 1000L);

        // 4. 配置冲突警告
        if (runMode == MockSourceConfig.RunMode.BATCH && config.contains("intervalMs")) {
            log.warn("batch 模式下 intervalMs 参数被忽略");
        }
        if (runMode == MockSourceConfig.RunMode.STREAMING &&
            (config.contains("rows") || config.contains("numRows"))) {
            log.warn("streaming 模式下 rows/numRows 参数被忽略");
        }

        // 5. 封装配置对象
        this.mockConfig = MockSourceConfig.builder()
            .runMode(runMode)
            .schema(schema)
            .rows(rows)
            .numRows(runMode == MockSourceConfig.RunMode.BATCH ?
                (rows != null ? rows.size() : numRows) : null)
            .intervalMs(runMode == MockSourceConfig.RunMode.STREAMING ? intervalMs : null)
            .build();

        log.info("创建 MockSource: runMode={}, rows={}, numRows={}, intervalMs={}",
            runMode, rows != null ? rows.size() : null,
            mockConfig.getNumRows(), mockConfig.getIntervalMs());
    }

    private MockSourceConfig.RunMode getRunModeFromJobConfig(SourceConfig config) {
        // 注意：SourceConfig 在 JobBuilder 创建时，会从 JobConfig 传递 mode 参数
        // 参考 JobBuilder.build() 中对 SourceConfig 的初始化逻辑
        String mode = config.getString("mode", "batch");
        return MockSourceConfig.RunMode.valueOf(mode.toUpperCase());
    }

    @SuppressWarnings("unchecked")
    private List<MockSourceConfig.RowData> parseRowsConfig(SourceConfig config) {
        if (!config.contains("rows")) {
            return null;
        }

        // 解析 rows 配置（JSON 数组）
        Object rowsObj = config.get("rows");
        if (!(rowsObj instanceof List)) {
            throw new IllegalArgumentException("配置项 'rows' 必须是列表类型");
        }

        List<Map<String, Object>> rowsList = (List<Map<String, Object>>) rowsObj;
        List<MockSourceConfig.RowData> rowsData = new ArrayList<>();

        for (Map<String, Object> rowMap : rowsList) {
            MockSourceConfig.RowData rowData = new MockSourceConfig.RowData();
            rowData.setKind((String) rowMap.get("kind"));
            rowData.setData((Map<String, Object>) rowMap.get("data"));
            rowsData.add(rowData);
        }

        return rowsData;
    }

    @Override
    public Boundedness getBoundedness() {
        return mockConfig.getRunMode() == MockSourceConfig.RunMode.BATCH
            ? Boundedness.BOUNDED
            : Boundedness.CONTINUOUS_UNBOUNDED;
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

    /**
     * 校验 schema 不包含复杂类型
     */
    private void validateSimpleTypesOnly(EtlSchema schema) {
        for (int i = 0; i < schema.getFieldCount(); i++) {
            TypeInformation<?> type = schema.getFieldType(i);
            if (isComplexType(type)) {
                throw new SchemaConfigException(
                    "Mock Source 不支持复杂类型字段 '" + schema.getFieldName(i) + "'。" +
                    "只支持简单类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
            }
        }
    }

    /**
     * 检查是否为复杂类型
     */
    private boolean isComplexType(TypeInformation<?> type) {
        return type instanceof RowTypeInfo
            || type instanceof BasicArrayTypeInfo
            || type instanceof ObjectArrayTypeInfo;
    }
}