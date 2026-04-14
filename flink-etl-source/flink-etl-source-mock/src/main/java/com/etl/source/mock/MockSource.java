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
 * <p>
 * 始终以 CONTINUOUS_UNBOUNDED 模式运行，行为由用户配置决定：
 * <ul>
 *   <li>配置了 rows 或 numRows：数据读取完毕后程序自然停止</li>
 *   <li>未配置 rows 和 numRows：按 intervalMs（默认 1000ms）持续生成数据</li>
 * </ul>
 */
@Slf4j
public class MockSource extends AbstractSplitSource<MockSplit, MockEnumCheckpoint> {

    private final MockSourceConfig mockConfig;

    public MockSource(SourceConfig config) {
        super(config);

        // 1. Schema 校验
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");
        validateSimpleTypesOnly(schema);

        // 2. 解析用户配置
        List<MockSourceConfig.RowData> rows = parseRowsConfig(config);
        Integer numRows = config.getInteger("numRows");
        Long intervalMs = config.getLong("intervalMs", 1000L);
        // 计算 numRows 值（避免 ternary auto-unboxing NPE）
        Integer numRowsValue;
        if (rows != null) {
            numRowsValue = rows.size();
        } else {
            numRowsValue = numRows;
        }

        // 3. 校验 rows 和 numRows 不能同时指定
        if (rows != null && numRows != null) {
            throw new IllegalArgumentException("rows 和 numRows 不能同时配置，请只选择其中一种");
        }

        // 4. 封装配置对象
        this.mockConfig = MockSourceConfig.builder()
            .schema(schema)
            .rows(rows)
            .numRows(numRowsValue)
            .intervalMs(intervalMs)
            .build();

        boolean bounded = rows != null || numRows != null;
        log.info("创建 MockSource: bounded={}, rows={}, numRows={}, intervalMs={}",
            bounded, rows != null ? rows.size() : null, mockConfig.getNumRows(), intervalMs);
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
        // 始终返回 CONTINUOUS_UNBOUNDED
        // 有界/无界行为由 rows/numRows 配置决定
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
