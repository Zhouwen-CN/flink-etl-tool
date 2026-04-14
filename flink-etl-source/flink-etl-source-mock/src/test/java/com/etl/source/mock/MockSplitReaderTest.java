package com.etl.source.mock;

import com.etl.core.schema.EtlSchema;
import com.etl.source.mock.config.MockSourceConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MockSplitReaderTest {

    @Test
    void testBatchModeWithFixedRows() throws Exception {
        // 创建 schema
        String[] fieldNames = {"id", "value"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.LONG, Types.INT};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<MockSourceConfig.RowData> rowsData = Arrays.asList(
            createRowData("INSERT", Map.of("id", 1L, "value", 100)),
            createRowData("INSERT", Map.of("id", 2L, "value", 200))
        );

        MockSourceConfig config = MockSourceConfig.builder()
            .runMode(MockSourceConfig.RunMode.BATCH)
            .schema(schema)
            .rows(rowsData)
            .build();

        MockSplitReader reader = new MockSplitReader(config);

        // Verify reader is created without exception
        assertNotNull(reader);

        reader.close();
    }

    @Test
    void testBatchModeWithRandomRows() throws Exception {
        // 创建 schema
        String[] fieldNames = {"id", "name"};
        org.apache.flink.api.common.typeinfo.TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        MockSourceConfig config = MockSourceConfig.builder()
            .runMode(MockSourceConfig.RunMode.BATCH)
            .schema(schema)
            .numRows(10)
            .build();

        MockSplitReader reader = new MockSplitReader(config);

        // Verify reader is created without exception
        assertNotNull(reader);

        reader.close();
    }

    private MockSourceConfig.RowData createRowData(String kind, Map<String, Object> data) {
        MockSourceConfig.RowData rowData = new MockSourceConfig.RowData();
        rowData.setKind(kind);
        rowData.setData(data);
        return rowData;
    }
}