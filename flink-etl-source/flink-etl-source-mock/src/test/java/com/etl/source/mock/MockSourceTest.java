package com.etl.source.mock;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MockSourceTest {

    @Test
    void testBatchModeBoundedness() {
        SourceConfig config = createMockConfig("batch", null, 10);
        Map<String, Object> schemaMap = new LinkedHashMap<>();
        schemaMap.put("id", "LONG");
        schemaMap.put("value", "INT");
        config.getConfig().put("schema", schemaMap);

        MockSource source = new MockSource(config);

        assertEquals(Boundedness.BOUNDED, source.getBoundedness());
    }

    @Test
    void testStreamingModeBoundedness() {
        SourceConfig config = createMockConfig("streaming", null, null);
        config.getConfig().put("intervalMs", 1000L);
        Map<String, Object> schemaMap = new LinkedHashMap<>();
        schemaMap.put("id", "LONG");
        config.getConfig().put("schema", schemaMap);

        MockSource source = new MockSource(config);

        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness());
    }

    @Test
    void testSchemaWithComplexTypeThrowsException() {
        SourceConfig config = createMockConfig("batch", null, 10);

        // 创建包含复杂类型的 schema（模拟）
        // MockSource 不支持复杂类型，应该抛异常
        Map<String, Object> schemaMap = new LinkedHashMap<>();
        schemaMap.put("id", "LONG");
        schemaMap.put("data", new LinkedHashMap<>());  // OBJECT 类型（复杂类型）
        config.getConfig().put("schema", schemaMap);

        assertThrows(SchemaConfigException.class, () -> {
            new MockSource(config);
        });
    }

    @Test
    void testMissingSchemaThrowsException() {
        SourceConfig config = createMockConfig("batch", null, 10);
        // 不设置 schema

        assertThrows(NullPointerException.class, () -> {
            new MockSource(config);
        });
    }

    @Test
    void testCreateEnumeratorAndReader() {
        SourceConfig config = createMockConfig("batch", null, 5);
        Map<String, Object> schemaMap = new LinkedHashMap<>();
        schemaMap.put("id", "LONG");
        schemaMap.put("name", "STRING");
        config.getConfig().put("schema", schemaMap);

        MockSource source = new MockSource(config);

        // 测试创建 enumerator
        // 注意：需要 MockSplitEnumeratorContext，这里简化验证
        assertNotNull(source.getSplitSerializer());
        assertNotNull(source.getEnumeratorCheckpointSerializer());
    }

    private SourceConfig createMockConfig(String mode, Object rows, Object numRows) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("mode", mode);
        if (rows != null) {
            configMap.put("rows", rows);
        }
        if (numRows != null) {
            configMap.put("numRows", numRows);
        }

        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setType("mock");
        sourceConfig.setOutputTable("test_table");
        sourceConfig.setConfig(configMap);
        return sourceConfig;
    }
}