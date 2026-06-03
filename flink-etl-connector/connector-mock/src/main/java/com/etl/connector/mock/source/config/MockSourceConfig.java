package com.etl.connector.mock.source.config;

import com.etl.core.config.SourceConfig;
import com.etl.core.exception.SchemaConfigException;
import com.etl.core.schema.EtlSchema;
import com.etl.core.utils.JsonUtils;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Mock Source 配置封装类
 */
@Data
@Builder
@Slf4j
public class MockSourceConfig implements Serializable {

    /** 是否有界 */
    private final boolean bounded;

    /** Schema 定义 */
    private final EtlSchema schema;

    /**
     * 固定数据配置（JSON 数组）
     * 配置后数据读取完毕程序自然停止
     */
    private final JsonNode data;

    /**
     * 随机生成的行数
     * 配置后数据读取完毕程序自然停止；未配置时按 intervalMs 持续生成
     */
    private final Integer numRows;

    /**
     * 数据生成间隔（毫秒）
     * 仅在未配置 data 和 numRows 时生效
     */
    private final Long intervalMs;

    /**
     * 从 SourceConfig 创建 MockSourceConfig，在此完成所有参数校验
     */
    public static MockSourceConfig fromSourceConfig(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        boolean bounded = runtimeMode == RuntimeExecutionMode.BATCH;

        // 1. Schema 校验
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");

        // 2. 检查是否存在复杂类型
        if (schema.hasComplexType()) {
            throw new SchemaConfigException("Mock Source 只支持简单类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
        }

        // 3. 解析用户配置
        JsonNode data = parseDataConfig(config);
        Integer numRows = config.get("numRows", Integer.class, 10);
        Long intervalMs = config.get("intervalMs", Long.class, 1000L);

        log.info("创建 MockSource: bounded={}, data={}, numRows={}, intervalMs={}",
                bounded,
                data != null ? data.size() : null,
                numRows,
                intervalMs
        );

        return MockSourceConfig.builder()
                .bounded(bounded)
                .schema(schema)
                .data(data)
                .numRows(numRows)
                .intervalMs(intervalMs)
                .build();
    }

    /**
     * 解析 data 配置
     */
    private static JsonNode parseDataConfig(SourceConfig config) {
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
}
