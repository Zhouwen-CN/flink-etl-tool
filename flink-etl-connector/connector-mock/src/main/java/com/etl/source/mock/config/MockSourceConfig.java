package com.etl.source.mock.config;

import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

/**
 * Mock Source 配置封装类
 */
@Data
@Builder
public class MockSourceConfig implements Serializable {

    /** 是否有界 */
    private boolean bounded;

    /** Schema 定义 */
    private EtlSchema schema;

    /**
     * 固定数据配置（JSON 数组）
     * 配置后数据读取完毕程序自然停止
     */
    private JsonNode data;

    /**
     * 随机生成的行数
     * 配置后数据读取完毕程序自然停止；未配置时按 intervalMs 持续生成
     */
    private Integer numRows;

    /**
     * 数据生成间隔（毫秒）
     * 仅在未配置 data 和 numRows 时生效
     */
    private Long intervalMs;
}
