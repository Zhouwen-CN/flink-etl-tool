package com.etl.source.mock.config;

import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Mock Source 配置封装类
 */
@Data
@Builder
public class MockSourceConfig implements Serializable {

    /** 运行模式：batch 或 streaming */
    private RunMode runMode;

    /** Schema 定义 */
    private EtlSchema schema;

    /** 固定数据配置（batch 模式） */
    private List<RowData> rows;

    /** batch 模式随机生成的行数 */
    private Integer numRows;

    /** streaming 模式生成间隔（毫秒） */
    private Long intervalMs;

    /**
     * 运行模式枚举
     */
    public enum RunMode {
        BATCH,
        STREAMING
    }

    /**
     * Row 数据配置
     */
    @Data
    public static class RowData implements Serializable {
        /** RowKind: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE */
        private String kind;

        /** 字段值映射 */
        private Map<String, Object> data;
    }
}