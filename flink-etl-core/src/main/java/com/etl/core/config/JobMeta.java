package com.etl.core.config;

import lombok.Data;

/**
 * Job 元信息
 */
@Data
public class JobMeta {
    private String name;
    private String mode;
    /** 并行度配置，null 表示使用 Flink 默认值 */
    private Integer parallelism;
}