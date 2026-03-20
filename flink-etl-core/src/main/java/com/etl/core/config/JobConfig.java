package com.etl.core.config;

import lombok.Data;

import java.util.List;

/**
 * Job 完整配置
 */
@Data
public class JobConfig {
    private JobMeta job;
    private List<SourceConfig> sources;
    private List<TransformConfig> transforms;
    private List<SinkConfig> sinks;
}