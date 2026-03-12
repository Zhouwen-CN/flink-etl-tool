package com.etl.core.config;

import java.util.List;

/**
 * Job 完整配置
 */
public class JobConfig {
    private JobMeta job;
    private SourceConfig source;
    private List<TransformConfig> transforms;
    private SinkConfig sink;

    public JobMeta getJob() {
        return job;
    }

    public void setJob(JobMeta job) {
        this.job = job;
    }

    public SourceConfig getSource() {
        return source;
    }

    public void setSource(SourceConfig source) {
        this.source = source;
    }

    public List<TransformConfig> getTransforms() {
        return transforms;
    }

    public void setTransforms(List<TransformConfig> transforms) {
        this.transforms = transforms;
    }

    public SinkConfig getSink() {
        return sink;
    }

    public void setSink(SinkConfig sink) {
        this.sink = sink;
    }
}