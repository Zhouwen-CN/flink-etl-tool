package com.etl.core.config;

/**
 * Job 完整配置
 */
public class JobConfig {
    private JobMeta job;
    private SourceConfig source;
    private TransformConfig transform;
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

    public TransformConfig getTransform() {
        return transform;
    }

    public void setTransform(TransformConfig transform) {
        this.transform = transform;
    }

    public SinkConfig getSink() {
        return sink;
    }

    public void setSink(SinkConfig sink) {
        this.sink = sink;
    }
}