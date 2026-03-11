package com.etl.core.config;

/**
 * Job 元信息
 */
public class JobMeta {
    private String name;
    private String mode;
    private Integer parallelism;  // 并行度配置，null 表示使用 Flink 默认值

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
}