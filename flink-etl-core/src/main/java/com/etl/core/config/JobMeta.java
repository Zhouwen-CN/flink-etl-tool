package com.etl.core.config;

/**
 * Job 元信息
 */
public class JobMeta {
    private String name;
    private String mode;

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
}