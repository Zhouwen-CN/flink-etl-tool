package com.etl.core.config;

import java.util.Map;

/**
 * Sink 配置类
 * 定义数据写入目标的基本配置
 */
public class SinkConfig {
    private String type;
    private String name;
    private Map<String, Object> properties;

    public SinkConfig() {
    }

    public SinkConfig(String type, String name, Map<String, Object> properties) {
        this.type = type;
        this.name = name;
        this.properties = properties;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}