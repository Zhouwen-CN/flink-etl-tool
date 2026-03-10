package com.etl.core.config;

import java.util.Map;

/**
 * Source 配置类
 * 定义数据源的基本配置
 */
public class SourceConfig {
    private String type;
    private String name;
    private Map<String, Object> properties;

    public SourceConfig() {
    }

    public SourceConfig(String type, String name, Map<String, Object> properties) {
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