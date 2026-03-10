package com.etl.core.config;

import java.util.Map;

/**
 * Transform 配置类
 * 定义数据转换的基本配置
 */
public class TransformConfig {
    private String type;
    private String name;
    private Map<String, Object> properties;

    public TransformConfig() {
    }

    public TransformConfig(String type, String name, Map<String, Object> properties) {
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