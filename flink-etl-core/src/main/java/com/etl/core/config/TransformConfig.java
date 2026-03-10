package com.etl.core.config;

import java.util.Map;

/**
 * Transform 配置类
 * 定义数据转换的基本配置
 */
public class TransformConfig {
    private String type;
    private Map<String, Object> config;

    public TransformConfig() {
    }

    public TransformConfig(String type, Map<String, Object> config) {
        this.type = type;
        this.config = config;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * 获取配置值
     *
     * @param key 配置键
     * @return 配置值
     */
    public Object get(String key) {
        return config.get(key);
    }
}