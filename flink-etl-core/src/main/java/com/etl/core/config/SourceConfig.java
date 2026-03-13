package com.etl.core.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.Map;

/**
 * Source 配置类
 * 定义数据源的基本配置
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceConfig {
    private String type;
    private Map<String, Object> config;

    /**
     * 获取字符串类型的配置值
     *
     * @param key 配置键
     * @return 配置值
     */
    public String getString(String key) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        return value != null ? String.valueOf(value) : null;
    }

    /**
     * 获取整数类型的配置值
     *
     * @param key 配置键
     * @return 配置值
     */
    public Integer getInteger(String key) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        return Integer.parseInt(String.valueOf(value));
    }

    /**
     * 获取配置值
     *
     * @param key 配置键
     * @return 配置值
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }
}