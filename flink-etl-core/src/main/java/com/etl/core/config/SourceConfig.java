package com.etl.core.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Source 配置类
 * 定义数据源的基本配置
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

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

    /**
     * 获取布尔类型的配置值
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        if (config == null) {
            return defaultValue;
        }
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(String.valueOf(value));
    }

    /**
     * 获取字符串列表类型的配置值
     *
     * @param key 配置键
     * @return 配置值列表，如果不存在返回 null
     */
    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof List) {
            return (List<String>) value;
        }
        throw new IllegalArgumentException("配置项 " + key + " 不是列表类型");
    }
}