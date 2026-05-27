package com.etl.core.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 配置基类
 * 提供通用的配置获取方法
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class BaseConfig implements Serializable {

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
     * 获取字符串类型的配置值，支持默认值
     *
     * @param key          配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public String getString(String key, String defaultValue) {
        String value = getString(key);
        return value != null ? value : defaultValue;
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
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 '" + key + "' 的值 '" + value + "' 无法转换为整数", e);
        }
    }

    /**
     * 获取整数类型的配置值，支持默认值
     *
     * @param key          配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public Integer getInteger(String key, Integer defaultValue) {
        Integer value = getInteger(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取布尔类型的配置值
     *
     * @param key          配置键
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


    public Long getLong(String key) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 '" + key + "' 的值 '" + value + "' 无法转换为长整数", e);
        }
    }

    /**
     * 获取长整数类型的配置值，支持默认值
     *
     * @param key          配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public Long getLong(String key, Long defaultValue) {
        Long value = getLong(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 检查配置项是否存在
     *
     * @param key 配置键
     * @return 如果配置项存在则返回 true，否则返回 false
     */
    public boolean contains(String key) {
        return config != null && config.containsKey(key);
    }

    /**
     * 获取列表类型的配置值
     *
     * @param key 配置键
     * @return 配置值列表
     */
    public List<String> getList(String key) {

        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof List<?>) {
            val list = new ArrayList<String>();
            ((List<?>) value).forEach(item -> list.add(String.valueOf(item)));
            return list;
        }
        throw new IllegalArgumentException("配置项 '" + key + "' 不是列表类型");
    }

    /**
     * 获取配置值
     *
     * @param key 配置键
     * @return 配置值
     */
    public Object getObject(String key) {
        return config != null ? config.get(key) : null;
    }

    /**
     * 获取映射类型的配置值
     *
     * @param key 配置键
     * @return 配置值映射
     */
    public Map<String, Object> getMap(String key) {
        if (config == null) {
            return null;
        }
        Object value = config.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?>) {
            Map<String, Object> map = new HashMap<>();
            ((Map<?, ?>) value).forEach((k, v) -> map.put(String.valueOf(k), v));
            return map;
        }
        throw new IllegalArgumentException("配置项 '" + key + "' 不是映射类型");
    }
}