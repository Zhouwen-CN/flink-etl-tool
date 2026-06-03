package com.etl.core.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 配置基类
 * 提供统一的 get(key, Class) 接口
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class BaseConfig implements Serializable {

    private Map<String, Object> config;

    /**
     * 根据期望类型获取配置值
     *
     * <p>支持的类型：String、Integer、Long、Boolean、List、Map。
     * 不支持的类型抛 IllegalArgumentException。
     * 当 config 为 null、key 不存在或值为 null 时返回 null。
     *
     * @param key   配置键
     * @param clazz 期望的返回类型
     * @param <T>   返回类型参数
     * @return 转换后的配置值；不存在时返回 null
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> clazz) {
        Object value = this.get(key);
        if (value == null) {
            return null;
        }
        if (clazz == String.class) {
            return (T) String.valueOf(value);
        }
        if (clazz == Integer.class) {
            if (value instanceof Integer) {
                return (T) value;
            }
            try {
                return (T) Integer.valueOf(Integer.parseInt(String.valueOf(value)));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "配置项 '" + key + "' 的值 '" + value + "' 无法转换为整数", e);
            }
        }
        if (clazz == Long.class) {
            if (value instanceof Long) {
                return (T) value;
            }
            if (value instanceof Integer) {
                return (T) Long.valueOf(((Integer) value).longValue());
            }
            try {
                return (T) Long.valueOf(Long.parseLong(String.valueOf(value)));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "配置项 '" + key + "' 的值 '" + value + "' 无法转换为长整数", e);
            }
        }
        if (clazz == Boolean.class) {
            if (value instanceof Boolean) {
                return (T) value;
            }
            return (T) Boolean.valueOf(Boolean.parseBoolean(String.valueOf(value)));
        }
        if (clazz == List.class) {
            if (!(value instanceof List<?>)) {
                throw new IllegalArgumentException("配置项 '" + key + "' 不是列表类型");
            }
            List<String> list = new ArrayList<>();
            for (Object item : (List<?>) value) {
                list.add(String.valueOf(item));
            }
            return (T) list;
        }
        if (clazz == Map.class) {
            if (!(value instanceof Map<?, ?>)) {
                throw new IllegalArgumentException("配置项 '" + key + "' 不是映射类型");
            }
            Map<String, Object> map = new LinkedHashMap<>();
            ((Map<?, ?>) value).forEach((k, v) -> map.put(String.valueOf(k), v));
            return (T) map;
        }
        throw new IllegalArgumentException("不支持的类型: " + clazz.getName());
    }

    /**
     * 根据期望类型获取配置值，未设置时返回默认值
     *
     * <p>类型转换异常仍然抛出，不会被默认值掩盖。
     */
    public <T> T get(String key, Class<T> clazz, T defaultValue) {
        T value = get(key, clazz);
        return value != null ? value : defaultValue;
    }

    /**
     * 直接获取原始对象（不做类型转换）
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }

    /**
     * 检查配置项是否存在
     */
    public boolean contains(String key) {
        return config != null && config.containsKey(key);
    }
}
