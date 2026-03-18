package com.etl.core.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Transform 配置类
 * 定义数据转换的基本配置
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransformConfig {
    private String type;
    private String inputTable;
    private String outputTable;
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
     * 获取配置值
     *
     * @param key 配置键
     * @return 配置值
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }
}