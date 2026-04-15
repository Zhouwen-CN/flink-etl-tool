package com.etl.connector.kafka.source.format;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Kafka Format Plugin 加载器
 * 使用 ServiceLoader 加载所有通过 @AutoService 注册的实现类
 */
public class KafkaFormatLoader {

    // 缓存已加载的 Plugin，避免重复加载
    private static final Map<String, KafkaFormatPlugin> formatPlugins;

    /**
     * 加载所有 Format Plugin 并缓存
     */
    static {
        Map<String, KafkaFormatPlugin> plugins = new HashMap<>();
        ServiceLoader<KafkaFormatPlugin> loader = ServiceLoader.load(KafkaFormatPlugin.class);

        for (KafkaFormatPlugin plugin : loader) {
            plugins.put(plugin.identifier(), plugin);
        }

        formatPlugins = plugins;
    }

    /**
     * 根据格式名称获取 Plugin
     *
     * @param format 格式名称（如 "json", "debezium-json"）
     * @return KafkaFormatPlugin 实例，如果未找到则返回 null
     */
    public static KafkaFormatPlugin getFormatPlugin(String format) {
        return formatPlugins.get(format);
    }

    /**
     * 列出所有支持的格式
     *
     * @return 支持的格式名称列表
     */
    public static String[] supportedFormats() {
        return formatPlugins.keySet().toArray(new String[0]);
    }
}