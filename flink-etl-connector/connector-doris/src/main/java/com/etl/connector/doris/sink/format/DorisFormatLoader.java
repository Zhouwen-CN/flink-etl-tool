package com.etl.connector.doris.sink.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Doris Format Plugin 加载器
 * 使用 ServiceLoader 加载所有 @AutoService 注册的实现并按 identifier 缓存
 */
public class DorisFormatLoader {

    private static final Map<String, DorisFormatPlugin> formatPlugins;

    static {
        Map<String, DorisFormatPlugin> plugins = new HashMap<>();
        ServiceLoader<DorisFormatPlugin> loader = ServiceLoader.load(DorisFormatPlugin.class);
        for (DorisFormatPlugin plugin : loader) {
            plugins.put(plugin.identifier(), plugin);
        }
        formatPlugins = plugins;
    }

    /**
     * 根据格式名获取插件，未找到返回 null
     */
    public static DorisFormatPlugin getFormatPlugin(String format) {
        return formatPlugins.get(format);
    }

    /**
     * 列出所有支持的格式
     */
    public static List<String> supportedFormats() {
        return new ArrayList<>(formatPlugins.keySet());
    }
}
