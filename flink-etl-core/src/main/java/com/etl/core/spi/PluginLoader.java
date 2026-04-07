package com.etl.core.spi;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * SPI 插件加载器
 * 通过 Java SPI 机制动态加载插件
 */
@Slf4j
public class PluginLoader {

    private static ClassLoader classLoader;

    private PluginLoader() {
        // 私有构造函数，防止实例化
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PluginLoader.class.getClassLoader();
        }
    }

    private static <T> ServiceLoader<T> serviceLoader(Class<T> clazz) {
        return ServiceLoader.load(clazz, classLoader);
    }


    private static <T extends Plugin> T loadPluginByType(Class<T> clazz, String type) {
        String simpleName = clazz.getSimpleName();
        log.info("加载 {}：{}", simpleName, type);

        ServiceLoader<T> serviceLoader = serviceLoader(clazz);
        for (T plugin : serviceLoader) {
            if (plugin.identifier().equals(type)) {
                log.info("{} 加载成功：{}", simpleName, plugin.getClass().getName());
                return plugin;
            }
        }

        String errorMsg = String.format("未找到 %s：%s", simpleName, type);
        log.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
    }

    /**
     * 加载 Source 插件
     *
     * @param type 插件类型
     * @return Source 插件实例
     */
    public static SourcePlugin loadSourcePlugin(String type) {
        return loadPluginByType(SourcePlugin.class, type);
    }

    /**
     * 加载 Transform 插件
     *
     * @param type 插件类型
     * @return Transform 插件实例
     */
    public static TransformPlugin loadTransformPlugin(String type) {
        return loadPluginByType(TransformPlugin.class, type);
    }

    /**
     * 加载 Sink 插件
     *
     * @param type 插件类型
     * @return Sink 插件实例
     */
    public static SinkPlugin loadSinkPlugin(String type) {
        return loadPluginByType(SinkPlugin.class, type);
    }

    /**
     * 批量加载所有 UDF 插件
     *
     * @return 所有 UDF 插件实例列表
     * @throws IllegalStateException 如果 SPI 配置文件加载失败
     */
    public static List<UdfPlugin> loadAllUdfPlugins() {
        log.info("批量加载所有 UDF 插件");

        ServiceLoader<UdfPlugin> serviceLoader = serviceLoader(UdfPlugin.class);
        List<UdfPlugin> plugins = new ArrayList<>();

        for (UdfPlugin plugin : serviceLoader) {
            String functionName = plugin.identifier();

            // 校验函数名非空
            if (functionName == null || functionName.trim().isEmpty()) {
                log.warn("UDF 插件 {} 的 identifier() 返回空值，跳过加载",
                        plugin.getClass().getName());
                continue;
            }

            log.info("UDF 插件加载成功：{} -> {}",
                    functionName, plugin.getClass().getName());
            plugins.add(plugin);
        }

        log.info("共加载 {} 个 UDF 插件", plugins.size());
        return plugins;
    }
}
