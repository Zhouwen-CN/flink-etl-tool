package com.etl.core.spi;

import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

/**
 * SPI 插件加载器
 * 通过 Java SPI 机制动态加载插件
 */
@Slf4j
public class PluginLoader {

    private PluginLoader() {
        // 私有构造函数，防止实例化
    }

    private static <T extends Plugin> T loadPlugin(Class<T> clazz, String type) {
        String simpleName = clazz.getSimpleName();
        log.info("加载 {}：{}", simpleName, type);

        // 使用当前线程的 ClassLoader 加载插件
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PluginLoader.class.getClassLoader();
        }

        ServiceLoader<T> loader = ServiceLoader.load(clazz, classLoader);
        for (T plugin : loader) {
            if (plugin.getType().equals(type)) {
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
        return loadPlugin(SourcePlugin.class, type);
    }

    /**
     * 加载 Transform 插件
     *
     * @param type 插件类型
     * @return Transform 插件实例
     */
    public static TransformPlugin loadTransformPlugin(String type) {
        return loadPlugin(TransformPlugin.class, type);
    }

    /**
     * 加载 Sink 插件
     *
     * @param type 插件类型
     * @return Sink 插件实例
     */
    public static SinkPlugin loadSinkPlugin(String type) {
        return loadPlugin(SinkPlugin.class, type);
    }
}
