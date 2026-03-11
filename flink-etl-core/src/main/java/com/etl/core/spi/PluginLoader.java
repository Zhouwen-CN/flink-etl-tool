package com.etl.core.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

/**
 * SPI 插件加载器
 * 通过 Java SPI 机制动态加载插件
 */
public class PluginLoader {
    private static final Logger logger = LoggerFactory.getLogger(PluginLoader.class);

    /**
     * 加载 Source 插件
     *
     * @param type 插件类型
     * @return Source 插件实例
     */
    public SourcePlugin loadSourcePlugin(String type) {
        logger.info("加载 Source 插件: {}", type);

        // 使用当前线程的 ClassLoader 加载插件
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PluginLoader.class.getClassLoader();
        }

        ServiceLoader<SourcePlugin> loader = ServiceLoader.load(SourcePlugin.class, classLoader);
        for (SourcePlugin plugin : loader) {
            if (plugin.getType().equals(type)) {
                logger.info("Source 插件加载成功: {}", plugin.getClass().getName());
                return plugin;
            }
        }

        String errorMsg = String.format("未找到 Source 插件: %s", type);
        logger.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
    }

    /**
     * 加载 Transform 插件
     *
     * @param type 插件类型
     * @return Transform 插件实例
     */
    public TransformPlugin loadTransformPlugin(String type) {
        logger.info("加载 Transform 插件: {}", type);

        // 使用当前线程的 ClassLoader 加载插件
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PluginLoader.class.getClassLoader();
        }

        ServiceLoader<TransformPlugin> loader = ServiceLoader.load(TransformPlugin.class, classLoader);
        for (TransformPlugin plugin : loader) {
            if (plugin.getType().equals(type)) {
                logger.info("Transform 插件加载成功: {}", plugin.getClass().getName());
                return plugin;
            }
        }

        String errorMsg = String.format("未找到 Transform 插件: %s", type);
        logger.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
    }

    /**
     * 加载 Sink 插件
     *
     * @param type 插件类型
     * @return Sink 插件实例
     */
    public SinkPlugin loadSinkPlugin(String type) {
        logger.info("加载 Sink 插件: {}", type);

        // 使用当前线程的 ClassLoader 加载插件
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PluginLoader.class.getClassLoader();
        }

        ServiceLoader<SinkPlugin> loader = ServiceLoader.load(SinkPlugin.class, classLoader);
        for (SinkPlugin plugin : loader) {
            if (plugin.getType().equals(type)) {
                logger.info("Sink 插件加载成功: {}", plugin.getClass().getName());
                return plugin;
            }
        }

        String errorMsg = String.format("未找到 Sink 插件: %s", type);
        logger.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
    }
}