package com.etl.core.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO 工具类，提供安静关闭资源等便捷方法
 */
public final class IOUtil {

    private static final Logger log = LoggerFactory.getLogger(IOUtil.class);

    private IOUtil() {
    }

    /**
     * 安静关闭资源，不记录日志
     *
     * @param resource 资源
     */
    public static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                // 忽略关闭异常
            }
        }
    }
}
