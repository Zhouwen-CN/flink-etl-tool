package com.etl.connector.http.source.format;

import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

/**
 * HTTP Format SPI 加载器
 * 通过 Java SPI 机制动态加载 Format 实现
 */
@Slf4j
public final class HttpFormatLoader {

    private static final ClassLoader CLASS_LOADER;

    static {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = HttpFormatLoader.class.getClassLoader();
        }
        CLASS_LOADER = cl;
    }

    private HttpFormatLoader() {
        // 私有构造函数，防止实例化
    }

    /**
     * 按格式标识加载 HttpFormat 实现
     *
     * @param identifier 格式标识（json / xml / raw）
     * @return HttpFormat 实例
     */
    public static HttpFormat load(String identifier) {
        log.info("加载 HttpFormat：{}", identifier);

        ServiceLoader<HttpFormat> serviceLoader = ServiceLoader.load(HttpFormat.class, CLASS_LOADER);
        for (HttpFormat format : serviceLoader) {
            if (format.identifier().equalsIgnoreCase(identifier)) {
                log.info("HttpFormat 加载成功：{}", format.getClass().getName());
                return format;
            }
        }

        String errorMsg = String.format("未找到 HttpFormat：%s", identifier);
        log.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
    }
}