package com.etl.core.dialect;

import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

/**
 * JDBC Dialect 简单工厂
 * 使用 SPI 加载所有 Dialect 实现，根据 URL 自动识别数据库类型
 */
@Slf4j
public final class JdbcDialects {

    private JdbcDialects() {}

    /**
     * 根据 JDBC URL 获取对应的 Dialect
     * @param url JDBC 连接 URL
     * @return 对应的 Dialect
     * @throws IllegalArgumentException 如果不支持的数据库类型
     */
    public static JdbcDialect get(String url) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("JDBC URL 不能为空");
        }

        // 使用 SPI 加载所有 Dialect 实现
        ServiceLoader<JdbcDialect> loader = ServiceLoader.load(JdbcDialect.class);
        for (JdbcDialect dialect : loader) {
            if (dialect.acceptsUrl(url)) {
                log.debug("URL {} 匹配 Dialect: {}", url, dialect.getName());
                return dialect;
            }
        }

        throw new IllegalArgumentException("不支持的数据库类型，URL: " + url);
    }

    /**
     * 检查 URL 是否被支持
     * @param url JDBC 连接 URL
     * @return 是否支持
     */
    public static boolean isSupported(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }
        ServiceLoader<JdbcDialect> loader = ServiceLoader.load(JdbcDialect.class);
        for (JdbcDialect dialect : loader) {
            if (dialect.acceptsUrl(url)) {
                return true;
            }
        }
        return false;
    }
}