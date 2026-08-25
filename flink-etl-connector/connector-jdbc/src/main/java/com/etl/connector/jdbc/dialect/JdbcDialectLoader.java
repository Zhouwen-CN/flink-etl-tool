package com.etl.connector.jdbc.dialect;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

/**
 * JDBC Dialect 简单工厂
 * 使用 SPI 加载所有 Dialect 实现，根据 URL 自动识别数据库类型
 */
@Slf4j
public final class JdbcDialectLoader {

    private static final List<JdbcDialect> DIALECTS = loadDialects();

    private JdbcDialectLoader() {
    }

    private static List<JdbcDialect> loadDialects() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = JdbcDialectLoader.class.getClassLoader();
        }

        List<JdbcDialect> list = new ArrayList<>();
        for (JdbcDialect dialect : ServiceLoader.load(JdbcDialect.class, classLoader)) {
            list.add(dialect);
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * 根据 JDBC URL 自动识别对应的 Dialect
     *
     * @param url JDBC 连接 URL
     * @return 对应的 Dialect
     * @throws IllegalArgumentException 如果不支持的数据库类型
     */
    public static JdbcDialect get(String url) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("JDBC URL 不能为空");
        }

        for (JdbcDialect dialect : DIALECTS) {
            if (dialect.acceptsUrl(url)) {
                log.debug("URL {} 匹配 Dialect: {}", url, dialect.getName());
                try {
                    Class.forName(dialect.driverClassName());
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("未找到对应的驱动: " + url);
                }
                return dialect;
            }
        }

        throw new IllegalArgumentException("不支持的数据库类型，URL: " + url);
    }
}
