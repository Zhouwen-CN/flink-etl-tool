package com.etl.core.dialect;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * JDBC Dialect 简单工厂
 * 使用 SPI 加载所有 Dialect 实现，根据 URL 自动识别数据库类型
 */
@Slf4j
public final class JdbcDialects {

    private static final List<JdbcDialect> DIALECTS = loadDialects();

    private JdbcDialects() {}

    private static List<JdbcDialect> loadDialects() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = JdbcDialects.class.getClassLoader();
        }

        List<JdbcDialect> list = new ArrayList<>();
        for (JdbcDialect dialect : ServiceLoader.load(JdbcDialect.class, classLoader)) {
            list.add(dialect);
        }
        return Collections.unmodifiableList(list);
    }

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

        for (JdbcDialect dialect : DIALECTS) {
            if (dialect.acceptsUrl(url)) {
                log.debug("URL {} 匹配 Dialect: {}", url, dialect.getName());
                return dialect;
            }
        }

        throw new IllegalArgumentException("不支持的数据库类型，URL: " + url);
    }

    /**
     * 根据 Dialect 名称获取对应的 Dialect
     * @param name Dialect 名称，如 "mysql", "postgresql", "oracle"
     * @return 对应的 Dialect
     * @throws IllegalArgumentException 如果不支持的 dialect 名称
     */
    public static JdbcDialect getByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Dialect 名称不能为空");
        }

        for (JdbcDialect dialect : DIALECTS) {
            if (dialect.getName().equalsIgnoreCase(name)) {
                log.debug("名称 {} 匹配 Dialect: {}", name, dialect.getName());
                return dialect;
            }
        }

        throw new IllegalArgumentException("不支持的 Dialect 类型，名称: " + name +
            "。支持的类型: " + DIALECTS.stream().map(JdbcDialect::getName).collect(Collectors.joining(", ")));
    }
}