package com.etl.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 配置加载器
 * 支持从文件或 JSON 字符串加载配置
 */
public class ConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    /**
     * 从文件加载配置
     *
     * @param filePath 配置文件路径
     * @return Job 配置对象
     * @throws IllegalArgumentException 文件不存在或解析失败
     */
    public static JobConfig loadFromFile(String filePath) {
        logger.info("从文件加载配置: {}", filePath);

        if (!Files.exists(Paths.get(filePath))) {
            String errorMsg = String.format("配置文件不存在: %s", filePath);
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        File file = new File(filePath);
        if (!file.isFile()) {
            String errorMsg = String.format("路径不是文件: %s", filePath);
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        return ConfigParser.parse(filePath);
    }

    /**
     * 从 JSON 字符串加载配置
     *
     * @param json JSON 字符串
     * @return Job 配置对象
     * @throws IllegalArgumentException 解析失败
     */
    public static JobConfig loadFromJsonString(String json) {
        logger.info("从 JSON 字符串加载配置");
        return ConfigParser.parseFromString(json);
    }
}