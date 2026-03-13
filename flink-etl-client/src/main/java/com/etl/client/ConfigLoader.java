package com.etl.client;

import com.etl.core.config.ConfigParser;
import com.etl.core.config.JobConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 配置加载器
 * 支持从文件或 JSON 字符串加载配置
 */
@Slf4j
public class ConfigLoader {

    /**
     * 从文件加载配置
     *
     * @param filePath 配置文件路径
     * @return Job 配置对象
     * @throws IllegalArgumentException 文件不存在或解析失败
     */
    public static JobConfig loadFromFile(String filePath) {
        log.info("从文件加载配置: {}", filePath);

        if (!Files.exists(Paths.get(filePath))) {
            String errorMsg = String.format("配置文件不存在: %s", filePath);
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        File file = new File(filePath);
        if (!file.isFile()) {
            String errorMsg = String.format("路径不是文件: %s", filePath);
            log.error(errorMsg);
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
        log.info("从 JSON 字符串加载配置");
        return ConfigParser.parseFromString(json);
    }
}