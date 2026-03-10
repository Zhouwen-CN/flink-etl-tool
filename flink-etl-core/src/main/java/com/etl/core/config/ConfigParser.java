package com.etl.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 配置文件解析器
 */
public class ConfigParser {
    private static final Logger logger = LoggerFactory.getLogger(ConfigParser.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 从文件解析 Job 配置
     *
     * @param configPath 配置文件路径
     * @return Job 配置对象
     */
    public static JobConfig parse(String configPath) {
        logger.info("解析配置文件: {}", configPath);

        try {
            JobConfig config = mapper.readValue(new File(configPath), JobConfig.class);
            validate(config);
            logger.info("配置文件解析成功");
            return config;
        } catch (Exception e) {
            String errorMsg = String.format("配置文件解析失败: %s", e.getMessage());
            logger.error(errorMsg, e);
            throw new IllegalArgumentException(errorMsg, e);
        }
    }

    /**
     * 校验配置完整性
     *
     * @param config Job 配置
     */
    private static void validate(JobConfig config) {
        if (config.getJob() == null) {
            throw new IllegalArgumentException("缺少 job 配置");
        }
        if (config.getJob().getName() == null || config.getJob().getName().isEmpty()) {
            throw new IllegalArgumentException("缺少 job.name 配置");
        }
        if (config.getJob().getMode() == null || config.getJob().getMode().isEmpty()) {
            throw new IllegalArgumentException("缺少 job.mode 配置");
        }
        if (config.getSource() == null) {
            throw new IllegalArgumentException("缺少 source 配置");
        }
        if (config.getSource().getType() == null || config.getSource().getType().isEmpty()) {
            throw new IllegalArgumentException("缺少 source.type 配置");
        }
        if (config.getSink() == null) {
            throw new IllegalArgumentException("缺少 sink 配置");
        }
        if (config.getSink().getType() == null || config.getSink().getType().isEmpty()) {
            throw new IllegalArgumentException("缺少 sink.type 配置");
        }
    }
}