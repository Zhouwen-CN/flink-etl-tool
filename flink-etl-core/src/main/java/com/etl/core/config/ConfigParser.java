package com.etl.core.config;


import com.etl.core.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

/**
 * 配置文件解析器
 */
@Slf4j
public class ConfigParser {

    /**
     * 从 JSON 字符串解析 Job 配置
     *
     * @param json JSON 字符串
     * @return Job 配置对象
     */
    public static JobConfig parse(String json) {
        log.info("从字符串解析配置");

        try {
            JobConfig config = JsonUtils.fromJson(json, JobConfig.class);
            validate(config);
            log.info("配置解析成功");
            return config;
        } catch (Exception e) {
            String errorMsg = String.format("配置解析失败: %s", e.getMessage());
            log.error(errorMsg, e);
            throw new IllegalArgumentException(errorMsg, e);
        }
    }

    /**
     * 校验配置完整性
     *
     * @param config Job 配置
     */
    private static void validate(JobConfig config) {
        // 校验 job 配置
        if (config.getJob() == null) {
            throw new IllegalArgumentException("缺少 job 配置");
        }
        if (config.getJob().getName() == null || config.getJob().getName().isEmpty()) {
            throw new IllegalArgumentException("缺少 job.name 配置");
        }
        if (config.getJob().getMode() == null) {
            throw new IllegalArgumentException("缺少 job.mode 配置");
        }

        // 校验 sources 数组
        if (config.getSources() == null || config.getSources().isEmpty()) {
            throw new IllegalArgumentException("缺少 sources 配置");
        }

        Set<String> outputTables = new HashSet<>();
        for (int i = 0; i < config.getSources().size(); i++) {
            SourceConfig source = config.getSources().get(i);
            if (source.getType() == null || source.getType().isEmpty()) {
                throw new IllegalArgumentException("缺少 sources[" + i + "].type 配置");
            }
            if (source.getOutputTable() == null || source.getOutputTable().isEmpty()) {
                throw new IllegalArgumentException("缺少 sources[" + i + "].outputTable 配置");
            }
            if (!outputTables.add(source.getOutputTable())) {
                throw new IllegalArgumentException("sources 中 outputTable 重复: " + source.getOutputTable());
            }
        }

        // 校验 transforms
        if (config.getTransforms() != null) {
            for (int i = 0; i < config.getTransforms().size(); i++) {
                TransformConfig transform = config.getTransforms().get(i);
                if (transform.getType() == null || transform.getType().isEmpty()) {
                    throw new IllegalArgumentException("缺少 transforms[" + i + "].type 配置");
                }
                if (transform.getOutputTable() == null || transform.getOutputTable().isEmpty()) {
                    throw new IllegalArgumentException("缺少 transforms[" + i + "].outputTable 配置");
                }
                if (!outputTables.add(transform.getOutputTable())) {
                    throw new IllegalArgumentException("transforms 中 outputTable 重复或与 sources 冲突: " + transform.getOutputTable());
                }
            }
        }

        // 校验 sinks 数组
        if (config.getSinks() == null || config.getSinks().isEmpty()) {
            throw new IllegalArgumentException("缺少 sinks 配置");
        }
        for (int i = 0; i < config.getSinks().size(); i++) {
            SinkConfig sink = config.getSinks().get(i);
            if (sink.getType() == null || sink.getType().isEmpty()) {
                throw new IllegalArgumentException("缺少 sinks[" + i + "].type 配置");
            }
            if (sink.getInputTable() == null || sink.getInputTable().isEmpty()) {
                throw new IllegalArgumentException("缺少 sinks[" + i + "].inputTable 配置");
            }
            // 验证 inputTable 是否在上游定义
            if (!outputTables.contains(sink.getInputTable())) {
                throw new IllegalArgumentException("sinks[" + i + "].inputTable '" + sink.getInputTable()
                        + "' 未在上游 source.outputTable 或 transform.outputTable 中定义");
            }
        }
    }
}