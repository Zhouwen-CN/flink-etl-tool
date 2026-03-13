package com.etl.client;

import com.etl.core.config.ConfigParser;
import com.etl.core.config.JobConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 命令行参数解析器
 * <p>
 * 负责解析命令行参数并加载对应的 Job 配置
 */
@Slf4j
public class CliArgumentParser {

    /**
     * 解析命令行参数并返回 Job 配置
     *
     * @param args 命令行参数
     * @return Job 配置，如果参数无效则返回 null
     */
    public JobConfig parse(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        if (params.has("file")) {
            return loadFromFile(params.get("file"));
        } else if (params.has("config")) {
            return loadFromJsonString(params.get("config"));
        }

        return null;
    }

    /**
     * 打印使用说明
     */
    public void printUsage() {
        System.err.println("用法:");
        System.err.println("  java -jar flink-etl-tool.jar --file <config.json>");
        System.err.println("  java -jar flink-etl-tool.jar --config '<json-string>'");
        System.err.println();
        System.err.println("参数:");
        System.err.println("  --file <path>      从文件加载配置");
        System.err.println("  --config <json>    从 JSON 字符串加载配置");
        System.err.println();
        System.err.println("示例:");
        System.err.println("  java -jar flink-etl-tool.jar --file config/mysql-to-console.json");
        System.err.println("  java -jar flink-etl-tool.jar --config '{\"job\":{\"name\":\"test\",\"mode\":\"batch\"},...}'");
        System.err.println();
    }

    /**
     * 从文件加载配置
     *
     * @param filePath 配置文件路径
     * @return Job 配置对象，参数无效时返回 null
     */
    private JobConfig loadFromFile(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            log.error("--file 参数值不能为空");
            System.err.println("错误: --file 参数值不能为空");
            return null;
        }

        log.info("从文件加载配置: {}", filePath);

        if (!Files.exists(Paths.get(filePath))) {
            String errorMsg = String.format("配置文件不存在: %s", filePath);
            log.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        if (!new File(filePath).isFile()) {
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
     * @return Job 配置对象，参数无效时返回 null
     */
    private JobConfig loadFromJsonString(String json) {
        if (json == null || json.trim().isEmpty()) {
            log.error("--config 参数值不能为空");
            System.err.println("错误: --config 参数值不能为空");
            return null;
        }

        log.info("从命令行 JSON 字符串加载配置");
        return ConfigParser.parseFromString(json);
    }
}