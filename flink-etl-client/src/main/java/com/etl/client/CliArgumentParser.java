package com.etl.client;

import com.etl.core.config.ConfigLoader;
import com.etl.core.config.JobConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 命令行参数解析器
 * <p>
 * 负责解析命令行参数并加载对应的 Job 配置
 */
public class CliArgumentParser {
    private static final Logger logger = LoggerFactory.getLogger(CliArgumentParser.class);

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

    private JobConfig loadFromFile(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            logger.error("--file 参数值不能为空");
            System.err.println("错误: --file 参数值不能为空");
            return null;
        }

        logger.info("从文件加载配置: {}", filePath);
        return ConfigLoader.loadFromFile(filePath);
    }

    private JobConfig loadFromJsonString(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            logger.error("--config 参数值不能为空");
            System.err.println("错误: --config 参数值不能为空");
            return null;
        }

        logger.info("从命令行 JSON 字符串加载配置");
        return ConfigLoader.loadFromJsonString(jsonString);
    }
}