package com.etl.client;

import com.etl.core.config.ConfigLoader;
import com.etl.core.config.JobConfig;
import com.etl.core.job.JobExecutor;
import com.etl.core.spi.PluginLoader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL 客户端启动器
 */
public class EtlClient {
    private static final Logger logger = LoggerFactory.getLogger(EtlClient.class);

    public static void main(String[] args) {
        logger.info("ETL 工具启动");

        try {
            ParameterTool params = ParameterTool.fromArgs(args);

            JobConfig config = null;

            if (params.has("file")) {
                String filePath = params.get("file");
                logger.info("从文件加载配置: {}", filePath);
                config = ConfigLoader.loadFromFile(filePath);
            } else if (params.has("config")) {
                String jsonString = params.get("config");
                logger.info("从命令行 JSON 字符串加载配置");
                config = ConfigLoader.loadFromJsonString(jsonString);
            } else if (args.length == 1 && !args[0].startsWith("--")) {
                logger.warn("使用已弃用的参数格式，建议使用 --file 或 --config 参数");
                String configPath = args[0];
                logger.info("从文件加载配置: {}", configPath);
                config = ConfigLoader.loadFromFile(configPath);
            } else {
                printUsage();
                System.exit(1);
            }

            PluginLoader pluginLoader = new PluginLoader();

            JobExecutor executor = new JobExecutor(pluginLoader);
            executor.execute(config);

            logger.info("Job 执行成功");
            System.exit(0);
        } catch (IllegalArgumentException e) {
            logger.error("配置错误: {}", e.getMessage());
            System.err.println("配置错误: " + e.getMessage());
            printUsage();
            System.exit(1);
        } catch (Exception e) {
            logger.error("Job 执行失败", e);
            System.err.println("Job 执行失败: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
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
        System.err.println("注意:");
        System.err.println("  旧格式（直接传文件路径）已弃用，建议使用 --file 参数");
    }
}