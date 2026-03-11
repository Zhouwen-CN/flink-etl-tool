package com.etl.client;

import com.etl.core.job.JobExecutor;
import com.etl.core.spi.PluginLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL 客户端启动器
 */
public class EtlClient {
    private static final Logger logger = LoggerFactory.getLogger(EtlClient.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("用法: java -jar flink-etl-tool.jar <config.json>");
            System.err.println("示例: java -jar flink-etl-tool.jar config/mysql-to-console.json");
            System.exit(1);
        }

        String configPath = args[0];
        logger.info("ETL 工具启动，配置文件: {}", configPath);

        try {
            // 初始化插件加载器
            PluginLoader pluginLoader = new PluginLoader();

            // 创建并执行 Job
            JobExecutor executor = new JobExecutor(pluginLoader);
            executor.execute(configPath);

            logger.info("Job 执行成功");
            System.exit(0);
        } catch (Exception e) {
            logger.error("Job 执行失败", e);
            System.err.println("Job 执行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}