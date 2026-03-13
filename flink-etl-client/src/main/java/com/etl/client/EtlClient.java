package com.etl.client;

import com.etl.core.config.JobConfig;
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
        logger.info("ETL 工具启动");

        CliArgumentParser argParser = new CliArgumentParser();

        try {
            JobConfig config = argParser.parse(args);
            if (config == null) {
                argParser.printUsage();
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
            argParser.printUsage();
            System.exit(1);
        } catch (Exception e) {
            logger.error("Job 执行失败", e);
            System.err.println("Job 执行失败: " + e.getMessage());
            argParser.printUsage();
            System.exit(1);
        }
    }
}