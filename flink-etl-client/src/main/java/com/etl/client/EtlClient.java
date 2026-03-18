package com.etl.client;

import com.etl.core.config.CliArgumentParser;
import com.etl.core.config.JobConfig;
import com.etl.core.job.JobExecutor;
import com.etl.core.spi.PluginLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * ETL 客户端启动器
 */
@Slf4j
public class EtlClient {
    public static void main(String[] args) {
        log.info("ETL 工具启动");

        try {
            JobConfig config = CliArgumentParser.parse(args);
            if (config == null) {
                CliArgumentParser.printUsage();
                System.exit(1);
            }

            PluginLoader pluginLoader = new PluginLoader();
            JobExecutor executor = new JobExecutor(pluginLoader);
            executor.execute(config);

            System.exit(0);
        } catch (IllegalArgumentException e) {
            log.error("配置错误：{}", e.getMessage());
            System.err.println("配置错误：" + e.getMessage());
            CliArgumentParser.printUsage();
            System.exit(1);
        } catch (Exception e) {
            log.error("Job 执行失败", e);
            System.err.println("Job 执行失败：" + e.getMessage());
            CliArgumentParser.printUsage();
            System.exit(1);
        }
    }
}
