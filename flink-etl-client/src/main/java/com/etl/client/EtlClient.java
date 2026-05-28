package com.etl.client;

import com.etl.core.config.CliArgumentParser;
import com.etl.core.config.JobConfig;
import com.etl.core.job.JobExecutor;
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
            JobExecutor executor = new JobExecutor();
            executor.execute(config);

        } catch (IllegalArgumentException e) {
            log.error("配置错误：{}", e.getMessage());
            CliArgumentParser.printUsage();
        } catch (Exception e) {
            log.error("Job 执行失败", e);
        }
    }
}