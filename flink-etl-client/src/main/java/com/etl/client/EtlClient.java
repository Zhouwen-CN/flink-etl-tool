package com.etl.client;

import com.etl.client.parser.CliArgumentParser;
import com.etl.core.config.JobConfig;
import com.etl.core.job.JobExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

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
        } catch (Exception e) {
            log.error("Job 执行失败", e);
            // 给 /jars/:jarid/plan 接口提供错误信息
            System.err.println(ExceptionUtils.getStackTrace(e));
        }
    }
}