package com.etl.core.job;

import com.etl.core.config.ConfigParser;
import com.etl.core.config.JobConfig;
import com.etl.core.spi.PluginLoader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job 执行器
 * 负责执行完整的 ETL Job
 */
public class JobExecutor {
    private static final Logger logger = LoggerFactory.getLogger(JobExecutor.class);

    private final PluginLoader pluginLoader;

    public JobExecutor(PluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    /**
     * 执行 Job
     *
     * @param configPath 配置文件路径
     */
    public void execute(String configPath) {
        logger.info("开始执行 Job");

        try {
            // 1. 解析配置
            JobConfig config = ConfigParser.parse(configPath);

            // 2. 创建 Flink 执行环境
            StreamExecutionEnvironment env = createExecutionEnvironment(config);

            // 3. 构建 Job
            JobBuilder jobBuilder = new JobBuilder(pluginLoader);
            jobBuilder.build(env, config);

            // 4. 执行 Job
            logger.info("提交 Job 到 Flink 执行引擎");
            env.execute(config.getJob().getName());

            logger.info("Job 执行成功");
        } catch (Exception e) {
            String errorMsg = String.format("Job 执行失败: %s", e.getMessage());
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    /**
     * 根据配置创建执行环境
     *
     * @param config Job 配置
     * @return Flink 执行环境
     */
    StreamExecutionEnvironment createExecutionEnvironment(JobConfig config) {
        String mode = config.getJob().getMode();
        Integer parallelism = config.getJob().getParallelism();
        logger.info("创建 Flink 执行环境: mode={}, parallelism={}", mode, parallelism);

        StreamExecutionEnvironment env;
        if ("batch".equals(mode)) {
            // 批处理模式
            Configuration configuration = new Configuration();
            configuration.setString("execution.runtime-mode", "BATCH");
            env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        } else {
            // 流处理模式
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 设置并行度（如果配置了）
        if (parallelism != null) {
            env.setParallelism(parallelism);
            logger.info("设置 Job 并行度: {}", parallelism);
        }

        return env;
    }
}