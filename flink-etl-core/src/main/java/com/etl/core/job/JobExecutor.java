package com.etl.core.job;

import com.etl.core.config.ConfigParser;
import com.etl.core.config.JobConfig;
import com.etl.core.spi.PluginLoader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
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
     * @deprecated 请使用 {@link #execute(JobConfig)} 方法
     */
    @Deprecated
    public void execute(String configPath) {
        logger.info("开始执行 Job（从文件: {}）", configPath);

        try {
            JobConfig config = ConfigParser.parse(configPath);
            execute(config);
        } catch (Exception e) {
            String errorMsg = String.format("Job 执行失败: %s", e.getMessage());
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    /**
     * 执行 Job（使用 JobConfig 对象）
     *
     * @param config Job 配置对象
     */
    public void execute(JobConfig config) {
        logger.info("开始执行 Job: {}", config.getJob().getName());

        try {
            StreamExecutionEnvironment env = createExecutionEnvironment(config);

            JobBuilder jobBuilder = new JobBuilder(pluginLoader);
            jobBuilder.build(env, config);

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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();

        if ("batch".equalsIgnoreCase(mode)) {
            configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        } else if ("stream".equalsIgnoreCase(mode)) {
            configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        }else{
            throw new IllegalArgumentException("job.mode 仅支持 batch | stream");
        }

        if (parallelism != null) {
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
            logger.info("设置 Job 并行度: {}", parallelism);
        }

        env.configure(configuration);
        return env;
    }
}