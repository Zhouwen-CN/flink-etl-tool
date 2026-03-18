package com.etl.core.job;

import com.etl.core.config.JobConfig;
import com.etl.core.spi.PluginLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Job 执行器
 * 负责执行完整的 ETL Job
 */
@Slf4j
public class JobExecutor {

    /**
     * 执行 Job（使用 JobConfig 对象）
     *
     * @param config Job 配置对象
     */
    public void execute(JobConfig config) {
        log.info("开始执行 Job: {}", config.getJob().getName());

        try {
            StreamExecutionEnvironment env = createExecutionEnvironment(config);

            JobBuilder.build(env, config);

            log.info("提交 Job 到 Flink 执行引擎");
            env.execute(config.getJob().getName());

            log.info("Job 执行成功");
        } catch (Exception e) {
            String errorMsg = String.format("Job 执行失败：%s", e.getMessage());
            log.error(errorMsg, e);
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
        log.info("创建 Flink 执行环境：mode={}, parallelism={}", mode, parallelism);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();

        if ("batch".equalsIgnoreCase(mode)) {
            configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        } else if ("stream".equalsIgnoreCase(mode)) {
            configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        } else {
            throw new IllegalArgumentException("job.mode 仅支持 batch | stream");
        }

        if (parallelism != null) {
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
            // 本地执行环境需要配置足够的 slot 数量
            configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
            log.info("设置 Job 并行度：{}, TaskManager slots: {}", parallelism, parallelism);
        }

        env.configure(configuration);
        return env;
    }
}
