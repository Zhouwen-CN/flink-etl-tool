package com.etl.core.job;

import com.etl.core.config.JobConfig;
import com.etl.core.config.JobMeta;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
        JobMeta jobConfig = config.getJob();
        String jobName = jobConfig.getName();
        log.info("开始执行 Job: {}", jobName);

        try {
            StreamExecutionEnvironment env = createExecutionEnvironment(jobConfig);

            JobBuilder.build(env, config);

            log.info("提交 Job 到 Flink 执行引擎");
            env.execute(jobName);

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
     * @param jobConfig Job 配置
     * @return Flink 执行环境
     */
    StreamExecutionEnvironment createExecutionEnvironment(JobMeta jobConfig) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();

        RuntimeExecutionMode runtimeMode = jobConfig.getMode().getRuntimeMode();
        configuration.set(ExecutionOptions.RUNTIME_MODE, runtimeMode);

        Integer parallelism = jobConfig.getParallelism();
        if (parallelism != null) {
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
            // 本地执行环境需要配置足够的 slot 数量
            configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
            log.info("设置 Job 并行度：{}, TaskManager slots: {}", parallelism, parallelism);
        }
        log.info("创建 Flink 执行环境：mode={}, parallelism={}", runtimeMode, parallelism);

        env.configure(configuration);

        // steaming 模式下才开启检查点
        if (runtimeMode == RuntimeExecutionMode.STREAMING) {
            long checkpointInterval = jobConfig.getCheckpointInterval();
            long checkpointTimeout = jobConfig.getCheckpointTimeout();
            log.info("开启检查点：interval={}ms, timeout={}ms", checkpointInterval, checkpointTimeout);
            // 检查点间隔
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.AT_LEAST_ONCE);
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            // 检查点超时
            checkpointConfig.setCheckpointTimeout(checkpointTimeout);
            // 上一个checkpoint结束之后,多久才能发出另一个checkpoint
            checkpointConfig.setMinPauseBetweenCheckpoints(500L);
            // 检查点最大并发数量
            checkpointConfig.setMaxConcurrentCheckpoints(1);
            // 可容忍检查点失败次数
            checkpointConfig.setTolerableCheckpointFailureNumber(3);
            // 取消作业时,是否保留checkpoint数据
            checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }

        return env;
    }
}
