package com.etl.core.job;

import com.etl.core.config.ExecutionMode;
import com.etl.core.config.JobConfig;
import com.etl.core.config.JobMeta;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

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
        log.info("开始执行 Job: {}", jobConfig.getName());

        try {
            StreamExecutionEnvironment env = createExecutionEnvironment(jobConfig);

            JobBuilder.build(env, config);

            log.info("提交 Job 到 Flink 执行引擎");
            env.execute(jobConfig.getName());

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
        ExecutionMode mode = jobConfig.getMode();
        Integer parallelism = jobConfig.getParallelism();
        log.info("创建 Flink 执行环境：mode={}, parallelism={}", mode, parallelism);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        mode.configure(configuration);

        if (parallelism != null) {
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
            // 本地执行环境需要配置足够的 slot 数量
            configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
            log.info("设置 Job 并行度：{}, TaskManager slots: {}", parallelism, parallelism);
        }

        env.configure(configuration);

        // 开启检查点
        env.enableCheckpointing(Duration.ofMinutes(3).toMillis(), CheckpointingMode.AT_LEAST_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 检查点超时
        checkpointConfig.setCheckpointTimeout(Duration.ofMinutes(3).toMillis());
        // 上一个checkpoint结束之后,多久才能发出另一个checkpoint
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
        // 检查点最大并发数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 可容忍检查点失败次数
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        // 取消作业时,是否保留checkpoint数据
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return env;
    }
}
