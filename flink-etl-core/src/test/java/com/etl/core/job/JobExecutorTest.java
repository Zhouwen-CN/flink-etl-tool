package com.etl.core.job;

import com.etl.core.config.JobConfig;
import com.etl.core.config.JobMeta;
import com.etl.core.config.SinkConfig;
import com.etl.core.config.SourceConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JobExecutor 集成测试
 */
class JobExecutorTest {

    @Test
    void testCreateExecutionEnvironmentWithParallelism() {
        // 准备配置
        JobConfig config = new JobConfig();
        JobMeta jobMeta = new JobMeta();
        jobMeta.setName("test-job");
        jobMeta.setMode("batch");
        jobMeta.setParallelism(4);
        config.setJob(jobMeta);
        config.setSource(new SourceConfig());
        config.setSink(new SinkConfig());

        // 创建执行环境
        JobExecutor executor = new JobExecutor(null);
        StreamExecutionEnvironment env = executor.createExecutionEnvironment(config);

        // 验证并行度
        assertEquals(4, env.getParallelism());
    }

    @Test
    void testCreateExecutionEnvironmentWithoutParallelism() {
        // 准备配置（不设置并行度）
        JobConfig config = new JobConfig();
        JobMeta jobMeta = new JobMeta();
        jobMeta.setName("test-job");
        jobMeta.setMode("batch");
        // 不设置 parallelism
        config.setJob(jobMeta);
        config.setSource(new SourceConfig());
        config.setSink(new SinkConfig());

        // 创建执行环境
        JobExecutor executor = new JobExecutor(null);
        StreamExecutionEnvironment env = executor.createExecutionEnvironment(config);

        // 验证使用默认并行度（Flink 默认值，通常是 CPU 核心数）
        assertTrue(env.getParallelism() > 0);
    }
}