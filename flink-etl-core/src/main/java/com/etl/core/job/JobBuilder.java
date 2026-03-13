package com.etl.core.job;

import com.etl.core.config.JobConfig;
import com.etl.core.config.TransformConfig;
import com.etl.core.spi.PluginLoader;
import com.etl.core.spi.SinkPlugin;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.TransformPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Job 构建器
 * 负责将配置转换为 Flink Job
 */
@Slf4j
public class JobBuilder {

    private final PluginLoader pluginLoader;

    public JobBuilder(PluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    /**
     * 构建 Flink Job
     *
     * @param env    Flink 执行环境
     * @param config Job 配置
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void build(StreamExecutionEnvironment env, JobConfig config) {
        log.info("开始构建 Flink Job: {}", config.getJob().getName());

        // 1. 加载 Source 插件
        SourcePlugin sourcePlugin = pluginLoader.loadSourcePlugin(config.getSource().getType());
        Source source = sourcePlugin.createSource(config.getSource());

        // 2. 创建 DataStream
        DataStream stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "source-" + config.getSource().getType()
        );

        log.info("Source 创建成功");

        // 3. 依次应用 Transform 列表（如果配置）
        if (config.getTransforms() != null && !config.getTransforms().isEmpty()) {
            for (TransformConfig transformConfig : config.getTransforms()) {
                TransformPlugin transformPlugin = pluginLoader.loadTransformPlugin(transformConfig.getType());
                MapFunction transform = transformPlugin.createTransform(transformConfig);
                stream = stream.map(transform);
                log.info("Transform 应用成功: {}", transformConfig.getType());
            }
        }

        // 4. 加载 Sink 插件并写入
        SinkPlugin sinkPlugin = pluginLoader.loadSinkPlugin(config.getSink().getType());
        SinkFunction sink = sinkPlugin.createSink(config.getSink());
        stream.addSink(sink);

        log.info("Sink 创建成功");
        log.info("Flink Job 构建完成");
    }
}