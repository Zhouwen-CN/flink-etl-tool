package com.etl.core.job;

import com.etl.core.config.JobConfig;
import com.etl.core.config.SinkConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.config.TransformConfig;
import com.etl.core.spi.PluginLoader;
import com.etl.core.spi.SinkPlugin;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.TransformPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Job 构建器
 * 负责将配置转换为 Flink Job
 */
@Slf4j
public class JobBuilder {

    /**
     * 构建 Flink Job
     *
     * @param env    Flink 执行环境
     * @param config Job 配置
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void build(StreamExecutionEnvironment env, JobConfig config) {
        log.info("开始构建 Flink Job: {}", config.getJob().getName());
        // 创建 Table 环境
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

        // 1. 处理所有 Source -> DataStream
        for (SourceConfig sourceConfig : config.getSources()) {
            String sourceType = sourceConfig.getType();
            SourcePlugin sourcePlugin = PluginLoader.loadSourcePlugin(sourceType);
            Source source = sourcePlugin.createSource(sourceConfig);
            DataStream<Row> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceType + " source");

            // DataStream<Row> -> Table
            String sourceOutputTable = sourceConfig.getOutputTable();
            stEnv.createTemporaryView(sourceOutputTable, sourceStream);
            log.info("注册 Table: {}", sourceOutputTable);
        }


        // 2. Transform 链式处理
        List<TransformConfig> transforms = config.getTransforms();
        if (transforms != null && !transforms.isEmpty()) {
            for (TransformConfig transformConfig : transforms) {
                TransformPlugin transformPlugin = PluginLoader.loadTransformPlugin(transformConfig.getType());
                Table transformed = transformPlugin.transform(transformConfig, stEnv);

                // 将 Transform 结果注册为中间表，供后续 SQL 引用
                String transformOutputTable = transformConfig.getOutputTable();
                stEnv.createTemporaryView(transformOutputTable, transformed);
                log.info("注册中间表：{}", transformOutputTable);

                log.info("Transform 应用成功：{}", transformConfig.getType());
            }
        }

        // 3. 处理所有 Sink
        for (SinkConfig sinkConfig : config.getSinks()) {
            String sinkInputTable = sinkConfig.getInputTable();
            DataStream<Row> resultStream;
            try {
                Table sinkTable = stEnv.from(sinkInputTable);
                resultStream = stEnv.toDataStream(sinkTable);
                log.info("Table 转换为 DataStream");
            } catch (TableException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
                throw new IllegalArgumentException("无法从表 '" + sinkInputTable + "' 读取数据，请检查 inputTable 配置是否正确，或上游 source.outputTable / transform.outputTable 是否已正确配置", e);
            }

            // Sink 消费 DataStream
            String sinkType = sinkConfig.getType();
            SinkPlugin sinkPlugin = PluginLoader.loadSinkPlugin(sinkType);
            SinkFunction<Row> sink = sinkPlugin.createSink(sinkConfig);
            resultStream.addSink(sink).name(sinkType + " sink");
            log.info("Sink 创建成功: {}", sinkType);
        }

        log.info("Flink Job 构建完成");
    }
}
