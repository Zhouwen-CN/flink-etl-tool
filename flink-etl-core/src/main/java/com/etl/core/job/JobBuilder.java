package com.etl.core.job;

import com.etl.core.config.JobConfig;
import com.etl.core.config.JobMeta;
import com.etl.core.config.SinkConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.config.TransformConfig;
import com.etl.core.spi.PluginLoader;
import com.etl.core.spi.SinkPlugin;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.TransformPlugin;
import com.etl.core.spi.UdfPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        JobMeta job = config.getJob();
        RuntimeExecutionMode runtimeMode = job.getMode().getRuntimeMode();
        log.info("开始构建 Flink Job: {}", job.getName());
        // 创建 Table 环境
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

        // 批量注册所有 UDF
        registerAllUdf(stEnv);

        // 1. 处理所有 Source -> DataStream
        for (SourceConfig sourceConfig : config.getSources()) {
            String sourceType = sourceConfig.getType();
            SourcePlugin sourcePlugin = PluginLoader.loadSourcePlugin(sourceType);
            Source source = sourcePlugin.createSource(sourceConfig, config.getJob().getMode().getRuntimeMode());
            DataStream<Row> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceType + " source");

            // DataStream<Row> -> Table
            String sourceOutputTable = sourceConfig.getOutputTable();
            stEnv.createTemporaryView(sourceOutputTable, fromDataStream(stEnv, sourceStream, runtimeMode));
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
                resultStream = toDataStream(stEnv,sinkTable,runtimeMode);
                log.info("Table 转换为 DataStream");
            } catch (ValidationException | TableException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalArgumentException("无法从表 '" + sinkInputTable + "' 读取数据，请检查 inputTable 配置是否正确，或上游 source.outputTable / transform.outputTable 是否已正确配置", e);
            }

            // Sink 消费 DataStream
            String sinkType = sinkConfig.getType();
            SinkPlugin sinkPlugin = PluginLoader.loadSinkPlugin(sinkType);
            Sink<Row> sink = sinkPlugin.createSink(sinkConfig);

            if (sink == null) {
                throw new IllegalArgumentException(
                        String.format("Sink 插件 '%s' 未实现新 API，请检查插件是否已迁移", sinkType));
            }

            resultStream.sinkTo(sink).name(sinkType + " sink");
            log.info("Sink 创建成功: {}", sinkType);
        }

        log.info("Flink Job 构建完成");
    }

    /**
     * datastream 转 table
     */
    private static Table fromDataStream(StreamTableEnvironment stEnv, DataStream<Row> dataStream, RuntimeExecutionMode runtimeMode) {
        if (runtimeMode == RuntimeExecutionMode.BATCH) {
            return stEnv.fromDataStream(dataStream);
        }

        return stEnv.fromChangelogStream(dataStream);
    }

    /**
     * table 转 datastream
     */
    private static  DataStream<Row> toDataStream(StreamTableEnvironment stEnv, Table table,RuntimeExecutionMode runtimeMode){
        if (runtimeMode == RuntimeExecutionMode.BATCH) {
            return stEnv.toDataStream(table);
        }

        return stEnv.toChangelogStream(table);
    }

    /**
     * 批量注册所有 UDF 到 TableEnvironment
     *
     * @param stEnv Table 环境
     * @throws IllegalStateException 如果 UDF 注册失败或函数名冲突
     */
    private static void registerAllUdf(StreamTableEnvironment stEnv) {
        List<UdfPlugin> udfPlugins = PluginLoader.loadAllUdfPlugins();

        Set<String> registeredFunctions = new HashSet<>();

        for (UdfPlugin udf : udfPlugins) {
            String functionName = udf.identifier();

            // 校验函数名唯一性
            if (registeredFunctions.contains(functionName)) {
                throw new IllegalStateException(
                        String.format("函数名冲突：'%s' 已被注册，请检查 UDF 插件的 identifier() 方法",
                                functionName)
                );
            }

            // 创建 UDF 实例
            UserDefinedFunction functionInstance = udf.createFunction();
            if (functionInstance == null) {
                throw new IllegalStateException(
                        String.format("UDF 插件 '%s' 的 createFunction() 返回 null",
                                udf.getClass().getName())
                );
            }

            // 注册函数
            try {
                stEnv.createTemporaryFunction(functionName, functionInstance);
                registeredFunctions.add(functionName);
                log.info("UDF 注册成功：{} -> {}",
                        functionName, functionInstance.getClass().getSimpleName());
            } catch (TableException e) {
                throw new IllegalStateException(
                        String.format("UDF 注册失败：%s", functionName), e
                );
            }
        }

        if (udfPlugins.isEmpty()) {
            log.info("未发现任何 UDF 插件");
        } else {
            log.info("成功注册 {} 个 UDF 函数", udfPlugins.size());
        }
    }
}
