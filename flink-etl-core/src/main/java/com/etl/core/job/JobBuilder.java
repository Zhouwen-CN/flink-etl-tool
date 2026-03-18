package com.etl.core.job;

import com.etl.core.config.JobConfig;
import com.etl.core.config.TransformConfig;
import com.etl.core.schema.EtlField;
import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.FlinkTypeConverter;
import com.etl.core.spi.PluginLoader;
import com.etl.core.spi.SinkPlugin;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.TransformPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

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

        // 创建 Table 环境
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);

        // 1. Source -> DataStream -> 注册 Table
        SourcePlugin sourcePlugin = pluginLoader.loadSourcePlugin(config.getSource().getType());
        Source source = sourcePlugin.createSource(config.getSource());
        DataStream<Row> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");

        // 强制校验 Schema
        EtlSchema schema = config.getSource().getSchema();
        if (schema == null) {
            throw new IllegalArgumentException("Source 必须配置 schema");
        }
        if (schema.getTableName() == null || schema.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Source 的 schema.tableName 不能为空");
        }

        // 使用 schema 字段名重建 Row，确保 Flink Table API 能识别列名
        final List<String> fieldNames = schema.getFieldNames();
        final List<EtlField> fields = schema.getFields();

        // 构建 RowTypeInfo 用于 Flink Table API
        TypeInformation<?>[] typeInfos = fields.stream()
                .map(f -> FlinkTypeConverter.toTypeInfo(f.getType()))
                .toArray(TypeInformation<?>[]::new);
        String[] names = fieldNames.toArray(new String[0]);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInfos, names);

        DataStream<Row> namedStream = sourceStream.map(row -> {
            // 创建位置模式的 Row，Flink 会根据 RowTypeInfo 自动映射字段名
            Row namedRow = new Row(fieldNames.size());
            for (int i = 0; i < fieldNames.size() && i < row.getArity(); i++) {
                namedRow.setField(i, row.getField(i));
            }
            return namedRow;
        }).returns(rowTypeInfo);

        // 注册为 Table
        stEnv.createTemporaryView(schema.getTableName(), namedStream);
        log.info("注册 Table: {}, 字段: {}", schema.getTableName(), fieldNames);

        // 2. Transform 链式处理
        Table resultTable = stEnv.from(schema.getTableName());

        List<TransformConfig> transforms = config.getTransforms();
        if (transforms != null && !transforms.isEmpty()) {
            for (int i = 0; i < transforms.size(); i++) {
                TransformConfig transformConfig = transforms.get(i);
                TransformPlugin transformPlugin = pluginLoader.loadTransformPlugin(transformConfig.getType());
                resultTable = transformPlugin.transform(resultTable, transformConfig, stEnv);

                // 将 Transform 结果注册为中间表，供后续 SQL 引用
                String intermediateTableName = "transform_result_" + i;
                stEnv.createTemporaryView(intermediateTableName, resultTable);
                log.info("注册中间表: {}", intermediateTableName);

                log.info("Transform 应用成功: {}", transformConfig.getType());
            }
        }

        // 3. Table -> DataStream<Row>
        DataStream<Row> resultStream = stEnv.toDataStream(resultTable);
        log.info("Table 转换为 DataStream");

        // 4. Sink 消费 DataStream
        SinkPlugin sinkPlugin = pluginLoader.loadSinkPlugin(config.getSink().getType());
        SinkFunction<Row> sink = sinkPlugin.createSink(config.getSink());
        resultStream.addSink(sink);
        log.info("Sink 创建成功");

        log.info("Flink Job 构建完成");
    }
}