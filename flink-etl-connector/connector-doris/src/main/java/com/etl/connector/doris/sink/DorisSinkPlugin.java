package com.etl.connector.doris.sink;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.etl.connector.doris.sink.format.DorisFormatLoader;
import com.etl.connector.doris.sink.format.DorisFormatPlugin;
import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

/**
 * Doris Sink 插件
 * 封装官方 flink-doris-connector，通过 Stream Load 写入，at-least-once（batch 模式）
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class DorisSinkPlugin implements SinkPlugin {

    @Override
    public String identifier() {
        return "doris";
    }

    @Override
    public Sink<Row> createSink(SinkConfig config) {
        DorisSinkConfig cfg = DorisSinkConfig.fromSinkConfig(config);

        // SPI 加载序列化格式
        DorisFormatPlugin fmt = DorisFormatLoader.getFormatPlugin(cfg.getFormat());
        if (fmt == null) {
            throw new IllegalArgumentException(
                    "不支持的 format: " + cfg.getFormat()
                            + "，支持: " + DorisFormatLoader.supportedFormats());
        }

        // Doris 连接配置
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(cfg.getFenodes())
                .setTableIdentifier(cfg.getTableIdentifier())
                .setUsername(cfg.getUsername())
                .setPassword(cfg.getPassword())
                .build();

        // 执行配置：batch 模式（at-least-once，非 2PC）+ format 的 Stream Load 属性
        DorisExecutionOptions.Builder execBuilder = DorisExecutionOptions.builder()
                .setBatchMode(true)
                .setStreamLoadProp(fmt.streamLoadProperties());
        if (cfg.getLabelPrefix() != null) {
            execBuilder.setLabelPrefix(cfg.getLabelPrefix());
        }
        if (cfg.getBatchSize() != null) {
            execBuilder.setBufferFlushMaxRows(cfg.getBatchSize());
        }
        DorisExecutionOptions execOptions = execBuilder.build();

        log.info("创建 Doris Sink: fenodes={}, table={}, format={}, labelPrefix={}, batchSize={}",
                cfg.getFenodes(), cfg.getTableIdentifier(), cfg.getFormat(),
                cfg.getLabelPrefix(), cfg.getBatchSize());

        return DorisSink.<Row>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(execOptions)
                .setSerializer(fmt.createSerializer(cfg))
                .build();
    }
}
