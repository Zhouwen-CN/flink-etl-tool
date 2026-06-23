package com.etl.connector.doris.sink;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
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

        // Doris 连接配置
        DorisOptions.Builder builder = DorisOptions.builder()
                .setFenodes(cfg.getFenodes())
                .setUsername(cfg.getUsername())
                .setPassword(cfg.getPassword());

        String table = cfg.getTable();
        if (table != null) {
            builder.setTableIdentifier(table);
        }

        DorisOptions dorisOptions = builder.build();

        // 执行配置：batch 模式 + 禁用 2PC（at-least-once）
        DorisExecutionOptions.Builder execBuilder = DorisExecutionOptions.builderDefaults()
                .setLabelPrefix(cfg.getLabelPrefix())
                .setBufferFlushMaxRows(cfg.getBatchSize())
                .setBufferFlushIntervalMs(cfg.getBatchIntervalMs())
                .setIgnoreUpdateBefore(true)
                .setBatchMode(true)
                .disable2PC();

        log.info("创建 Doris Sink: fenodes={}, tableMapping={}, labelPrefix={}, batchSize={}, batchIntervalMs={}",
                cfg.getFenodes(), cfg.getTableMapping(), cfg.getLabelPrefix(), cfg.getBatchSize(), cfg.getBatchIntervalMs());

        return DorisSink.<Row>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(execBuilder.build())
                .setSerializer(new RowToJsonSerializer(cfg.getTableMapping()))
                .build();
    }
}
