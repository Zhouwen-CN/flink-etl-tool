package com.etl.connector.doris.sink.v1;

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
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Doris Sink 插件
 * 封装官方 flink-doris-connector，通过 Stream Load 写入，at-least-once（batch 模式）
 * 适用于 flink-doris-connector-1.15:1.4.0
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
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(cfg.getFenodes())
                .setTableIdentifier(cfg.getTable())
                .setUsername(cfg.getUsername())
                .setPassword(cfg.getPassword())
                .build();

        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        // 执行配置：batch 模式 + 禁用 2PC（at-least-once）
        DorisExecutionOptions.Builder execBuilder = DorisExecutionOptions.builder()
                .setStreamLoadProp(properties)
                .setLabelPrefix(cfg.getLabelPrefix())
                .setDeletable(true)
                .disable2PC();

        log.info("创建 Doris Sink: fenodes={}, table={}, labelPrefix={}, batchSize={}, batchIntervalMs={}",
                cfg.getFenodes(), cfg.getTable(), cfg.getLabelPrefix(), cfg.getBatchSize(), cfg.getBatchIntervalMs());

        DorisSink<Row> dorisSink = DorisSink.<Row>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(execBuilder.build())
                .setSerializer(new RowToJsonSerializer())
                .build();

        return SinkV1Adapter.wrap(dorisSink);
    }
}
