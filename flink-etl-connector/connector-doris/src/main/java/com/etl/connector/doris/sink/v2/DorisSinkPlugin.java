package com.etl.connector.doris.sink.v2;

/**
 * Doris Sink 插件
 * 封装官方 flink-doris-connector，通过 Stream Load 写入，at-least-once（batch 模式）
 * 适用于 flink-doris-connector-1.15:1.5.2，建议使用这个
 */
/*@Slf4j
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

        // 执行配置：batch 模式 + 禁用 2PC（at-least-once）
        DorisExecutionOptions.Builder execBuilder = DorisExecutionOptions.builderDefaults()
                .setLabelPrefix(cfg.getLabelPrefix())
                .setBufferFlushMaxRows(cfg.getBatchSize())
                .setBufferFlushIntervalMs(cfg.getBatchIntervalMs())
                .setIgnoreUpdateBefore(true)
                .setBatchMode(true)
                .disable2PC();

        log.info("创建 Doris Sink: fenodes={}, table={}, labelPrefix={}, batchSize={}, batchIntervalMs={}",
                cfg.getFenodes(), cfg.getTable(), cfg.getLabelPrefix(), cfg.getBatchSize(), cfg.getBatchIntervalMs());

        return DorisSink.<Row>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(execBuilder.build())
                .setSerializer(new RowToJsonSerializer())
                .build();
    }
}*/
