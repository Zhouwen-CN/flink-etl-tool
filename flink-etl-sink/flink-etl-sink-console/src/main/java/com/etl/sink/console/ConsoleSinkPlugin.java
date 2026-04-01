package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

/**
 * Console Sink 插件
 * 将数据输出到控制台
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class ConsoleSinkPlugin implements SinkPlugin {

    @Override
    public String getType() {
        return "console";
    }

    @Override
    public Sink<Row> createSink(SinkConfig config) {
        boolean showSubtask = config.getBoolean("showSubtask", true);
        log.info("创建 Console Sink, showSubtask={}", showSubtask);

        return new ConsoleSink(config, showSubtask);
    }
}