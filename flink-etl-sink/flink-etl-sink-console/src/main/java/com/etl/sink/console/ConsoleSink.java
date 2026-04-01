package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.sink.AbstractSink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Console Sink 实现
 * 将数据输出到控制台
 */
public class ConsoleSink extends AbstractSink {

    private final boolean showSubtask;

    public ConsoleSink(SinkConfig config, boolean showSubtask) {
        super(config);
        this.showSubtask = showSubtask;
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) throws IOException {
        return new ConsoleSinkWriter(context, showSubtask);
    }
}