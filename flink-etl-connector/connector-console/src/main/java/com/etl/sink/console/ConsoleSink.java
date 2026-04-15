package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.sink.AbstractSink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Console Sink 实现
 * 将数据输出到控制台，默认显示 subtask 信息
 */
public class ConsoleSink extends AbstractSink {

    public ConsoleSink(SinkConfig config) {
        super(config);
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) throws IOException {
        return new ConsoleSinkWriter(context);
    }
}