package com.etl.sink.console;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Console Sink 插件
 * 将数据输出到控制台
 */
public class ConsoleSinkPlugin implements SinkPlugin {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ConsoleSinkPlugin.class);

    @Override
    public String getType() {
        return "console";
    }

    @Override
    public SinkFunction<?> createSink(SinkConfig config) {
        String format = config.getString("format");
        logger.info("创建 Console Sink, format={}", format);

        return new ConsoleSinkFunction(format);
    }

    /**
     * Console Sink Function
     */
    private static class ConsoleSinkFunction implements SinkFunction<Object> {
        private static final long serialVersionUID = 1L;
        private final String format;

        public ConsoleSinkFunction(String format) {
            this.format = format != null ? format : "json";
        }

        @Override
        public void invoke(Object value, Context context) {
            // 简单实现：直接打印对象
            System.out.println("console print: " + value.toString());
        }
    }
}
