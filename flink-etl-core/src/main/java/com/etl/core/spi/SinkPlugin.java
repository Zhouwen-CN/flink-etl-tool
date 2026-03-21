package com.etl.core.spi;

import com.etl.core.config.SinkConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

/**
 * Sink 插件接口
 * 所有数据写入插件必须实现此接口
 */
public interface SinkPlugin extends Plugin {

    /**
     * 创建 Sink 函数
     *
     * @param config Sink 配置
     * @return Flink SinkFunction，强制消费 Row 类型
     */
    SinkFunction<Row> createSink(SinkConfig config);

    /**
     * 所有 sink 默认的 batchSize
     *
     * @return 批次大小
     */
    default int getDefaultBatchSize() {
        return 100;
    }
}