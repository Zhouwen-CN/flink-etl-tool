package com.etl.core.spi;

import com.etl.core.config.SinkConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

/**
 * Sink 插件接口
 * 所有数据写入插件必须实现此接口
 */
public interface SinkPlugin extends Plugin {

    /**
     * 创建 Sink 实例
     *
     * @param config Sink 配置
     * @return Flink Sink 接口，强制消费 Row 类型
     */
    Sink<Row> createSink(SinkConfig config);

    /**
     * 所有 sink 默认的 batchSize
     *
     * @return 批次大小
     */
    default int getDefaultBatchSize() {
        return 100;
    }
}