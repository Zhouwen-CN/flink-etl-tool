package com.etl.core.spi;

import com.etl.core.config.SinkConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Sink 插件接口
 * 所有数据写入插件必须实现此接口
 */
public interface SinkPlugin extends Serializable {

    /**
     * 获取插件类型标识
     *
     * @return 插件类型标识
     */
    String getType();

    /**
     * 创建 Sink 函数
     *
     * @param config Sink 配置
     * @return Flink SinkFunction，强制消费 Row 类型
     */
    SinkFunction<Row> createSink(SinkConfig config);
}