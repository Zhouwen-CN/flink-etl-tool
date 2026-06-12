package com.etl.connector.doris.sink.format;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Properties;

/**
 * Doris Sink 序列化格式 SPI 接口
 * 定义在 Doris Sink 模块内部，不污染 core 模块
 */
public interface DorisFormatPlugin extends Serializable {

    /**
     * Format 标识符，如 "json"、"debezium-json"
     */
    String identifier();

    /**
     * 创建 Row 序列化器
     */
    DorisRecordSerializer<Row> createSerializer(DorisSinkConfig config);

    /**
     * 该 format 对应的 Stream Load 属性
     * 如 json -> format=json, read_json_by_line=true
     */
    Properties streamLoadProperties();
}
