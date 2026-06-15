package com.etl.connector.doris.sink.format.cdc;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.etl.connector.doris.sink.format.DorisFormatPlugin;
import com.google.auto.service.AutoService;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.types.Row;

/**
 * JSON 格式插件
 * Row 序列化为每行一个 JSON 对象，配合 Stream Load read_json_by_line=true
 */
@AutoService(DorisFormatPlugin.class)
public class DebeziumJsonFormatPlugin implements DorisFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "debezium-json";
    }

    @Override
    public DorisRecordSerializer<Row> createSerializer(DorisSinkConfig config) {
        return new DebeziumJsonSerializerImpl(config);
    }

}
