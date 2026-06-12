package com.etl.connector.doris.sink.format;

import com.etl.connector.doris.sink.config.DorisSinkConfig;
import com.google.auto.service.AutoService;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * JSON 格式插件
 * Row 序列化为每行一个 JSON 对象，配合 Stream Load read_json_by_line=true
 */
@AutoService(DorisFormatPlugin.class)
public class JsonFormatPlugin implements DorisFormatPlugin {

    private static final long serialVersionUID = 1L;

    @Override
    public String identifier() {
        return "json";
    }

    @Override
    public DorisRecordSerializer<Row> createSerializer(DorisSinkConfig config) {
        return new RowToDorisJsonSerializer(config);
    }

    @Override
    public Properties streamLoadProperties() {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        return props;
    }
}
