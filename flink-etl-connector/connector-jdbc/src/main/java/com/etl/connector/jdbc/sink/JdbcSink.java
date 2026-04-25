package com.etl.connector.jdbc.sink;

import com.etl.core.config.SinkConfig;
import com.etl.core.sink.AbstractSink;
import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * JDBC Sink 实现
 * 支持将数据写入关系型数据库
 */
@Slf4j
public class JdbcSink extends AbstractSink {

    private final JdbcSinkConfig jdbcSinkConfig;

    public JdbcSink(SinkConfig config) {
        super(config);
        this.jdbcSinkConfig = JdbcSinkConfig.fromSinkConfig(config, super.getDefaultBatchSize());
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) throws IOException {
        return new JdbcSinkWriter(context, jdbcSinkConfig);
    }
}