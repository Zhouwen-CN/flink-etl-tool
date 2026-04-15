package com.etl.connector.jdbc.sink;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.types.Row;

/**
 * JDBC Sink 插件
 * 支持所有 JDBC 数据库，提供 table 和 sql 两种配置模式
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class JdbcSinkPlugin implements SinkPlugin {

    @Override
    public String identifier() {
        return "jdbc";
    }

    @Override
    public Sink<Row> createSink(SinkConfig config) {
        return new JdbcSink(config);
    }
}