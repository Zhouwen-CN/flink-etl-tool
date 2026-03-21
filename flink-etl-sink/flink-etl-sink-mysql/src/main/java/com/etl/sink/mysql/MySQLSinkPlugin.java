package com.etl.sink.mysql;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.etl.sink.jdbc.JdbcSinkFunction;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

/**
 * MySQL Sink 插件
 * 将数据写入 MySQL 数据库，支持 table 模式和 sql 模式
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class MySQLSinkPlugin implements SinkPlugin {

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public SinkFunction<Row> createSink(SinkConfig config) {
        String url = config.getString("url");
        String username = config.getString("username");
        String password = config.getString("password");
        String table = config.getString("table");
        String sql = config.getString("sql");
        int batchSize = config.getInteger("batchSize", 100);

        if (url == null || username == null || password == null) {
            throw new IllegalArgumentException("MySQL Sink 缺少必要配置: url, username, password");
        }
        if (table == null && sql == null) {
            throw new IllegalArgumentException("MySQL Sink 需要配置 table 或 sql");
        }

        JdbcSinkConfig jdbcConfig = JdbcSinkConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .batchSize(batchSize)
                .build();

        log.info("创建 MySQL Sink, table={}, sql={}, batchSize={}", table, sql != null ? "[自定义SQL]" : null, batchSize);
        return new JdbcSinkFunction(jdbcConfig);
    }
}