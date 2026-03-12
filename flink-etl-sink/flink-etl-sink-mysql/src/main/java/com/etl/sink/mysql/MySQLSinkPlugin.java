package com.etl.sink.mysql;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL Sink 插件
 * 将数据写入 MySQL 数据库，支持批量写入和 upsert 模式
 * 列名从运行时 Row 的字段名中动态获取，无需在配置中指定
 */
public class MySQLSinkPlugin implements SinkPlugin {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(MySQLSinkPlugin.class);

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public SinkFunction<?> createSink(SinkConfig config) {
        String url = config.getString("url");
        String username = config.getString("username");
        String password = config.getString("password");
        String table = config.getString("table");
        int batchSize = config.getInteger("batchSize") != null ? config.getInteger("batchSize") : 100;
        String writeMode = config.getString("writeMode") != null ? config.getString("writeMode") : "insert";

        if (url == null || username == null || password == null || table == null) {
            throw new IllegalArgumentException("MySQL Sink 缺少必要配置: url, username, password, table");
        }

        logger.info("创建 MySQL Sink, table={}, mode={}, batchSize={}", table, writeMode, batchSize);
        return new MySQLSinkFunction(url, username, password, table, batchSize, writeMode);
    }
}
