package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

/**
 * JDBC Sink 插件
 * 支持所有 JDBC 数据库，提供 table 和 sql 两种配置模式
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class JdbcSinkPlugin implements SinkPlugin {

    @Override
    public String getType() {
        return "jdbc";
    }

    @Override
    public SinkFunction<Row> createSink(SinkConfig config) {
        String url = config.getString("url");
        // 必要参数校验
        if (url == null) {
            throw new IllegalArgumentException("JDBC Sink 缺少必要配置: url");
        }

        // MySQL 需要添加 useCursorFetch 参数，使 batchSize 生效
        if (url.contains(":mysql:") && !url.contains("rewriteBatchedStatements=true")) {
            url = url.contains("?") ? url + "&rewriteBatchedStatements=true" : url + "?rewriteBatchedStatements=true";
            log.info("MySQL URL 添加 rewriteBatchedStatements 参数");
        }

        String username = config.getString("username");
        if (username == null) {
            throw new IllegalArgumentException("JDBC Sink 缺少必要配置: username");
        }

        String password = config.getString("password");
        if (password == null) {
            throw new IllegalArgumentException("JDBC Sink 缺少必要配置: password");
        }

        String table = config.getString("table");
        String sql = config.getString("sql");
        if (table == null && sql == null) {
            throw new IllegalArgumentException("JDBC Sink 需要配置 table 或 sql");
        }

        int batchSize = config.getInteger("batchSize", getDefaultBatchSize());

        // table 优先
        JdbcSinkConfig jdbcConfig = JdbcSinkConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .batchSize(batchSize)
                .build();

        String mode = table != null ? "table" : "sql";
        log.info("创建 JDBC Sink, mode={}, batchSize={}", mode, batchSize);

        return new JdbcSinkFunction(jdbcConfig);
    }
}