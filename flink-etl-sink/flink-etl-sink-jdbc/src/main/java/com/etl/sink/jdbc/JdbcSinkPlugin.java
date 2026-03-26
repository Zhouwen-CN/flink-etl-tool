package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialects;
import com.etl.core.dialect.WriteMode;
import com.etl.core.spi.SinkPlugin;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        // 获取 Dialect 并包装 URL
        JdbcDialect dialect = JdbcDialects.get(url);
        url = dialect.wrapUrl(url);

        int batchSize = config.getInteger("batchSize", getDefaultBatchSize());

        // 解析写入模式
        String modeStr = config.getString("mode", "insert").toUpperCase();
        WriteMode mode;
        try {
            mode = WriteMode.valueOf(modeStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("不支持的写入模式: " + modeStr + "。支持的模式: INSERT, UPSERT");
        }

        // 解析主键字段
        List<String> keyFields = Collections.emptyList();
        if (mode == WriteMode.UPSERT) {
            String keyFieldsStr = config.getString("keyFields");
            if (keyFieldsStr == null || keyFieldsStr.isEmpty()) {
                throw new IllegalArgumentException("UPSERT 模式需要配置 keyFields（主键/唯一键字段）");
            }
            keyFields = Arrays.asList(keyFieldsStr.split(","));
        }

        // table 模式检查 upsert 支持
        if (table != null && mode == WriteMode.UPSERT && !dialect.supportsUpsert()) {
            throw new IllegalArgumentException("数据库 " + dialect.getName() + " 不支持 UPSERT 模式");
        }

        JdbcSinkConfig jdbcConfig = JdbcSinkConfig.builder()
                .url(url)
                .username(username)
                .password(password)
                .table(table)
                .sql(sql)
                .batchSize(batchSize)
                .mode(mode)
                .keyFields(keyFields)
                .dialect(dialect)
                .build();

        String configMode = table != null ? "table" : "sql";
        log.info("创建 JDBC Sink, mode={}, writeMode={}, batchSize={}", configMode, mode, batchSize);

        return new JdbcSinkFunction(jdbcConfig);
    }
}