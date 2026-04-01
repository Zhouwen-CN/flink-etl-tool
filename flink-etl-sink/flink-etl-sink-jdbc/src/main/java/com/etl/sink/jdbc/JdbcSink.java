package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialects;
import com.etl.core.dialect.WriteMode;
import com.etl.core.sink.AbstractSink;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * JDBC Sink 实现
 * 支持将数据写入关系型数据库
 */
@Slf4j
public class JdbcSink extends AbstractSink {

    private final JdbcSinkConfig jdbcSinkConfig;

    public JdbcSink(SinkConfig config) {
        super(config);

        String url = Preconditions.checkNotNull(config.getString("url"), "url is null");
        String username = config.getString("username");
        String password = config.getString("password");

        JdbcDialect dialect = JdbcDialects.get(url);

        String table = config.getString("table");
        String sql = config.getString("sql");
        Preconditions.checkArgument(table != null || sql != null,
            "table 和 sql 必须配置其中一个");

        String modeStr = config.getString("mode", "INSERT");
        WriteMode mode = WriteMode.valueOf(modeStr.toUpperCase());

        List<String> keyFields = null;
        if (mode == WriteMode.UPSERT) {
            List<String> keyFieldsConfig = config.getList("keyFields");
            Preconditions.checkNotNull(keyFieldsConfig, "UPSERT 模式必须配置 keyFields");
            keyFields = keyFieldsConfig;
            log.info("JDBC Sink upsert 模式: table={}, keyFields={}", table, keyFields);
        } else {
            log.info("JDBC Sink insert 模式: table={}", table);
        }

        Integer batchSize = config.getInteger("batchSize", 100);
        Preconditions.checkArgument(batchSize != null && batchSize > 0, "batchSize must be greater than 0");

        this.jdbcSinkConfig = JdbcSinkConfig.builder()
            .url(dialect.wrapUrl(url))
            .username(username)
            .password(password)
            .table(table)
            .sql(sql)
            .dialect(dialect)
            .mode(mode)
            .keyFields(keyFields)
            .batchSize(batchSize)
            .build();

        log.info("创建 JdbcSink: {}", this.jdbcSinkConfig);
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) throws IOException {
        return new JdbcSinkWriter(context, jdbcSinkConfig);
    }
}