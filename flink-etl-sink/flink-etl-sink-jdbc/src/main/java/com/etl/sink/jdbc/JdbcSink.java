package com.etl.sink.jdbc;

import com.etl.core.config.SinkConfig;
import com.etl.core.dialect.JdbcDialect;
import com.etl.core.dialect.JdbcDialectLoader;
import com.etl.core.dialect.WriteMode;
import com.etl.core.sink.AbstractSink;
import com.etl.core.utils.SqlUtils;
import com.etl.sink.jdbc.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        // 支持显式配置 dialect
        String dialectName = config.getString("dialect");
        JdbcDialect dialect = JdbcDialectLoader.get(dialectName, url);

        String table = config.getString("table");
        String sql = config.getString("sql");
        Preconditions.checkArgument(table != null || sql != null,
            "table 和 sql 必须配置其中一个");

        String modeStr = config.getString("mode", "INSERT");
        WriteMode mode = WriteMode.valueOf(modeStr.toUpperCase());

        List<String> keyFields = config.getList("keyFields");

        if (keyFields == null && mode == WriteMode.UPSERT) {
            // UPSERT 模式必须配置 table，不能配置 sql
            Preconditions.checkArgument(table != null,
                    "UPSERT 模式必须配置 table，因为需要主键信息");

            // 自动获取主键
            Map<String, Integer> pkInfo = SqlUtils.getPrimaryKey(url, table, username, password);
            keyFields = new ArrayList<>(pkInfo.keySet());
            log.info("JDBC Sink UPSERT 模式自动获取主键: table={}, keyFields={}", table, keyFields);
        } else {
            log.info("JDBC Sink INSERT 模式: table={}", table);
        }

        Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
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

    /**
     * 获取 JDBC Sink 配置对象（用于测试）
     */
    public JdbcSinkConfig getJdbcSinkConfig() {
        return jdbcSinkConfig;
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) throws IOException {
        return new JdbcSinkWriter(context, jdbcSinkConfig);
    }
}