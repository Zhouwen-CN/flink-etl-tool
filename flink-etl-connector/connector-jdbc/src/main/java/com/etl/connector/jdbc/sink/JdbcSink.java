package com.etl.connector.jdbc.sink;

import com.etl.core.config.SinkConfig;
import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.dialect.JdbcDialectLoader;
import com.etl.connector.jdbc.dialect.WriteMode;
import com.etl.core.exception.NoPrimaryKeyException;
import com.etl.core.sink.AbstractSink;
import com.etl.core.utils.SqlUtils;
import com.etl.connector.jdbc.sink.config.JdbcSinkConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

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
        String modeStr = config.getString("mode", "UPSERT");
        WriteMode mode = WriteMode.valueOf(modeStr.toUpperCase());

        List<String> keyFields = config.getList("keyFields");

        // 根据写入模式进行校验和配置
        validateAndConfigureMode(mode, table, sql, keyFields);

        // UPSERT/CDC 模式且未配置 keyFields 时，需要自动获取主键
        if ((mode == WriteMode.UPSERT || mode == WriteMode.CDC) && keyFields == null) {
            // 必须配置 table
            Preconditions.checkArgument(table != null,
                    String.format("%s 模式必须配置 table，因为需要主键信息", mode));

            // 自动获取主键
            List<Pair<String, Integer>> pkInfo;
            try {
                pkInfo = SqlUtils.getPrimaryKey(url, table, username, password);
            } catch (NoPrimaryKeyException e) {
                throw new RuntimeException(
                        String.format("表 '%s' 没有主键，无法使用 %s 模式。请使用 INSERT 模式、手动配置 keyFields 或为表添加主键",
                                e.getTableName(), mode));
            }
            keyFields = pkInfo.stream().map(Pair::getKey).collect(Collectors.toList());
            log.info("JDBC Sink {} 模式自动获取主键: table={}, keyFields={}", mode, table, keyFields);
        }

        Integer batchSize = config.getInteger("batchSize", super.getDefaultBatchSize());
        Preconditions.checkArgument(batchSize != null && batchSize > 0, "batchSize must be greater than 0");

        Long batchIntervalMs = config.getLong("batchIntervalMs", 0L);

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
                .batchIntervalMs(batchIntervalMs)
                .build();

        log.info("创建 JdbcSink: {}", this.jdbcSinkConfig);
    }

    /**
     * 根据写入模式校验配置参数
     */
    private void validateAndConfigureMode(WriteMode mode, String table, String sql, List<String> keyFields) {
        switch (mode) {
            case INSERT:
                // INSERT 模式：必须配置 table，不能配置 sql，keyFields 必须为 null
                Preconditions.checkArgument(table != null,
                        "INSERT 模式必须配置 table");
                log.info("JDBC Sink INSERT 模式: table={}", table);
                break;

            case UPSERT:
            case CDC:
                // UPSERT/CDC 模式：必须配置 table，不能配置 sql，keyFields 可选
                Preconditions.checkArgument(table != null,
                        String.format("%s 模式必须配置 table", mode));
                if (keyFields != null) {
                    log.info("JDBC Sink {} 模式使用用户配置主键: table={}, keyFields={}", mode, table, keyFields);
                }
                break;

            case CUSTOM:
                // CUSTOM 模式：必须配置 sql，不能配置 table，keyFields 必须为 null
                Preconditions.checkArgument(sql != null,
                        "CUSTOM 模式必须配置 sql");
                log.info("JDBC Sink CUSTOM 模式: sql={}", sql);
                break;

            default:
                throw new IllegalArgumentException("不支持的写入模式: " + mode);
        }
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext context) throws IOException {
        return new JdbcSinkWriter(context, jdbcSinkConfig);
    }
}