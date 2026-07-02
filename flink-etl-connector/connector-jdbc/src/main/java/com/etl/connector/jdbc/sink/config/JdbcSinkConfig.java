package com.etl.connector.jdbc.sink.config;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.dialect.JdbcDialectLoader;
import com.etl.connector.jdbc.dialect.WriteMode;
import com.etl.core.config.SinkConfig;
import com.etl.core.exception.NoPrimaryKeyException;
import com.etl.core.util.SqlUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JDBC Sink 配置
 */
@Getter
@Builder
@Slf4j
public class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 数据库连接 URL */
    private final String url;
    /** 用户名 */
    private final String username;
    /** 密码 */
    private final String password;
    /** 目标表名（与 sql 二选一，优先） */
    private final String table;
    /** 自定义 SQL，支持具名占位符 :paramName */
    private final String sql;
    /** 批量写入大小，默认 100 */
    private final Integer batchSize;
    /** 批量刷写间隔（毫秒），默认 0 表示禁用 */
    private final Long batchIntervalMs;
    /** 写入模式：INSERT 或 UPSERT */
    private final WriteMode mode;
    /** Upsert 模式下的主键/唯一键字段列表 */
    private final List<String> keyFields;
    /** 数据库方言 */
    private final JdbcDialect dialect;

    /**
     * 从 SinkConfig 创建 JdbcSinkConfig，在此完成所有参数校验和推断
     */
    public static JdbcSinkConfig fromSinkConfig(SinkConfig config, int defaultBatchSize) {
        String url = Preconditions.checkNotNull(config.get("url", String.class), "url is null");
        String username = config.get("username", String.class);
        String password = config.get("password", String.class);

        // 支持显式配置 dialect
        String dialectName = config.get("dialect", String.class);
        JdbcDialect dialect = JdbcDialectLoader.get(dialectName, url);
        url = dialect.wrapUrl(url);

        String table = config.get("table", String.class);
        String sql = config.get("sql", String.class);
        String modeStr = config.get("mode", String.class, "UPSERT");
        WriteMode mode = WriteMode.valueOf(modeStr.toUpperCase());

        List<String> keyFields = config.get("keyFields", List.class);

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
                pkInfo = SqlUtil.getPrimaryKey(url, table, username, password);
            } catch (NoPrimaryKeyException e) {
                throw new RuntimeException(
                        String.format("表 '%s' 没有主键，无法使用 %s 模式。请使用 INSERT 模式、手动配置 keyFields 或为表添加主键",
                                e.getTableName(), mode));
            }
            keyFields = pkInfo.stream().map(Pair::getKey).collect(Collectors.toList());
            log.info("JDBC Sink {} 模式自动获取主键: table={}, keyFields={}", mode, table, keyFields);
        }

        Integer batchSize = config.get("batchSize", Integer.class, defaultBatchSize);
        Preconditions.checkArgument(batchSize != null && batchSize > 0, "batchSize must be greater than 0");

        Long batchIntervalMs = config.get("batchIntervalMs", Long.class, 0L);

        log.info("创建 JdbcSink: url={}, dialect={}, table={}, sql={}, mode={}, keyFields={}, batchSize={}, batchIntervalMs={}",
                dialect.wrapUrl(url),
                dialect,
                table,
                sql,
                mode,
                keyFields,
                batchSize,
                batchIntervalMs
        );

        return JdbcSinkConfig.builder()
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
    }

    /**
     * 根据写入模式校验配置参数
     */
    private static void validateAndConfigureMode(WriteMode mode, String table, String sql, List<String> keyFields) {
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
}