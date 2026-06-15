package com.etl.connector.doris.sink.config;

import com.etl.core.config.SinkConfig;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;

/**
 * Doris Sink 配置
 */
@Getter
@Builder
public class DorisSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * Doris FE 节点，host:port
     */
    private final String fenodes;
    /**
     * 目标表标识，db.table
     */
    private final String table;
    /**
     * 用户名
     */
    private final String username;
    /**
     * 密码
     */
    private final String password;
    /**
     * Stream Load label 前缀（可选）
     */
    private final String labelPrefix;
    /**
     * 批量缓冲条数（可选）
     */
    private final Integer batchSize;
    /**
     * 批量刷写间隔（毫秒）
     */
    private final Long batchIntervalMs;

    private String format;

    private Map<String,String> tableMapping;

    /**
     * 从 SinkConfig 解析并校验
     */
    public static DorisSinkConfig fromSinkConfig(SinkConfig config) {
        String fenodes = config.get("fenodes", String.class);
        Preconditions.checkArgument(fenodes != null && !fenodes.trim().isEmpty(), "fenodes 不能为空");

        String table = config.get("table", String.class);
        Preconditions.checkArgument(table != null && !table.trim().isEmpty(),
                "table 不能为空");
        Preconditions.checkArgument(table.matches("^[a-zA-Z0-9_-]+\\.[a-zA-Z0-9_-]+$"),
                "table 必须为 db.table 格式: " + table);

        String username = config.get("username", String.class);
        Preconditions.checkArgument(username != null && !username.trim().isEmpty(), "username 不能为空");

        // password 允许空字符串，但不允许 null
        String password = config.get("password", String.class);
        Preconditions.checkArgument(password != null, "password 不能为 null");

        String labelPrefix = config.get("labelPrefix", String.class, "doris-sink");
        Integer batchSize = config.get("batchSize", Integer.class, 50000);
        Preconditions.checkArgument(batchSize > 0, "batchSize 需要大于0");
        Long batchIntervalMs = config.get("batchIntervalMs", Long.class, 10000L);
        Preconditions.checkArgument(batchIntervalMs > 0, "batchIntervalMs 需要大于0");

        return DorisSinkConfig.builder()
                .fenodes(fenodes)
                .table(table)
                .username(username)
                .password(password)
                .labelPrefix(labelPrefix)
                .batchSize(batchSize)
                .batchIntervalMs(batchIntervalMs)
                .build();
    }
}
