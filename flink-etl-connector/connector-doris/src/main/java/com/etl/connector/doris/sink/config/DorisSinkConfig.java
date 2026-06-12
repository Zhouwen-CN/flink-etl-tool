package com.etl.connector.doris.sink.config;

import com.etl.core.config.SinkConfig;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Doris Sink 配置
 */
@Getter
@Builder
public class DorisSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Doris FE 节点，host:port */
    private final String fenodes;
    /** 目标表标识，db.table */
    private final String tableIdentifier;
    /** 用户名 */
    private final String username;
    /** 密码 */
    private final String password;
    /** Stream Load label 前缀（可选） */
    private final String labelPrefix;
    /** 批量缓冲条数（可选） */
    private final Integer batchSize;
    /** 序列化格式，默认 json */
    private final String format;

    /**
     * 从 SinkConfig 解析并校验
     */
    public static DorisSinkConfig fromSinkConfig(SinkConfig config) {
        String fenodes = config.get("fenodes", String.class);
        Preconditions.checkArgument(fenodes != null && !fenodes.trim().isEmpty(), "fenodes 不能为空");

        String tableIdentifier = config.get("tableIdentifier", String.class);
        Preconditions.checkArgument(tableIdentifier != null && !tableIdentifier.trim().isEmpty(),
                "tableIdentifier 不能为空");
        Preconditions.checkArgument(tableIdentifier.contains("."),
                "tableIdentifier 必须为 db.table 格式: " + tableIdentifier);

        String username = config.get("username", String.class);
        Preconditions.checkArgument(username != null && !username.trim().isEmpty(), "username 不能为空");

        // password 允许空字符串，但不允许 null
        String password = config.get("password", String.class);
        Preconditions.checkArgument(password != null, "password 不能为 null");

        String labelPrefix = config.get("labelPrefix", String.class);
        Integer batchSize = config.get("batchSize", Integer.class);
        String format = config.get("format", String.class, "json");

        return DorisSinkConfig.builder()
                .fenodes(fenodes)
                .tableIdentifier(tableIdentifier)
                .username(username)
                .password(password)
                .labelPrefix(labelPrefix)
                .batchSize(batchSize)
                .format(format)
                .build();
    }
}
