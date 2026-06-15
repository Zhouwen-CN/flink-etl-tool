package com.etl.connector.doris.sink.config;

import com.etl.core.config.SinkConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.val;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Doris Sink 配置
 */
@Getter
@Builder
public class DorisSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String tablePattern = "^[a-zA-Z0-9_-]+\\.[a-zA-Z0-9_-]+$";
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
    /**
     * 序列化格式，默认 json，通过 SPI 加载
     */
    private final String format;
    /**
     * CDC 表映射（debezium-json / ogg-json 格式必须）
     */
    private final Map<String, String> tableMapping;

    /**
     * 从 SinkConfig 解析并校验
     */
    public static DorisSinkConfig fromSinkConfig(SinkConfig config) {
        String fenodes = config.get("fenodes", String.class);
        Preconditions.checkArgument(fenodes != null && !fenodes.trim().isEmpty(), "fenodes 不能为空");

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

        // tableMapping 解析
        Map<String, String> tableMapping = new HashMap<>();
        Object object = config.get("tableMapping");
        if (object != null) {
            if (!(object instanceof Map<?, ?>)) {
                throw new IllegalArgumentException("tableMapping 不是映射类型");
            }
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                val key = String.valueOf(entry.getKey());
                val value = String.valueOf(entry.getValue());

                Preconditions.checkArgument(value.matches(tablePattern),
                        "tableMapping value 必须为 db.table 格式: " + value);
                tableMapping.put(key, value);
            }
        }

        // format 解析，校验
        String format = config.get("format", String.class, "json");
        String table = config.get("table", String.class);
        if ("json".equals(format)) {
            Preconditions.checkArgument(table != null && !table.trim().isEmpty(),
                    "format 为 json 时 table 不能为空");
            Preconditions.checkArgument(table.matches(tablePattern),
                    "table 必须为 db.table 格式: " + table);
        } else if ("debezium-json".equals(format) || "ogg-json".equals(format)) {
            Preconditions.checkArgument(!tableMapping.isEmpty(),
                    "format 为 " + format + " 时 tableMapping 不能为空");
        }

        return DorisSinkConfig.builder()
                .fenodes(fenodes)
                .table(table)
                .username(username)
                .password(password)
                .labelPrefix(labelPrefix)
                .batchSize(batchSize)
                .batchIntervalMs(batchIntervalMs)
                .format(format)
                .tableMapping(tableMapping)
                .build();
    }
}
