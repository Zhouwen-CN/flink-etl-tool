package com.etl.connector.mqtt.source.config;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * MQTT Source 配置
 * 用于传递所有参数到 Enumerator 和 SplitReader
 */
@Getter
@Builder
public class MqttSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** MQTT broker 地址，如 tcp://localhost:1883 */
    private final String broker;
    /**
     * 订阅的 topic 列表，支持同时订阅多个 topic
     */
    private final List<String> topics;
    /** 客户端 ID（可选，默认自动生成 UUID） */
    private final String clientId;
    /** 认证用户名（可选） */
    private final String username;
    /** 认证密码（可选） */
    private final String password;
    /** Schema 定义 */
    private final EtlSchema schema;

    /**
     * 从 SourceConfig 解析配置
     *
     * @param config Source 配置
     * @return MQTT 配置对象
     */
    public static MqttSourceConfig fromSourceConfig(SourceConfig config) {
        // 校验必填参数
        String broker = config.get("broker", String.class);
        Preconditions.checkArgument(broker != null && !broker.isEmpty(),
                "broker 不能为空");

        List<String> topics = config.get("topics", List.class);
        Preconditions.checkArgument(topics != null && !topics.isEmpty(),
                "topics 不能为空");
        for (String topic : topics) {
            Preconditions.checkArgument(topic != null && !topic.trim().isEmpty(),
                    "topics 中存在空的 topic");
        }

        // clientId（可选，默认自动生成）
        String clientId = config.get("clientId", String.class);
        if (clientId == null || clientId.isEmpty()) {
            clientId = "mqtt-source-" + UUID.randomUUID().toString().substring(0, 8);
        }

        // username/password（可选）
        String username = config.get("username", String.class);
        String password = config.get("password", String.class);

        // schema（必填）
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema 不能为空");

        return MqttSourceConfig.builder()
                .broker(broker)
                .topics(topics)
                .clientId(clientId)
                .username(username)
                .password(password)
                .schema(schema)
                .build();
    }
}