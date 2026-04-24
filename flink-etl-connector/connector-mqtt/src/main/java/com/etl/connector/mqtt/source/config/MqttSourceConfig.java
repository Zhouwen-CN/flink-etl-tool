package com.etl.connector.mqtt.source.config;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
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
    /** 订阅的 topic 名称 */
    private final String topic;
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
        String broker = config.getString("broker");
        Preconditions.checkArgument(broker != null && !broker.isEmpty(),
                "broker 不能为空");

        String topic = config.getString("topic");
        Preconditions.checkArgument(topic != null && !topic.isEmpty(),
                "topic 不能为空");

        // clientId（可选，默认自动生成）
        String clientId = config.getString("clientId");
        if (clientId == null || clientId.isEmpty()) {
            clientId = "mqtt-source-" + UUID.randomUUID().toString().substring(0, 8);
        }

        // username/password（可选）
        String username = config.getString("username");
        String password = config.getString("password");

        // schema（必填）
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema 不能为空");

        return MqttSourceConfig.builder()
                .broker(broker)
                .topic(topic)
                .clientId(clientId)
                .username(username)
                .password(password)
                .schema(schema)
                .build();
    }
}