package com.etl.connector.mqtt.source;

import com.etl.core.schema.EtlSchema;
import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.Getter;

/**
 * MQTT 分片
 * 单分片模式，包含完整的 MQTT 配置
 */
@Getter
public class MqttSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    /**
     * 分片 ID && topic
     */
    private final String splitId;

    /**
     * topic
     */
    private final String topic;

    /**
     * MQTT broker 地址，如 tcp://localhost:1883
     */
    private final String broker;
    /**
     * 客户端 ID（可选，默认自动生成 UUID）
     */
    private final String clientId;
    /**
     * 认证用户名（可选）
     */
    private final String username;
    /**
     * 认证密码（可选）
     */
    private final String password;
    /**
     * Schema 定义
     */
    private final EtlSchema schema;


    public MqttSplit(String topic,
                     String broker,
                     String clientId,
                     String username,
                     String password,
                     EtlSchema schema
    ) {
        this.splitId = topic;
        this.topic = topic;
        this.broker = broker;
        this.clientId = clientId;
        this.username = username;
        this.password = password;
        this.schema = schema;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "MqttSplit{" +
                "splitId='" + splitId + '\'' +
                ", topic='" + topic + '\'' +
                ", broker='" + broker + '\'' +
                ", clientId='" + clientId + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", schema=" + schema +
                '}';
    }
}