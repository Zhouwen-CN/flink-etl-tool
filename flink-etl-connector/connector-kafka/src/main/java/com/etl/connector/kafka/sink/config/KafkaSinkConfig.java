package com.etl.connector.kafka.sink.config;

import com.etl.core.config.SinkConfig;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Sink 配置
 */
@Getter
@Builder
public class KafkaSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Kafka 集群地址 */
    private final String bootstrapServers;
    /** 目标 Topic */
    private final String topic;
    /** Key 字段名（可选） */
    private final String keyField;
    /** Kafka Producer 配置 */
    private final Properties kafkaProperties;

    /**
     * 从 SinkConfig 解析配置
     */
    public static KafkaSinkConfig fromSinkConfig(SinkConfig config) {
        // 校验必填参数
        String bootstrapServers = config.getString("bootstrapServers");
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException("bootstrapServers 不能为空");
        }

        String topic = config.getString("topic");
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空");
        }

        // 可选参数
        String keyField = config.getString("keyField");

        // 解析 properties
        Properties kafkaProperties = parseKafkaProperties(config);

        return KafkaSinkConfig.builder()
                .bootstrapServers(bootstrapServers)
                .topic(topic)
                .keyField(keyField)
                .kafkaProperties(kafkaProperties)
                .build();
    }

    /**
     * 解析额外的 Kafka 配置属性
     */
    @SuppressWarnings("unchecked")
    private static Properties parseKafkaProperties(SinkConfig config) {
        Properties properties = new Properties();
        Object propsObj = config.get("properties");
        if (propsObj instanceof Map) {
            ((Map<String, Object>) propsObj).forEach((key, value) -> {
                if (key != null && value != null) {
                    properties.setProperty(key, value.toString());
                }
            });
        }
        return properties;
    }
}