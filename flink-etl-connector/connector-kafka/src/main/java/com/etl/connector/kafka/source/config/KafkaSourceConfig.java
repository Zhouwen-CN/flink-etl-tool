package com.etl.connector.kafka.source.config;

import com.etl.connector.kafka.source.StartupMode;
import com.etl.connector.kafka.source.format.KafkaFormatLoader;
import com.etl.connector.kafka.source.format.KafkaFormatPlugin;
import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Source 配置
 */
@Getter
@Builder
public class KafkaSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Kafka 集群地址 */
    private final String bootstrapServers;
    /** 消费者组 ID */
    private final String groupId;
    /** Topic 列表（与 topicPattern 二选一） */
    private final List<String> topics;
    /** Topic 正则表达式（与 topics 二选一） */
    private final String topicPattern;
    /** 启动模式 */
    private final StartupMode startupMode;
    /** 额外的 Kafka consumer 配置 */
    private final Properties kafkaProperties;
    /** Schema 定义 */
    private final EtlSchema schema;
    /** 消息格式：json、debezium-json 等 */
    private final KafkaFormatPlugin formatPlugin;

    /**
     * 从 SourceConfig 解析配置
     */
    public static KafkaSourceConfig fromSourceConfig(SourceConfig config) {
        // 校验必填参数
        String bootstrapServers = config.getString("bootstrapServers");
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("bootstrapServers 不能为空");
        }

        String groupId = config.getString("groupId");
        if (groupId == null) {
            throw new IllegalArgumentException("groupId 不能为空");
        }

        // 校验 topics 和 topicPattern 至少配置一个
        List<String> topics = parseTopics(config);
        String topicPattern = config.getString("topicPattern");
        if ((topics == null || topics.isEmpty()) && topicPattern == null) {
            throw new IllegalArgumentException("topics 和 topicPattern 至少需要配置一个");
        }

        // 解析 startupMode
        String startupModeValue = config.getString("startupMode", "earliest");
        StartupMode startupMode = StartupMode.fromConfigValue(startupModeValue);

        // 校验 schema
        EtlSchema schema = config.getSchema();
        if (schema == null) {
            throw new IllegalArgumentException("schema 不能为空");
        }

        // 解析额外的 Kafka 属性
        Properties kafkaProperties = parseKafkaProperties(config);

        // 解析 format（默认 "json"）
        String format = config.getString("format", "json");

        // 校验 format 是否支持
        KafkaFormatPlugin formatPlugin = KafkaFormatLoader.getFormatPlugin(format);
        if (formatPlugin == null) {
            List<String> supported = KafkaFormatLoader.supportedFormats();
            throw new IllegalArgumentException(
                String.format("不支持的 Kafka format: '%s'，支持的格式: %s",
                        format, supported)
            );
        }

        return KafkaSourceConfig.builder()
                .bootstrapServers(bootstrapServers)
                .groupId(groupId)
                .topics(topics)
                .topicPattern(topicPattern)
                .startupMode(startupMode)
                .kafkaProperties(kafkaProperties)
                .schema(schema)
                .formatPlugin(formatPlugin)
                .build();
    }

    /**
     * 判断是否使用 Topic 列表模式
     */
    public boolean isTopicsMode() {
        return topics != null && !topics.isEmpty();
    }

    /**
     * 获取 Flink OffsetsInitializer
     * 封装枚举到 Flink 组件的转换，调用方无需了解枚举内部实现
     */
    public OffsetsInitializer getOffsetsInitializer() {
        return startupMode.toOffsetsInitializer();
    }

    /**
     * 解析 topics 列表
     */
    private static List<String> parseTopics(SourceConfig config) {
        Object topicsObj = config.get("topics");
        if (topicsObj == null) {
            return null;
        }
        if (topicsObj instanceof List) {
            List<String> result = new ArrayList<>();
            for (Object item : (List<?>) topicsObj) {
                if (item != null) {
                    result.add(item.toString());
                }
            }
            return result;
        }
        return null;
    }

    /**
     * 解析额外的 Kafka 配置属性
     */
    @SuppressWarnings("unchecked")
    private static Properties parseKafkaProperties(SourceConfig config) {
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