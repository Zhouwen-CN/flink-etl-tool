package com.etl.connector.kafka.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.etl.connector.kafka.source.format.KafkaFormatPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Kafka Source 插件
 * 封装 Flink KafkaSource，支持 JSON 格式消息消费
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class KafkaSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "kafka";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 Kafka Source");

        // 解析配置
        KafkaSourceConfig kafkaConfig = KafkaSourceConfig.fromSourceConfig(config);

        // 创建反序列化器（schema 是业务数据 schema）
        KafkaFormatPlugin formatPlugin = kafkaConfig.getFormatPlugin();
        KafkaRecordDeserializationSchema<Row> deserializer =
            formatPlugin.createDeserializer(kafkaConfig.getSchema());

        log.info("Kafka Source format: {}", formatPlugin.identifier());

        // 构建 KafkaSource
        KafkaSourceBuilder<Row> builder = KafkaSource.<Row>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setGroupId(kafkaConfig.getGroupId())
                .setStartingOffsets(kafkaConfig.getOffsetsInitializer())
                .setDeserializer(deserializer);

        // 设置 Topic 订阅方式
        if (kafkaConfig.isTopicsMode()) {
            builder.setTopics(kafkaConfig.getTopics());
            log.info("订阅 Topic 列表: {}", kafkaConfig.getTopics());
        } else {
            builder.setTopicPattern(Pattern.compile(kafkaConfig.getTopicPattern()));
            // 每 10 秒检查一次新分区
            builder.setProperty("partition.discovery.interval.ms", "10000");
            log.info("订阅 Topic 正则: {}", kafkaConfig.getTopicPattern());
        }

        // 设置额外的 Kafka 属性
        Properties kafkaProperties = kafkaConfig.getKafkaProperties();
        if (kafkaProperties != null && !kafkaProperties.isEmpty()) {
            builder.setProperties(kafkaProperties);
            log.info("额外 Kafka 配置: {}", kafkaProperties);
        }

        return builder.build();
    }
}