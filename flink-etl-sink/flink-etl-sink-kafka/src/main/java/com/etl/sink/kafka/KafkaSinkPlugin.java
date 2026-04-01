package com.etl.sink.kafka;

import com.etl.core.config.SinkConfig;
import com.etl.core.spi.SinkPlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Kafka Sink 插件
 * 封装 Flink KafkaSink，支持 JSON 格式消息写入
 */
@Slf4j
@AutoService(SinkPlugin.class)
public class KafkaSinkPlugin implements SinkPlugin {

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public Sink<Row> createSink(SinkConfig config) {
        log.info("创建 Kafka Sink");

        // 解析配置
        KafkaSinkConfig kafkaConfig = KafkaSinkConfig.fromSinkConfig(config);

        // 构建 KafkaSink
        KafkaSinkBuilder<Row> builder = KafkaSink.<Row>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setRecordSerializer(new RowToJsonSerializationSchema(kafkaConfig));

        // 设置额外的 Kafka 属性
        Properties kafkaProperties = kafkaConfig.getKafkaProperties();
        if (kafkaProperties != null && !kafkaProperties.isEmpty()) {
            builder.setKafkaProducerConfig(kafkaProperties);
            log.info("额外 Kafka Producer 配置: {}", kafkaProperties);
        }

        log.info("Kafka Sink 配置完成: bootstrapServers={}, topic={}, keyField={}",
                kafkaConfig.getBootstrapServers(),
                kafkaConfig.getTopic(),
                kafkaConfig.getKeyField());

        return builder.build();
    }
}