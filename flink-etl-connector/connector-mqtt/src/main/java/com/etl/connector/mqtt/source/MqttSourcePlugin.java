package com.etl.connector.mqtt.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.types.Row;

/**
 * MQTT Source 插件
 * 使用 Eclipse Paho 客户端订阅 MQTT topic
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class MqttSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "mqtt";
    }

    @Override
    public Source<Row, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 MQTT Source");
        return new MqttSource(config);
    }
}