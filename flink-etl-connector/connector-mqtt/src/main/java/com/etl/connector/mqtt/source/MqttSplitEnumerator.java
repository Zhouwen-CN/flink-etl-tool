package com.etl.connector.mqtt.source;

import com.etl.connector.mqtt.source.config.MqttSourceConfig;
import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.core.source.BaseEnumCheckpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.Collections;

/**
 * MQTT 分片枚举器
 * 单分片模式，创建一个包含完整配置的分片
 */
@Slf4j
public class MqttSplitEnumerator extends AbstractSplitEnumerator<MqttSplit> {

    private final MqttSourceConfig mqttSourceConfig;

    public MqttSplitEnumerator(
            SplitEnumeratorContext<MqttSplit> context,
            MqttSourceConfig mqttSourceConfig) {
        super(context);
        this.mqttSourceConfig = mqttSourceConfig;
    }

    public MqttSplitEnumerator(
            SplitEnumeratorContext<MqttSplit> context,
            BaseEnumCheckpoint<MqttSplit> checkpoint,
            MqttSourceConfig mqttSourceConfig) {
        super(context, checkpoint);
        this.mqttSourceConfig = mqttSourceConfig;
    }

    @Override
    public void start() {
        log.info("MqttSplitEnumerator 启动，broker: {}, topic: {}",
                mqttSourceConfig.getBroker(), mqttSourceConfig.getTopic());

        MqttSplit split = new MqttSplit("mqtt-split-0", mqttSourceConfig);
        addPendingSplits(Collections.singletonList(split));
        log.info("创建 MQTT 分片: {}", split);
    }

    @Override
    public void close() throws IOException {
        log.info("MqttSplitEnumerator 关闭");
    }
}
