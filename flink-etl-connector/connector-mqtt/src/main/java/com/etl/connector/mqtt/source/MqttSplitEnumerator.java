package com.etl.connector.mqtt.source;

import com.etl.connector.mqtt.source.config.MqttSourceConfig;
import com.etl.core.source.BaseSplitEnumerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * MQTT 分片枚举器
 * 单分片模式，创建一个包含完整配置的分片
 */
@Slf4j
public class MqttSplitEnumerator extends BaseSplitEnumerator<MqttSplit, MqttEnumCheckpoint> {

    private final MqttSourceConfig mqttSourceConfig;

    /**
     * 构造函数
     *
     * @param context          枚举器上下文
     * @param mqttSourceConfig MQTT 配置
     */
    public MqttSplitEnumerator(
            SplitEnumeratorContext<MqttSplit> context,
            MqttSourceConfig mqttSourceConfig) {
        super(context);
        this.mqttSourceConfig = mqttSourceConfig;
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context          枚举器上下文
     * @param checkpoint       检查点
     * @param mqttSourceConfig MQTT 配置
     */
    public MqttSplitEnumerator(
            SplitEnumeratorContext<MqttSplit> context,
            MqttEnumCheckpoint checkpoint,
            MqttSourceConfig mqttSourceConfig) {
        super(context, checkpoint);
        this.mqttSourceConfig = mqttSourceConfig;
    }

    @Override
    public void start() {
        log.info("MqttSplitEnumerator 启动，broker: {}, topic: {}",
                mqttSourceConfig.getBroker(), mqttSourceConfig.getTopic());

        // 创建单分片
        MqttSplit split = new MqttSplit("mqtt-split-0", mqttSourceConfig);

        // 添加到待处理队列
        addPendingSplits(Collections.singletonList(split));
        log.info("创建 MQTT 分片: {}", split);
    }

    @Override
    public MqttEnumCheckpoint snapshotState(long checkpointId) {
        List<MqttSplit> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new MqttEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("MqttSplitEnumerator 关闭");
    }
}