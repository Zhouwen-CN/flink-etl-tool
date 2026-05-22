package com.etl.connector.mqtt.source;

import com.etl.core.source.AbstractEnumCheckpoint;

import java.util.Collection;

/**
 * MQTT 分片枚举器检查点
 */
public class MqttEnumCheckpoint extends AbstractEnumCheckpoint<MqttSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public MqttEnumCheckpoint(Collection<MqttSplit> pendingSplits) {
        super(pendingSplits);
    }
}