package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseEnumCheckpoint;
import com.etl.core.source.serde.DefaultCheckpointSerializer;

import java.util.Collection;

/**
 * MQTT 分片枚举器检查点
 */
public class MqttEnumCheckpoint extends BaseEnumCheckpoint<MqttSplit> {

    private static final long serialVersionUID = DefaultCheckpointSerializer.VERSION;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public MqttEnumCheckpoint(Collection<MqttSplit> pendingSplits) {
        super(pendingSplits);
    }
}