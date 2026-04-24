package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;
import lombok.Setter;

/**
 * MQTT 分片状态
 */
@Getter
@Setter
public class MqttSplitState extends BaseSplitState<MqttSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param split MQTT 分片
     */
    public MqttSplitState(MqttSplit split) {
        super(split);
    }

    @Override
    public String toString() {
        return "MqttSplitState{" +
                "split=" + getSplit() +
                ", recordsRead=" + getRecordsRead() +
                '}';
    }
}