package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.Getter;

/**
 * MQTT 分片
 * 单分片模式，包含完整的 MQTT 配置
 */
@Getter
public class MqttSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    /** 分片 ID */
    private final String splitId;

    /** MQTT 配置 */
    private final MqttSourceConfig config;

    /**
     * 构造函数
     *
     * @param splitId 分片 ID
     * @param config  MQTT 配置
     */
    public MqttSplit(String splitId, MqttSourceConfig config) {
        this.splitId = splitId;
        this.config = config;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "MqttSplit{" +
                "splitId='" + splitId + '\'' +
                ", topic='" + config.getTopic() + '\'' +
                ", broker='" + config.getBroker() + '\'' +
                '}';
    }
}