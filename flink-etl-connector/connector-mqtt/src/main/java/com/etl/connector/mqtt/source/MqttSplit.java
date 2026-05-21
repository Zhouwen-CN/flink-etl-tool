package com.etl.connector.mqtt.source;

import com.etl.connector.mqtt.source.config.MqttSourceConfig;
import com.etl.core.source.BaseSourceSplit;
import lombok.Getter;

/**
 * MQTT 分片
 * 单分片模式，包含完整的 MQTT 配置
 */
@Getter
public class MqttSplit implements BaseSourceSplit {

    private static final long serialVersionUID = 1L;

    /**
     * 分片 ID && topic
     */
    private final String splitId;

    /**
     * 配置信息
     */
    private final MqttSourceConfig config;

    public MqttSplit(String splitId,
                     MqttSourceConfig config
    ) {
        this.splitId = splitId;
        this.config=config;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}