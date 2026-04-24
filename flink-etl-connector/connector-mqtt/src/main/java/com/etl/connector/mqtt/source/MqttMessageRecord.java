package com.etl.connector.mqtt.source;

import lombok.Getter;

import java.io.Serializable;

/**
 * MQTT 消息记录
 * 包装 MQTT 消息内容，用于传递到 RecordEmitter
 */
@Getter
public class MqttMessageRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 分片 ID */
    private final String splitId;

    /** JSON 消息内容 */
    private final String jsonContent;

    /**
     * 构造函数
     *
     * @param splitId     分片 ID
     * @param jsonContent JSON 消息内容
     */
    public MqttMessageRecord(String splitId, String jsonContent) {
        this.splitId = splitId;
        this.jsonContent = jsonContent;
    }

    @Override
    public String toString() {
        return "MqttMessageRecord{" +
                "splitId='" + splitId + '\'' +
                ", jsonContent='" + jsonContent + '\'' +
                '}';
    }
}