package com.etl.connector.mqtt.source;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

/**
 * MQTT 记录发射器
 * 将 JSON 消息转换为 Row 并发射到下游
 */
@Slf4j
public class MqttRecordEmitter implements RecordEmitter<MqttMessageRecord, Row, MqttSplitState> {

    @Override
    public void emitRecord(MqttMessageRecord record, SourceOutput<Row> output, MqttSplitState splitState) throws Exception {
        try {
            // 解析 JSON
            JsonNode jsonNode = JsonUtils.readTree(record.getJsonContent());

            if (jsonNode == null || !jsonNode.isObject()) {
                log.warn("JSON 解析失败或不是对象类型: {}", record.getJsonContent());
                return;
            }

            // 转换为 Row
            EtlSchema schema = splitState.getSplit().getConfig().getSchema();
            Row row = JsonToRowConverter.convertJsonToRow(jsonNode, schema);

            // 发射到下游
            output.collect(row);

            // 更新状态
            splitState.addRecordsRead(1);

        } catch (Exception e) {
            log.error("JSON 转 Row 失败: {}, 原始消息: {}", e.getMessage(), record.getJsonContent());
            // 跳过该消息，继续处理后续消息
        }
    }
}