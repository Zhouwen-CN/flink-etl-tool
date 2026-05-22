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
public class MqttRecordEmitter implements RecordEmitter<Row, Row, MqttSplitState> {

    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, MqttSplitState splitState) throws Exception {
        // 发射到下游
        output.collect(record);

        // 更新状态
        splitState.addRecordsRead();
    }
}