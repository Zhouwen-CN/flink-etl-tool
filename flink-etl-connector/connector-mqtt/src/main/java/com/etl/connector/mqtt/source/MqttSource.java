package com.etl.connector.mqtt.source;

import com.etl.connector.mqtt.source.config.MqttSourceConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * MQTT Source 实现
 * 使用 Paho 客户端订阅 MQTT topic，消费 JSON 消息
 */
@Slf4j
public class MqttSource extends AbstractSplitSource<MqttSplit, MqttEnumCheckpoint> {

    private final MqttSourceConfig mqttSourceConfig;

    public MqttSource(SourceConfig config) {
        super(config);
        this.mqttSourceConfig = MqttSourceConfig.fromSourceConfig(config);
        log.info("创建 MqttSource: broker={}, topic={}, clientId={}",
                mqttSourceConfig.getBroker(),
                mqttSourceConfig.getTopic(),
                mqttSourceConfig.getClientId());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MqttSplit, MqttEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<MqttSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new MqttSplitEnumerator(enumContext, mqttSourceConfig);
    }

    @Override
    public SplitEnumerator<MqttSplit, MqttEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<MqttSplit> enumContext,
            MqttEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new MqttSplitEnumerator(enumContext, checkpoint, mqttSourceConfig);
    }

    @Override
    public SourceReader<Row, MqttSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        Supplier<BaseSplitReader<Row, MqttSplit>> splitReaderSupplier =
                MqttSplitReader::new;
        return new MqttSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<MqttSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<MqttEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}