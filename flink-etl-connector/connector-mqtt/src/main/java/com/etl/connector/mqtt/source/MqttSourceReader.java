package com.etl.connector.mqtt.source;

import com.etl.core.source.BaseSourceReader;
import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * MQTT Source Reader
 * 继承 BaseSourceReader，自动处理线程模型和状态管理
 */
@Slf4j
public class MqttSourceReader extends BaseSourceReader<MqttMessageRecord, Row, MqttSplit, MqttSplitState> {

    /**
     * 构造函数
     *
     * @param splitReaderSupplier 分片读取器供应器
     * @param context             读取器上下文
     */
    public MqttSourceReader(
            Supplier<BaseSplitReader<MqttMessageRecord, MqttSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new MqttRecordEmitter(), context);
    }

    @Override
    public MqttSplitState initializedState(MqttSplit split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new MqttSplitState(split);
    }

    @Override
    protected MqttSplit toSplitType(String splitId, MqttSplitState splitState) {
        return splitState.getSplit();
    }
}