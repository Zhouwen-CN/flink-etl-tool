package com.etl.connector.mqtt.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.BaseRecordEmitter;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * MQTT Source Reader
 * 继承 AbstractSourceReader，自动处理线程模型和状态管理
 */
public class MqttSourceReader extends AbstractSourceReader<Row, Row, MqttSplit> {

    public MqttSourceReader(
            Supplier<AbstractSplitReader<Row, MqttSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new BaseRecordEmitter<>(context), context);
    }
}
