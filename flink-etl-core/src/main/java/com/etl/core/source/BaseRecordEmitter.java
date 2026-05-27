package com.etl.core.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.types.Row;

@Slf4j
public class BaseRecordEmitter<SplitT extends BaseSourceSplit> implements RecordEmitter<Row, Row, BaseSplitState<SplitT>> {

    private final SourceReaderContext context;

    public BaseRecordEmitter(SourceReaderContext context) {
        this.context = context;
    }

    @Override
    public void emitRecord(Row element, SourceOutput<Row> output, BaseSplitState<SplitT> splitState) throws Exception {
        output.collect(element);

        OperatorIOMetricGroup ioMetricGroup = context.metricGroup().getIOMetricGroup();
        ioMetricGroup.getNumRecordsInCounter();
        ioMetricGroup.getNumBytesInCounter().inc();
    }
}
