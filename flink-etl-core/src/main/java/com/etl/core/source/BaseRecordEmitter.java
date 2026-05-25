package com.etl.core.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

@Slf4j
public class BaseRecordEmitter<SplitStateT extends AbstractSplitState<?>> implements RecordEmitter<Row, Row, SplitStateT> {

    private final SourceReaderContext context;

    public BaseRecordEmitter(SourceReaderContext context) {
        this.context = context;
    }

    @Override
    public void emitRecord(Row element, SourceOutput<Row> output, SplitStateT splitState) throws Exception {
        // 发射记录到下游
        output.collect(element);

        // 这个指标是 source scope 级别的，需要在 metrics 面板上自己拉图表，不够直观
        // context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();

        // 这里使用 bytes 来指代条数，因为可以在webui上直观的看到
        context.metricGroup().getIOMetricGroup().getNumBytesInCounter().inc();
    }
}
