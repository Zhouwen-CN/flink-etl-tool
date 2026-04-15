package com.etl.connector.mock.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

/**
 * Mock Source 记录发射器
 * 发射 Row 数据到下游
 */
public class MockRecordEmitter implements RecordEmitter<Row, Row, MockSplitState> {

    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, MockSplitState splitState) throws Exception {
        // 发射记录到下游
        output.collect(record);

        // 更新状态
        splitState.addRecordsRead(1);
    }
}