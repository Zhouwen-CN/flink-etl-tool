package com.etl.connector.http.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

/**
 * HTTP 记录发射器
 * 将 Row 直接发射到下游
 */
@Slf4j
public class HttpRecordEmitter implements RecordEmitter<Row, Row, HttpSplitState> {

    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, HttpSplitState splitState) throws Exception {
        // 发射记录到下游
        output.collect(record);

        // 更新状态
        splitState.addRecordsRead(1);
    }
}