package com.etl.connector.jdbc.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

/**
 * Row 记录发射器
 * 将 Row 直接发射到下游
 *
 * <p>支持：
 * <ul>
 *   <li>事件时间戳提取（如果 Row 中包含时间字段）</li>
 *   <li>读取统计信息更新</li>
 * </ul>
 */
@Slf4j
public class RowRecordEmitter implements RecordEmitter<Row, Row, RangeSplitState> {

    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, RangeSplitState splitState) throws Exception {
        // 发射记录到下游
        output.collect(record);

        // 更新状态
        splitState.addRecordsRead(1);
    }
}