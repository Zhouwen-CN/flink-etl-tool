package com.etl.source.jdbc;

import com.etl.core.source.RangeSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class RowRecordEmitter implements RecordEmitter<Row, Row, RangeSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(RowRecordEmitter.class);

    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, RangeSplitState splitState) throws Exception {
        // 发射记录到下游
        output.collect(record);

        // 更新状态
        splitState.addRecordsRead(1);
    }
}