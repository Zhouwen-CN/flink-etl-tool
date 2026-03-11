package com.etl.source.jdbc;

import com.etl.core.source.RangeSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC 记录发射器
 * 将 JdbcRecord 发射到下游
 *
 * <p>支持：
 * <ul>
 *   <li>事件时间戳提取（如果 Row 中包含时间字段）</li>
 *   <li>读取统计信息更新</li>
 * </ul>
 */
public class JdbcRecordEmitter implements RecordEmitter<JdbcRecord, JdbcRecord, RangeSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcRecordEmitter.class);

    @Override
    public void emitRecord(JdbcRecord record, SourceOutput<JdbcRecord> output, RangeSplitState splitState) throws Exception {
        // 发射记录到下游
        output.collect(record);

        // 更新状态
        splitState.addRecordsRead(1);
    }
}