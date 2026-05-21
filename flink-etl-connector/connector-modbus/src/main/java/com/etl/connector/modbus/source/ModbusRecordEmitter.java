package com.etl.connector.modbus.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

/**
 * Modbus Source 记录发射器
 */
public class ModbusRecordEmitter implements RecordEmitter<Row, Row, ModbusSplitState> {
    @Override
    public void emitRecord(Row record, SourceOutput<Row> output, ModbusSplitState splitState) throws Exception {
        output.collect(record);
        splitState.addRecordsRead(1);
    }
}
