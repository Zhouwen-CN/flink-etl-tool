package com.etl.connector.modbus.source;

import com.etl.core.source.BaseSourceReader;
import com.etl.core.source.BaseSplitReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Modbus Source 阅读器
 */
public class ModbusSourceReader
        extends BaseSourceReader<Row, Row, ModbusSplit, ModbusSplitState> {

    public ModbusSourceReader(
            Supplier<BaseSplitReader<Row, ModbusSplit>> splitReaderSupplier,
            SourceReaderContext context) {
        super(splitReaderSupplier, new ModbusRecordEmitter(), context);
    }

    @Override
    public ModbusSplitState initializedState(ModbusSplit split) {
        return new ModbusSplitState(split);
    }

    @Override
    protected ModbusSplit toSplitType(String splitId, ModbusSplitState splitState) {
        return splitState.getSplit();
    }
}
