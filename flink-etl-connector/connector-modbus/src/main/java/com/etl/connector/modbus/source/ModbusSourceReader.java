package com.etl.connector.modbus.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.BaseRecordEmitter;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Modbus Source 阅读器
 */
public class ModbusSourceReader
        extends AbstractSourceReader<Row, Row, ModbusSplit, ModbusSplitState> {

    public ModbusSourceReader(
            Supplier<AbstractSplitReader<Row, ModbusSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new BaseRecordEmitter<>(context), context);
    }

    @Override
    public ModbusSplitState initializedState(ModbusSplit split) {
        return new ModbusSplitState(split);
    }
}
