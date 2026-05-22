package com.etl.connector.modbus.source;

import com.etl.core.source.AbstractSplitState;
import lombok.Getter;

/**
 * Modbus Split 状态
 */
@Getter
public class ModbusSplitState extends AbstractSplitState<ModbusSplit> {
    public ModbusSplitState(ModbusSplit split) {
        super(split);
    }
}
