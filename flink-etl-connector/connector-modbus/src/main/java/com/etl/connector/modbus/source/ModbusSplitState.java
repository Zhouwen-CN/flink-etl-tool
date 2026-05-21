package com.etl.connector.modbus.source;

import com.etl.core.source.BaseSplitState;
import lombok.Getter;

/**
 * Modbus Split 状态
 */
@Getter
public class ModbusSplitState extends BaseSplitState<ModbusSplit> {
    public ModbusSplitState(ModbusSplit split) {
        super(split);
    }
}
