package com.etl.connector.modbus.source;

import com.etl.core.source.AbstractSplitState;
import lombok.Getter;

/**
 * Modbus Split 状态
 */
@Getter
public class ModbusSplitState extends AbstractSplitState<ModbusSplit> {

    private static final long serialVersionUID = 1L;

    public ModbusSplitState(ModbusSplit split) {
        super(split);
    }
}
