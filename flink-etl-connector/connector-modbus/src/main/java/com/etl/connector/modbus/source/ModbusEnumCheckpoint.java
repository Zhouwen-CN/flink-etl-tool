package com.etl.connector.modbus.source;

import com.etl.core.source.AbstractEnumCheckpoint;
import lombok.Getter;

import java.util.List;

/**
 * Modbus Source Enumerator 检查点
 */
@Getter
public class ModbusEnumCheckpoint extends AbstractEnumCheckpoint<ModbusSplit> {

    private static final long serialVersionUID = 1L;

    public ModbusEnumCheckpoint(List<ModbusSplit> pendingSplits) {
        super(pendingSplits);
    }
}
