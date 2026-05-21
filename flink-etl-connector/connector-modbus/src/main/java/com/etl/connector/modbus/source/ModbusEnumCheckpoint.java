package com.etl.connector.modbus.source;

import com.etl.core.source.BaseEnumCheckpoint;
import lombok.Getter;
import java.util.List;

/**
 * Modbus Source Enumerator 检查点
 */
@Getter
public class ModbusEnumCheckpoint extends BaseEnumCheckpoint<ModbusSplit> {
    public ModbusEnumCheckpoint(List<ModbusSplit> pendingSplits) {
        super(pendingSplits);
    }
}
