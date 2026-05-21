package com.etl.connector.modbus.source;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import lombok.Getter;

/**
 * Modbus Source 单分片
 */
@Getter
public class ModbusSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    private final String splitId = "modbus-split-0";
    private final ModbusSourceConfig modbusConfig;

    public ModbusSplit(ModbusSourceConfig modbusConfig) {
        this.modbusConfig = modbusConfig;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "ModbusSplit{splitId='" + splitId + "'}";
    }
}
