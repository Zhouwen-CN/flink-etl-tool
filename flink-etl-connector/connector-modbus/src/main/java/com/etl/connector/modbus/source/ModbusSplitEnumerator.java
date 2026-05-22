package com.etl.connector.modbus.source;

import com.etl.core.source.AbstractSplitEnumerator;
import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Modbus Source 分片枚举器
 * 在 start() 时创建单个 ModbusSplit 并分配到队列
 */
@Slf4j
public class ModbusSplitEnumerator
        extends AbstractSplitEnumerator<ModbusSplit, ModbusEnumCheckpoint> {

    private final ModbusSourceConfig modbusConfig;

    public ModbusSplitEnumerator(
            SplitEnumeratorContext<ModbusSplit> context,
            ModbusSourceConfig modbusConfig) {
        super(context);
        this.modbusConfig = modbusConfig;
    }

    public ModbusSplitEnumerator(
            SplitEnumeratorContext<ModbusSplit> context,
            ModbusEnumCheckpoint checkpoint,
            ModbusSourceConfig modbusConfig) {
        super(context, checkpoint);
        this.modbusConfig = modbusConfig;
    }

    @Override
    public void start() {
        ModbusSplit split = new ModbusSplit(modbusConfig);
        pendingSplits.add(split);
        log.info("Modbus Source 创建单分片: {}", split.splitId());
    }

    @Override
    public ModbusEnumCheckpoint snapshotState(long checkpointId) {
        List<ModbusSplit> pending = new ArrayList<>(pendingSplits);
        return new ModbusEnumCheckpoint(pending);
    }

    @Override
    public void close() {
        log.info("ModbusSplitEnumerator 关闭");
    }
}
