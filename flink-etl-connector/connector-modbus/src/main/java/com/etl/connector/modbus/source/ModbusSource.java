package com.etl.connector.modbus.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import com.etl.connector.modbus.source.config.ModbusSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * Modbus Source 主类
 * <p>
 * 通过 Modbus TCP 协议读取 Holding Registers。
 * 输出固定 Schema: Row(address: INT, value: INT)
 */
@Slf4j
public class ModbusSource extends AbstractSplitSource<ModbusSplit, ModbusEnumCheckpoint> {

    private final ModbusSourceConfig modbusConfig;
    private final boolean bounded;

    public ModbusSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        super(config);
        this.modbusConfig = ModbusSourceConfig.fromSourceConfig(config, runtimeMode);
        this.bounded = runtimeMode == RuntimeExecutionMode.BATCH;
    }

    @Override
    public Boundedness getBoundedness() {
        if (bounded) {
            return Boundedness.BOUNDED;
        }
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW_NAMED(
                new String[]{"address", "value"},
                Types.INT, Types.INT
        );
    }

    @Override
    public SplitEnumerator<ModbusSplit, ModbusEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<ModbusSplit> enumContext) {
        log.info("创建 ModbusSplitEnumerator");
        return new ModbusSplitEnumerator(enumContext, modbusConfig);
    }

    @Override
    public SplitEnumerator<ModbusSplit, ModbusEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<ModbusSplit> enumContext,
            ModbusEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 ModbusSplitEnumerator");
        return new ModbusSplitEnumerator(enumContext, checkpoint, modbusConfig);
    }

    @Override
    public SourceReader<Row, ModbusSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 ModbusSourceReader");
        Supplier<AbstractSplitReader<Row, ModbusSplit>> splitReaderSupplier = ModbusSplitReader::new;
        return new ModbusSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<ModbusSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<ModbusEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}
