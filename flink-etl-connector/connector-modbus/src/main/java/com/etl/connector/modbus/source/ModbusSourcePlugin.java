package com.etl.connector.modbus.source;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.types.Row;

/**
 * Modbus Source 插件
 * 通过 Modbus TCP 读取 Holding Registers
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class ModbusSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "modbus";
    }

    @Override
    public Source<Row, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 Modbus Source");
        return new ModbusSource(config, runtimeMode);
    }
}
