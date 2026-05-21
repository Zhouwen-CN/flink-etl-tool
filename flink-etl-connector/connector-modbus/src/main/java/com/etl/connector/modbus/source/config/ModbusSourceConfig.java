package com.etl.connector.modbus.source.config;

import com.etl.core.config.SourceConfig;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Modbus Source 配置封装类
 */
@Data
@Builder
@Slf4j
public class ModbusSourceConfig implements Serializable {

    private final boolean bounded;
    private final String ip;
    private final int port;
    private final int slaveId;
    private final int startAddress;
    private final int quantity;
    private final long intervalMs;

    public static ModbusSourceConfig fromSourceConfig(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        boolean bounded = runtimeMode == RuntimeExecutionMode.BATCH;

        // 1. 校验并拆分 host
        String host = config.getString("host");
        Preconditions.checkArgument(host != null && !host.isEmpty(), "配置项 'host' 不能为空");

        String[] parts = host.split(":");
        Preconditions.checkArgument(parts.length == 2, "配置项 'host' 格式必须为 'ip:port'，当前值: %s", host);

        String ip = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 'host' 中的端口必须为数字，当前值: " + parts[1]);
        }
        Preconditions.checkArgument(port >= 1 && port <= 65535,
                "配置项 'host' 中的端口范围必须为 1-65535，当前值: %s", port);

        // 2. 校验 slaveId
        int slaveId = config.getInteger("slaveId", 1);
        Preconditions.checkArgument(slaveId >= 1 && slaveId <= 247,
                "配置项 'slaveId' 范围必须为 1-247，当前值: %s", slaveId);

        // 3. 校验 startAddress
        Integer startAddress = config.getInteger("startAddress");
        Preconditions.checkNotNull(startAddress, "配置项 'startAddress' 不能为空");
        Preconditions.checkArgument(startAddress >= 0,
                "配置项 'startAddress' 必须 >= 0，当前值: %s", startAddress);

        // 4. 校验 quantity
        Integer quantity = config.getInteger("quantity");
        Preconditions.checkNotNull(quantity, "配置项 'quantity' 不能为空");
        Preconditions.checkArgument(quantity > 0,
                "配置项 'quantity' 必须 > 0，当前值: %s", quantity);
        Preconditions.checkArgument(startAddress + quantity <= 65536,
                "startAddress(%s) + quantity(%s) 不能超过 65536", startAddress, quantity);

        // 5. 校验 intervalMs
        long intervalMs = config.getLong("intervalMs", 1000L);
        Preconditions.checkArgument(intervalMs > 0,
                "配置项 'intervalMs' 必须 > 0，当前值: %s", intervalMs);

        log.info("创建 ModbusSource: bounded={}, host={}:{}, slaveId={}, startAddress={}, quantity={}, intervalMs={}",
                bounded, ip, port, slaveId, startAddress, quantity, intervalMs);

        return ModbusSourceConfig.builder()
                .bounded(bounded)
                .ip(ip)
                .port(port)
                .slaveId(slaveId)
                .startAddress(startAddress)
                .quantity(quantity)
                .intervalMs(intervalMs)
                .build();
    }
}
