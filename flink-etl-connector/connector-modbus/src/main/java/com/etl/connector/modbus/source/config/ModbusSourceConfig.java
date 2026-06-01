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
    private final int deviceId;
    private final int address;
    private final int count;
    private final int wordSize;
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

        // 2. 校验 deviceId
        int deviceId = config.getInteger("deviceId", 1);
        Preconditions.checkArgument(deviceId >= 1 && deviceId <= 247,
                "配置项 'deviceId' 范围必须为 1-247，当前值: %s", deviceId);

        // 3. 校验 address
        Integer address = config.getInteger("address");
        Preconditions.checkNotNull(address, "配置项 'address' 不能为空");
        Preconditions.checkArgument(address >= 0,
                "配置项 'address' 必须 >= 0，当前值: %s", address);

        // 4. 校验 count
        Integer count = config.getInteger("count");
        Preconditions.checkNotNull(count, "配置项 'count' 不能为空");
        Preconditions.checkArgument(count > 0,
                "配置项 'count' 必须 > 0，当前值: %s", count);
        Preconditions.checkArgument(address + count <= 65536,
                "address(%s) + count(%s) 不能超过 65536", address, count);

        // 5. 校验 wordSize
        int wordSize = config.getInteger("wordSize", 1);
        Preconditions.checkArgument(wordSize == 1 || wordSize == 2,
                "配置项 'wordSize' 只能为 1 或 2，当前值: %s", wordSize);
        Preconditions.checkArgument(count % wordSize == 0,
                "配置项 'count'(%s) 必须能被 'wordSize'(%s) 整除", count, wordSize);

        // 6. 校验 intervalMs
        long intervalMs = config.getLong("intervalMs", 1000L);
        Preconditions.checkArgument(intervalMs > 0,
                "配置项 'intervalMs' 必须 > 0，当前值: %s", intervalMs);

        log.info("创建 ModbusSource: bounded={}, host={}:{}, deviceId={}, address={}, count={}, wordSize={}, intervalMs={}",
                bounded, ip, port, deviceId, address, count, wordSize, intervalMs);

        return ModbusSourceConfig.builder()
                .bounded(bounded)
                .ip(ip)
                .port(port)
                .deviceId(deviceId)
                .address(address)
                .count(count)
                .wordSize(wordSize)
                .intervalMs(intervalMs)
                .build();
    }
}
