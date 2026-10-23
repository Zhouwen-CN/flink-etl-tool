package com.etl.connector.modbus.source.config;

import com.etl.core.config.SourceConfig;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
    private final int timeoutMs;
    /**
     * 是否将寄存器值按 IEEE 754 32 位浮点数解析，为 true 时要求 wordSize=2
     */
    private final boolean floatValue;
    /**
     * 字序：ABCD（高字在前，默认）或 CDAB（低字在前），wordSize=2 时生效
     */
    private final WordOrder wordOrder;
    /**
     * 间隔读取的寄存器起始地址列表（0-based）。
     * <p>
     * 非空时启用间隔模式：逐个点位发起独立读请求，每个点位读取 {@code wordSize} 个寄存器，
     * 与 {@link #address}/{@link #count} 互斥。
     */
    private final List<Integer> points;

    public static ModbusSourceConfig fromSourceConfig(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        boolean bounded = runtimeMode == RuntimeExecutionMode.BATCH;

        // 1. 校验并拆分 host
        String host = config.get("host", String.class);
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
        int deviceId = config.get("deviceId", Integer.class, 1);
        Preconditions.checkArgument(deviceId >= 1 && deviceId <= 247,
                "配置项 'deviceId' 范围必须为 1-247，当前值: %s", deviceId);

        // 3. 校验 wordSize（points 模式下的点位范围校验依赖该值，需提前解析）
        int wordSize = config.get("wordSize", Integer.class, 1);
        Preconditions.checkArgument(wordSize == 1 || wordSize == 2,
                "配置项 'wordSize' 只能为 1 或 2，当前值: %s", wordSize);

        // 4. 解析读取模式：points（间隔）与 address+count（连续）互斥
        List<Integer> points = parsePoints(config, wordSize);
        int address = -1;
        int count = -1;
        if (points != null) {
            // 间隔模式：address/count 不参与读取，但仍做格式校验以便给出明确报错
            Preconditions.checkArgument(!config.contains("address") && !config.contains("count"),
                    "配置项 'points' 与 'address'/'count' 互斥，请只使用其中一种方式指定读取范围");
        } else {
            // 5. 校验 address
            Integer addressValue = config.get("address", Integer.class);
            Preconditions.checkNotNull(addressValue, "配置项 'address' 不能为空");
            Preconditions.checkArgument(addressValue >= 0,
                    "配置项 'address' 必须 >= 0，当前值: %s", addressValue);
            address = addressValue;

            // 6. 校验 count
            Integer countValue = config.get("count", Integer.class);
            Preconditions.checkNotNull(countValue, "配置项 'count' 不能为空");
            Preconditions.checkArgument(countValue > 0,
                    "配置项 'count' 必须 > 0，当前值: %s", countValue);
            Preconditions.checkArgument(address + countValue <= 65536,
                    "address(%s) + count(%s) 不能超过 65536", address, countValue);
            Preconditions.checkArgument(countValue % wordSize == 0,
                    "配置项 'count'(%s) 必须能被 'wordSize'(%s) 整除", countValue, wordSize);
            count = countValue;
        }

        // 7. 校验 intervalMs
        long intervalMs = config.get("intervalMs", Long.class, 1000L);
        Preconditions.checkArgument(intervalMs > 0,
                "配置项 'intervalMs' 必须 > 0，当前值: %s", intervalMs);

        // 8. 校验 timeoutMs（连接与读取共用，默认 3000ms）
        int timeoutMs = config.get("timeoutMs", Integer.class, 3000);
        Preconditions.checkArgument(timeoutMs > 0,
                "配置项 'timeoutMs' 必须 > 0，当前值: %s", timeoutMs);

        // 9. 校验 floatValue（默认 false，为 true 时按 IEEE 754 浮点解析）
        boolean floatValue = config.get("floatValue", Boolean.class, false);
        if (floatValue) {
            Preconditions.checkArgument(wordSize == 2,
                    "配置项 'floatValue' 为 true 时，'wordSize' 必须为 2（IEEE 754 32位浮点数），当前值: %s", wordSize);
        }

        // 10. 校验 wordOrder（默认 ABCD，wordSize=2 时生效）
        String wordOrderValue = config.get("wordOrder", String.class, WordOrder.ABCD.name());
        WordOrder wordOrder = WordOrder.parse(wordOrderValue);
        Preconditions.checkArgument(wordOrder != null,
                "配置项 'wordOrder' 只能为 ABCD 或 CDAB，当前值: %s", wordOrderValue);

        log.info("创建 ModbusSource: bounded={}, host={}:{}, deviceId={}, mode={}, address={}, count={}, "
                        + "points={}, wordSize={}, intervalMs={}, timeoutMs={}, floatValue={}, wordOrder={}",
                bounded, ip, port, deviceId, points != null ? "POINTS" : "RANGE", address, count,
                points, wordSize, intervalMs, timeoutMs, floatValue, wordOrder);

        return ModbusSourceConfig.builder()
                .bounded(bounded)
                .ip(ip)
                .port(port)
                .deviceId(deviceId)
                .address(address)
                .count(count)
                .wordSize(wordSize)
                .intervalMs(intervalMs)
                .timeoutMs(timeoutMs)
                .floatValue(floatValue)
                .wordOrder(wordOrder)
                .points(points)
                .build();
    }

    /**
     * 解析 points 配置项
     *
     * <p>支持整数数组写法：{@code "points": [0, 8, 16, 58]}；
     * 元素也允许写成数字字符串（如 {@code "8"}），由 {@code getList} 统一做类型转换。
     *
     * @param config   来源配置
     * @param wordSize 每个点位占用的寄存器数，用于校验点位不越界
     * @return 点位地址列表；未配置 points 时返回 null（连续模式）
     */
    private static List<Integer> parsePoints(SourceConfig config, int wordSize) {
        List<Integer> rawPoints = config.getList("points", Integer.class);
        if (rawPoints == null) {
            return null;
        }
        Preconditions.checkArgument(!rawPoints.isEmpty(),
                "配置项 'points' 不能为空数组，请改用 'address' + 'count' 进行连续读取");

        Set<Integer> distinct = new LinkedHashSet<>();
        List<Integer> points = new ArrayList<>(rawPoints.size());
        for (Integer point : rawPoints) {
            Preconditions.checkArgument(point >= 0,
                    "配置项 'points' 的元素必须 >= 0，当前值: %s", point);
            Preconditions.checkArgument(point + wordSize <= 65536,
                    "配置项 'points' 的元素 %s 加上 wordSize(%s) 不能超过 65536", point, wordSize);
            if (!distinct.add(point)) {
                log.warn("配置项 'points' 中存在重复点位 {}，已自动去重", point);
                continue;
            }
            points.add(point);
        }
        return points;
    }

    /**
     * 是否为间隔读取模式
     */
    public boolean isPointsMode() {
        return points != null && !points.isEmpty();
    }
}
