package com.etl.core.udf.scalar;

import com.etl.core.spi.UdfPlugin;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * 雪花 ID 生成函数
 * 基于 Twitter Snowflake 算法，生成全局唯一的 64 位长整型 ID
 *
 * <p>ID 结构（64 bit）：
 * <pre>
 * | 1 bit 符号位 | 41 bit 时间戳 | 5 bit 数据中心 ID | 5 bit 工作机器 ID | 12 bit 序列号 |
 * </pre>
 *
 * <p>SQL 用法：
 * <pre>
 *   SELECT snowflake_id() AS id FROM ...
 * </pre>
 */
@AutoService(UdfPlugin.class)
public class SnowflakeIdUdf implements UdfPlugin {

    @Override
    public String identifier() {
        return "snowflake_id";
    }

    @Override
    public UserDefinedFunction createFunction() {
        return new SnowflakeIdFunction();
    }

    /**
     * 雪花 ID 生成器（Flink ScalarFunction 实现）
     */
    public static class SnowflakeIdFunction extends ScalarFunction {

        /**
         * 起始时间戳：2022-01-01 00:00:00 UTC
         */
        private static final long START_TIMESTAMP = 1640995200000L;

        // 各字段位数
        private static final long WORKER_ID_BITS = 5L;
        private static final long DATACENTER_ID_BITS = 5L;
        private static final long SEQUENCE_BITS = 12L;

        // 各字段最大值
        private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);     // 31
        private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS); // 31
        private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);      // 4095

        // 各字段左移位数
        private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;                                    // 12
        private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;                   // 17
        private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS; // 22

        private final long workerId;
        private final long datacenterId;
        private long sequence = 0L;
        private long lastTimestamp = -1L;

        /**
         * 默认构造：workerId=0，datacenterId=0
         */
        public SnowflakeIdFunction() {
            this(0L, 0L);
        }

        /**
         * 指定工作机器 ID 和数据中心 ID
         *
         * @param workerId     工作机器 ID，范围 [0, 31]
         * @param datacenterId 数据中心 ID，范围 [0, 31]
         */
        public SnowflakeIdFunction(long workerId, long datacenterId) {
            if (workerId < 0 || workerId > MAX_WORKER_ID) {
                throw new IllegalArgumentException(
                        "workerId 超出范围 [0, " + MAX_WORKER_ID + "]，实际值：" + workerId);
            }
            if (datacenterId < 0 || datacenterId > MAX_DATACENTER_ID) {
                throw new IllegalArgumentException(
                        "datacenterId 超出范围 [0, " + MAX_DATACENTER_ID + "]，实际值：" + datacenterId);
            }
            this.workerId = workerId;
            this.datacenterId = datacenterId;
        }

        /**
         * 生成下一个雪花 ID
         *
         * @return 64 位全局唯一 ID
         */
        @Override
        public boolean isDeterministic() {
            // 每行都必须重新调用，否则 Flink 优化器会对整批复用同一结果
            return false;
        }

        public long eval() {
            long timestamp = currentTimestamp();

            // 时钟回拨检测
            if (timestamp < lastTimestamp) {
                throw new RuntimeException(
                        "时钟发生回拨，拒绝生成 ID，回拨时长：" + (lastTimestamp - timestamp) + " ms");
            }

            if (timestamp == lastTimestamp) {
                // 同一毫秒内，序列号自增
                sequence = (sequence + 1) & SEQUENCE_MASK;
                if (sequence == 0) {
                    // 序列号溢出，阻塞到下一毫秒
                    timestamp = waitNextMillis(lastTimestamp);
                }
            } else {
                // 新的毫秒，序列号归零
                sequence = 0L;
            }

            lastTimestamp = timestamp;

            return ((timestamp - START_TIMESTAMP) << TIMESTAMP_SHIFT)
                    | (datacenterId << DATACENTER_ID_SHIFT)
                    | (workerId << WORKER_ID_SHIFT)
                    | sequence;
        }

        /**
         * 自旋等待直到下一毫秒
         *
         * @param lastTs 上次生成 ID 的时间戳
         * @return 下一毫秒的时间戳
         */
        private long waitNextMillis(long lastTs) {
            long ts = currentTimestamp();
            while (ts <= lastTs) {
                ts = currentTimestamp();
            }
            return ts;
        }

        private long currentTimestamp() {
            return System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return "snowflake_id()";
        }
    }
}
