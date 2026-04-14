package com.etl.source.mock.generator;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Random;

/**
 * 根据 Schema 随机生成 Row 数据
 */
public class RandomRowGenerator {

    private static final Random RANDOM = new Random();

    /**
     * 根据 Schema 随机生成一个 Row
     *
     * @param schema Schema 定义
     * @return 生成的 Row（RowKind 为 INSERT）
     */
    public static Row generateRow(EtlSchema schema) {
        int arity = schema.getFieldCount();
        Row row = Row.withPositions(RowKind.INSERT, arity);

        for (int i = 0; i < arity; i++) {
            TypeInformation<?> fieldType = schema.getFieldType(i);
            Object value = generateRandomValue(fieldType);
            row.setField(i, value);
        }

        return row;
    }

    /**
     * 根据类型信息生成随机值
     *
     * @param type Flink TypeInformation
     * @return 随机生成的值
     */
    private static Object generateRandomValue(TypeInformation<?> type) {
        // 处理基本类型
        if (type == Types.LONG) {
            return RANDOM.nextLong();
        } else if (type == Types.INT) {
            return RANDOM.nextInt();
        } else if (type == Types.STRING) {
            return generateRandomString();
        } else if (type == Types.BOOLEAN) {
            return RANDOM.nextBoolean();
        } else if (type == Types.DOUBLE) {
            return RANDOM.nextDouble();
        } else if (type == Types.FLOAT) {
            return RANDOM.nextFloat();
        } else if (type == Types.BIG_DEC) {
            return generateRandomBigDecimal();
        } else if (type == Types.SQL_TIMESTAMP) {
            return generateRandomTimestamp();
        } else if (type == Types.SQL_DATE) {
            return generateRandomDate();
        } else if (type == Types.SQL_TIME) {
            return generateRandomTime();
        } else if (type == Types.SHORT) {
            return (short) RANDOM.nextInt(Short.MAX_VALUE + 1);
        } else if (type == Types.BYTE) {
            return (byte) RANDOM.nextInt(Byte.MAX_VALUE + 1);
        } else if (type == Types.CHAR) {
            return (char) (RANDOM.nextInt(26) + 'a');
        }

        // 对于不支持的类型，返回 null
        return null;
    }

    /**
     * 生成随机字符串（长度 1-100）
     */
    private static String generateRandomString() {
        int length = RANDOM.nextInt(100) + 1;
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            char c = (char) (RANDOM.nextInt(26) + 'a');
            sb.append(c);
        }

        return sb.toString();
    }

    /**
     * 生成随机 BigDecimal（0-10000，保留 2 位小数）
     */
    private static BigDecimal generateRandomBigDecimal() {
        double value = RANDOM.nextDouble() * 10000;
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * 生成随机 Timestamp（当前时间附近）
     */
    private static Timestamp generateRandomTimestamp() {
        long currentTime = System.currentTimeMillis();
        long offset = RANDOM.nextInt(1000000);
        return new Timestamp(currentTime + offset);
    }

    /**
     * 生成随机 Date（当前日期附近）
     */
    private static java.sql.Date generateRandomDate() {
        long currentTime = System.currentTimeMillis();
        long offset = RANDOM.nextInt(1000000) * 86400000L; // 天级别偏移
        return new java.sql.Date(currentTime + offset);
    }

    /**
     * 生成随机 Time
     */
    private static java.sql.Time generateRandomTime() {
        long millis = RANDOM.nextInt(86400000);
        return new java.sql.Time(millis);
    }
}