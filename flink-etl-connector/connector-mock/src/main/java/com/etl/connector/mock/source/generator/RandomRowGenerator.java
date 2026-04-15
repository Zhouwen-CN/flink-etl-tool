package com.etl.connector.mock.source.generator;

import com.etl.core.schema.EtlSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 根据 Schema 随机生成 Row 数据
 */
@Slf4j
public class RandomRowGenerator {

    private static final Random RANDOM = new Random();

    /**
     * 批量生成 Row 数据
     *
     * @param schema Schema 定义
     * @param numRows 生成的行数
     * @return 生成的 Row 列表
     */
    public static List<Row> generateRows(EtlSchema schema, int numRows) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            Row row = generateRow(schema);
            rows.add(row);
        }
        log.info("随机生成 {} 行数据", numRows);
        return rows;
    }

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
     * 仅支持简单类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP
     *
     * @param type Flink TypeInformation
     * @return 随机生成的值，不支持的类型返回 null
     */
    private static Object generateRandomValue(TypeInformation<?> type) {
        if (type.equals(Types.LONG)) {
            return RANDOM.nextLong();
        } else if (type.equals(Types.INT)) {
            return RANDOM.nextInt();
        } else if (type.equals(Types.STRING)) {
            return generateRandomString();
        } else if (type.equals(Types.BOOLEAN)) {
            return RANDOM.nextBoolean();
        } else if (type.equals(Types.DOUBLE)) {
            return RANDOM.nextDouble();
        } else if (type.equals(Types.BIG_DEC)) {
            return generateRandomBigDecimal();
        } else if (type.equals(Types.LOCAL_DATE_TIME)) {
            return generateRandomTimestamp();
        }

        // 不支持的类型返回 null
        return null;
    }

    /**
     * 生成随机字符串（长度 1-10）
     */
    private static String generateRandomString() {
        int length = RANDOM.nextInt(10) + 1;
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
    private static LocalDateTime generateRandomTimestamp() {
        long currentTime = System.currentTimeMillis();
        long offset = RANDOM.nextInt(1000000);
        return LocalDateTime.ofInstant(
            Instant.ofEpochMilli(currentTime + offset), ZoneId.systemDefault());
    }
}