package com.etl.connector.jdbc.source.enums;

import lombok.Getter;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 分片策略枚举
 * 定义支持的分片类型和对应的 JDBC 类型
 *
 * <p>设计说明：
 * <ul>
 *   <li>使用枚举而非接口，便于定义固定的分片类型和类型判断逻辑</li>
 *   <li>每个策略定义支持的 JDBC 类型集合，便于扩展新类型</li>
 *   <li>未来可通过新增枚举值支持字符串哈希分片、日期范围分片等</li>
 * </ul>
 */
public enum SplitStrategy {

    /**
     * 数值范围分片
     * 支持所有数值类型：TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL 等
     */
    NUMERIC("数值范围分片", new int[]{
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.FLOAT,
            Types.REAL,
            Types.DOUBLE,
            Types.NUMERIC,
            Types.DECIMAL
    }),

    /**
     * 字符串 Hash Mod 分片
     * 支持字符串类型：CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR
     */
    STRING_HASH("字符串 Hash Mod 分片", new int[]{
            Types.CHAR,
            Types.VARCHAR,
            Types.LONGVARCHAR,
            Types.NCHAR,
            Types.NVARCHAR
    }),

    /**
     * 日期动态粒度分片
     * 支持日期类型：DATE, TIMESTAMP
     */
    DATE_RANGE("日期动态粒度分片", new int[]{
            Types.DATE,
            Types.TIMESTAMP
    }),

    /**
     * 全表扫描（无分片）
     * 当 splitColumn 未配置时使用
     */
    FULL_TABLE_SCAN("全表扫描", new int[]{});

    private static final Map<Integer, String> JDBC_TYPE_NAMES = createJdbcTypeNames();

    @Getter
    private final String description;
    private final int[] supportedJdbcTypes;

    SplitStrategy(String description, int[] supportedJdbcTypes) {
        this.description = description;
        this.supportedJdbcTypes = supportedJdbcTypes;
    }

    private static Map<Integer, String> createJdbcTypeNames() {
        Map<Integer, String> map = new HashMap<>();
        // 数值类型
        map.put(Types.TINYINT, "TINYINT");
        map.put(Types.SMALLINT, "SMALLINT");
        map.put(Types.INTEGER, "INTEGER");
        map.put(Types.BIGINT, "BIGINT");
        map.put(Types.FLOAT, "FLOAT");
        map.put(Types.REAL, "REAL");
        map.put(Types.DOUBLE, "DOUBLE");
        map.put(Types.NUMERIC, "NUMERIC");
        map.put(Types.DECIMAL, "DECIMAL");
        // 字符串类型
        map.put(Types.CHAR, "CHAR");
        map.put(Types.VARCHAR, "VARCHAR");
        map.put(Types.LONGVARCHAR, "LONGVARCHAR");
        map.put(Types.NCHAR, "NCHAR");
        map.put(Types.NVARCHAR, "NVARCHAR");
        // 日期类型
        map.put(Types.DATE, "DATE");
        map.put(Types.TIMESTAMP, "TIMESTAMP");
        return map;
    }

    /**
     * 根据 JDBC 类型查找匹配的分片策略
     *
     * @param jdbcType JDBC 类型常量（来自 java.sql.Types）
     * @return 匹配的分片策略，如果没有匹配则返回 null
     */
    public static SplitStrategy fromJdbcType(int jdbcType) {
        for (SplitStrategy strategy : values()) {
            if (strategy.supports(jdbcType)) {
                return strategy;
            }
        }
        return null;
    }

    /**
     * 检查 JDBC 类型是否支持当前分片策略
     *
     * @param jdbcType JDBC 类型常量（来自 java.sql.Types）
     * @return true 表示支持
     */
    public boolean supports(int jdbcType) {
        for (int supportedType : supportedJdbcTypes) {
            if (supportedType == jdbcType) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取支持的 JDBC 类型名称列表（用于错误提示）
     *
     * @return 类型名称列表
     */
    public String getSupportedTypeNames() {
        if (this == FULL_TABLE_SCAN || supportedJdbcTypes.length == 0) {
            return "无";
        }
        return Arrays.stream(supportedJdbcTypes)
                .mapToObj(type -> JDBC_TYPE_NAMES.getOrDefault(type, String.valueOf(type)))
                .collect(Collectors.joining(", "));
    }

}