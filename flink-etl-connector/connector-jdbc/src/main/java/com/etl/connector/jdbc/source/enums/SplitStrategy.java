package com.etl.connector.jdbc.source.enums;

import lombok.Getter;

import javax.annotation.Nullable;
import java.sql.Types;

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
@Getter
public enum SplitStrategy {

    /**
     * 全表扫描（无分片）
     * 当 splitColumn 未配置时使用
     */
    FULL_TABLE_SCAN(-1, "全表扫描", new int[]{}),

    /**
     * 数值范围分片
     * 支持所有数值类型：TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL 等
     */
    NUMERIC(0, "数值范围分片", new int[]{
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
     * 日期动态粒度分片
     * 支持日期类型：DATE, TIMESTAMP
     */
    DATE_RANGE(1, "日期动态粒度分片", new int[]{
            Types.DATE,
            Types.TIMESTAMP
    }),

    /**
     * 字符串 Hash Mod 分片
     * 支持字符串类型：CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR
     */
    STRING_HASH(2, "字符串 Hash Mod 分片", new int[]{
            Types.CHAR,
            Types.VARCHAR,
            Types.LONGVARCHAR,
            Types.NCHAR,
            Types.NVARCHAR
    });

    private final String description;
    private final int[] supportedJdbcTypes;
    private final int order;

    SplitStrategy(int order, String description, int[] supportedJdbcTypes) {
        this.order = order;
        this.description = description;
        this.supportedJdbcTypes = supportedJdbcTypes;
    }

    /**
     * 根据 JDBC 类型查找匹配的分片策略
     *
     * @param jdbcType JDBC 类型常量（来自 java.sql.Types）
     * @return 匹配的分片策略，如果没有匹配则返回 null
     */
    public static @Nullable SplitStrategy fromJdbcType(int jdbcType) {
        for (SplitStrategy strategy : values()) {
            for (int supportedJdbcType : strategy.getSupportedJdbcTypes()) {
                if (supportedJdbcType == jdbcType) {
                    return strategy;
                }
            }
        }
        return null;
    }
}