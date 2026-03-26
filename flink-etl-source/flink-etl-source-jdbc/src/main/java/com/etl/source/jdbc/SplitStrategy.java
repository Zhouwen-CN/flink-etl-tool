package com.etl.source.jdbc;

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
     * 全表扫描（无分片）
     * 当 splitColumn 未配置时使用
     */
    FULL_TABLE_SCAN("全表扫描", new int[]{});

    private final String description;
    private final int[] supportedJdbcTypes;

    SplitStrategy(String description, int[] supportedJdbcTypes) {
        this.description = description;
        this.supportedJdbcTypes = supportedJdbcTypes;
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
        if (this == FULL_TABLE_SCAN) {
            return "无";
        }
        return "TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, NUMERIC";
    }

    public String getDescription() {
        return description;
    }
}