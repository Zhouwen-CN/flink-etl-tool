package com.etl.source.jdbc;

import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * JDBC 记录包装类
 * 包装 Row 数据和分片 ID，用于追踪数据来源
 */
public class JdbcRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Row row;
    private final String splitId;

    /**
     * 构造函数
     *
     * @param row 数据行
     * @param splitId 分片 ID
     */
    public JdbcRecord(Row row, String splitId) {
        this.row = row;
        this.splitId = splitId;
    }

    /**
     * 获取数据行
     *
     * @return 数据行
     */
    public Row getRow() {
        return row;
    }

    /**
     * 获取分片 ID
     *
     * @return 分片 ID
     */
    public String getSplitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "JdbcRecord{" +
                "row=" + row +
                ", splitId='" + splitId + '\'' +
                '}';
    }
}