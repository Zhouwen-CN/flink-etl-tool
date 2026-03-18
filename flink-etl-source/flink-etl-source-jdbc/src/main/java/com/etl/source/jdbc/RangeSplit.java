package com.etl.source.jdbc;

import com.etl.core.source.BaseSourceSplit;
import lombok.Getter;

import java.io.Serializable;

/**
 * 范围分片
 * 表示一个数据范围，如 [1, 10000]
 *
 * <p>实现了 {@link BaseSourceSplit} 接口，支持序列化和状态管理
 */
@Getter
public class RangeSplit implements BaseSourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final String columnName;
    private final long start;
    private final long end;

    /**
     * 构造函数
     *
     * @param columnName 分片列名
     * @param start 起始值（包含）
     * @param end 结束值（包含）
     */
    public RangeSplit(String columnName, long start, long end) {
        this.columnName = columnName;
        this.start = start;
        this.end = end;
        this.splitId = columnName + "_" + start + "_" + end;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    /**
     * 获取分片包含的记录数量
     *
     * @return 记录数量
     */
    public long getRecordCount() {
        return end - start + 1;
    }

    @Override
    public String toString() {
        return "RangeSplit{" +
                "splitId='" + splitId + '\'' +
                ", columnName='" + columnName + '\'' +
                ", start=" + start +
                ", end=" + end +
                ", recordCount=" + getRecordCount() +
                '}';
    }
}