package com.etl.core.source;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * 范围分片
 * 表示一个数据范围，如 [1, 10000]
 */
public class RangeSplit implements SourceSplit {
    private final String splitId;
    private final String columnName;
    private final long start;
    private final long end;

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

    public String getColumnName() {
        return columnName;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "RangeSplit{" +
                "splitId='" + splitId + '\'' +
                ", columnName='" + columnName + '\'' +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}