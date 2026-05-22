package com.etl.connector.jdbc.source;

import com.etl.core.source.BaseEnumCheckpoint;

import java.util.Collection;

/**
 * 范围分片枚举器检查点
 * 用于保存 JdbcSplitEnumerator 的状态
 */
public class JdbcEnumCheckpoint extends BaseEnumCheckpoint<JdbcSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param pendingSplits 待处理的分片集合
     */
    public JdbcEnumCheckpoint(Collection<JdbcSplit> pendingSplits) {
        super(pendingSplits);
    }
}