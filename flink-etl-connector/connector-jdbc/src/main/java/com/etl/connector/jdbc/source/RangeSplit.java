package com.etl.connector.jdbc.source;

import com.etl.core.source.BaseSourceSplit;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.Getter;


/**
 * JDBC 分片
 * 存储分片 ID 和该分片的查询 SQL
 *
 * <p>设计说明：
 * <ul>
 *   <li>直接存储查询 SQL，职责清晰（Enumerator 负责生成 SQL，Reader 只执行）</li>
 *   <li>支持任意复杂的分片条件，便于扩展新分片类型</li>
 *   <li>分片 ID 用于状态管理和调试</li>
 * </ul>
 */
@Getter
public class RangeSplit implements BaseSourceSplit {

    private static final long serialVersionUID = DefaultSplitSerializer.VERSION;

    /** 分片 ID，用于状态管理和调试 */
    private final String splitId;

    /** 该分片的查询 SQL */
    private final String querySql;

    private final String url;
    private final String username;
    private final String password;
    private final Integer batchSize;
    private final Integer queryTimeout;

    /**
     * 构造函数
     *
     * @param splitId  分片 ID
     * @param querySql 该分片的查询 SQL
     */
    public RangeSplit(String splitId,
                      String querySql,
                      String url,
                      String username,
                      String password,
                      Integer batchSize,
                      Integer queryTimeout
    ) {
        this.splitId = splitId;
        this.querySql = querySql;
        this.url = url;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.queryTimeout = queryTimeout;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}