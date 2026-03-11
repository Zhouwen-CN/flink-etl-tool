package com.etl.source.jdbc;

import com.etl.core.source.RangeSplit;
import com.etl.core.source.RangeSplitState;
import com.etl.core.source.base.BaseSourceReader;
import com.etl.core.source.base.BaseSplitReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Supplier;

/**
 * JDBC Source Reader
 * 继承 BaseSourceReader，自动处理线程模型和状态管理
 *
 * <p>优化后代码行数：~50 行（优化前：~160 行）
 * <p>消除的重复代码：线程管理、状态追踪、pollNext 逻辑
 * <p>直接输出 Flink Row 类型，无需额外包装
 *
 * <p>子类需要实现的方法：
 * <ul>
 *   <li>{@link #initializedState(RangeSplit)} - 初始化分片状态</li>
 *   <li>{@link #toSplitType(String, RangeSplitState)} - 状态转换为分片</li>
 *   <li>{@link #onSplitFinished(Map)} - 分片完成回调</li>
 * </ul>
 */
public class JdbcSourceReader extends BaseSourceReader<Row, Row, RangeSplit, RangeSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSourceReader.class);

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String sql;
    private final String splitColumn;
    private final Integer fetchSize;
    private final Integer queryTimeout;
    private final JdbcDialect dialect;

    /**
     * 构造函数
     *
     * @param splitReaderSupplier 分片读取器供应器
     * @param config 配置
     * @param context 读取器上下文
     * @param url JDBC URL
     * @param username 用户名
     * @param password 密码
     * @param table 表名
     * @param sql SQL 语句
     * @param splitColumn 分片列
     * @param fetchSize 获取大小
     * @param queryTimeout 查询超时
     * @param dialect 方言
     */
    public JdbcSourceReader(
            Supplier<BaseSplitReader<Row, RangeSplit>> splitReaderSupplier,
            Configuration config,
            SourceReaderContext context,
            String url, String username, String password,
            String table, String sql, String splitColumn,
            Integer fetchSize, Integer queryTimeout,
            JdbcDialect dialect) {
        super(splitReaderSupplier, new RowRecordEmitter(), config, context);
        this.url = url;
        this.username = username;
        this.password = password;
        this.table = table;
        this.sql = sql;
        this.splitColumn = splitColumn;
        this.fetchSize = fetchSize;
        this.queryTimeout = queryTimeout;
        this.dialect = dialect;
    }

    @Override
    protected void onSplitFinished(Map<String, RangeSplitState> finishedSplitIds) {
        // 分片完成时请求新分片
        logger.info("分片完成: {}", finishedSplitIds.keySet());
        context.sendSplitRequest();
    }

    @Override
    public RangeSplitState initializedState(RangeSplit split) {
        logger.debug("初始化分片状态: {}", split.splitId());
        return new RangeSplitState(split);
    }

    @Override
    protected RangeSplit toSplitType(String splitId, RangeSplitState splitState) {
        return splitState.getSplit();
    }

    /**
     * 创建 JdbcSplitReader 供应器
     *
     * @return 供应器
     */
    public static Supplier<BaseSplitReader<Row, RangeSplit>> createSplitReaderSupplier(
            String url, String username, String password,
            String table, String sql, String splitColumn,
            Integer fetchSize, Integer queryTimeout,
            JdbcDialect dialect) {
        return () -> new JdbcSplitReader(url, username, password, table, sql,
                splitColumn, fetchSize, queryTimeout, dialect);
    }
}