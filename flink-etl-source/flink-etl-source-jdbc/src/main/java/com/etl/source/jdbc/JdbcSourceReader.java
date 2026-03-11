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

    public JdbcSourceReader(
            Supplier<BaseSplitReader<Row, RangeSplit>> splitReaderSupplier,
            Configuration config,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new RowRecordEmitter(), config, context);
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
}