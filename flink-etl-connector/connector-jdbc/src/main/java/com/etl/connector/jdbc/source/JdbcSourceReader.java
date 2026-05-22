package com.etl.connector.jdbc.source;

import com.etl.core.source.AbstractSourceReader;
import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * JDBC Source Reader
 * 继承 AbstractSourceReader，自动处理线程模型和状态管理
 *
 * <p>优化后代码行数：~50 行（优化前：~160 行）
 * <p>消除的重复代码：线程管理、状态追踪、pollNext 逻辑
 * <p>直接输出 Flink Row 类型，无需额外包装
 *
 * <p>子类需要实现的方法：
 * <ul>
 *   <li>{@link #initializedState(JdbcSplit)} - 初始化分片状态</li>
 *   <li>{@link #toSplitType(String, JdbcSplitState)} - 状态转换为分片</li>
 * </ul>
 */
@Slf4j
public class JdbcSourceReader extends AbstractSourceReader<Row, Row, JdbcSplit, JdbcSplitState> {

    public JdbcSourceReader(
            Supplier<BaseSplitReader<Row, JdbcSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new JdbcRecordEmitter(), context);
    }

    @Override
    public JdbcSplitState initializedState(JdbcSplit split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new JdbcSplitState(split);
    }

    @Override
    protected JdbcSplit toSplitType(String splitId, JdbcSplitState splitState) {
        return splitState.getSplit();
    }
}