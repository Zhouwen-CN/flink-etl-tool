package com.etl.source.jdbc;

import com.etl.core.source.PendingSplitsCheckpoint;
import com.etl.core.source.RangeSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * JDBC 分片枚举器
 * 负责分配分片给 SourceReader
 */
public class JdbcSplitEnumerator
        implements SplitEnumerator<RangeSplit, PendingSplitsCheckpoint<RangeSplit>> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSplitEnumerator.class);

    private final List<RangeSplit> splits;
    private final SplitEnumeratorContext<RangeSplit> context;
    private int currentSplitIndex = 0;

    public JdbcSplitEnumerator(List<RangeSplit> splits, SplitEnumeratorContext<RangeSplit> context) {
        this.splits = splits;
        this.context = context;
    }

    @Override
    public void start() {
        logger.info("JDBC Split Enumerator 启动，总分片数: {}", splits.size());
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (currentSplitIndex < splits.size()) {
            RangeSplit split = splits.get(currentSplitIndex++);
            logger.info("分配分片 {} 给 Reader {}", split.splitId(), subtaskId);
            context.assignSplit(split, subtaskId);
        } else {
            logger.info("所有分片已分配完毕，通知 Reader {}", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<RangeSplit> splits, int subtaskId) {
        logger.warn("Reader {} 返回 {} 个未处理的分片", subtaskId, splits.size());
        this.splits.addAll(splits);
    }

    @Override
    public PendingSplitsCheckpoint<RangeSplit> snapshotState(long checkpointId) {
        List<RangeSplit> pendingSplits = splits.subList(currentSplitIndex, splits.size());
        logger.info("创建检查点 {}，待处理分片数: {}", checkpointId, pendingSplits.size());
        return new PendingSplitsCheckpoint<>(pendingSplits);
    }

    @Override
    public void close() {
        logger.info("JDBC Split Enumerator 关闭");
    }
}