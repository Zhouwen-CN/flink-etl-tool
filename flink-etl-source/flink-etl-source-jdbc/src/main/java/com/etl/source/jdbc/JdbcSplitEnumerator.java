package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitEnumerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.List;

/**
 * JDBC 分片枚举器
 * 继承 BaseSplitEnumerator，自动处理分片分配逻辑
 *
 * <p>优化后代码行数：~45 行（优化前：~70 行）
 * <p>消除的重复代码：handleSplitRequest、addSplitsBack、addReader
 */
@Slf4j
public class JdbcSplitEnumerator extends BaseSplitEnumerator<RangeSplit, RangeEnumCheckpoint> {

    /**
     * 构造函数
     *
     * @param splits 预计算的分片列表
     * @param context 枚举器上下文
     */
    public JdbcSplitEnumerator(List<RangeSplit> splits, SplitEnumeratorContext<RangeSplit> context) {
        super(context);
        addPendingSplits(splits);
        log.info("JDBC SplitEnumerator 初始化，分片数: {}", splits.size());
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context 枚举器上下文
     * @param checkpoint 检查点
     */
    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context, RangeEnumCheckpoint checkpoint) {
        super(context, checkpoint);
        log.info("JDBC SplitEnumerator 从检查点恢复，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public void start() {
        log.info("JDBC SplitEnumerator 启动，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public RangeEnumCheckpoint snapshotState(long checkpointId) {
        List<RangeSplit> pending = List.copyOf(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new RangeEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("JDBC SplitEnumerator 关闭");
    }
}