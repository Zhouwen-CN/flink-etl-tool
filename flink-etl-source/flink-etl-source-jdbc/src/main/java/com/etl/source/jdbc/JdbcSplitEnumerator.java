package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitEnumerator;
import com.etl.source.jdbc.SplitStrategy;
import com.etl.source.jdbc.config.JdbcSourceConfig;
import com.etl.source.jdbc.utils.JdbcSplitHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC 分片枚举器
 * 继承 BaseSplitEnumerator，在 start() 中执行分片计算
 *
 * <p>分片计算延迟到 enumerator 启动时执行，而非创建时预计算。
 * 这样可以在运行时动态获取数据范围，支持更灵活的分片策略。
 */
@Slf4j
public class JdbcSplitEnumerator extends BaseSplitEnumerator<RangeSplit, RangeEnumCheckpoint> {

    private final JdbcSourceConfig jdbcSourceConfig;

    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context, JdbcSourceConfig jdbcSourceConfig) {
        super(context);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 初始化");
    }

    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context,
                               RangeEnumCheckpoint checkpoint,
                               JdbcSourceConfig jdbcSourceConfig) {
        super(context, checkpoint);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.debug("JDBC SplitEnumerator 从检查点恢复，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public void start() {
        log.info("JDBC SplitEnumerator 启动，开始计算分片");

        List<RangeSplit> splits;

        // 根据分片策略决定分片方式
        if (jdbcSourceConfig.getSplitStrategy() == SplitStrategy.FULL_TABLE_SCAN) {
            // 全表扫描模式，生成单个分片
            log.warn("使用单分片全表扫描模式");
            splits = JdbcSplitHelper.createFullTableScanSplits(
                    jdbcSourceConfig.getUrl(),
                    jdbcSourceConfig.getTable(),
                    jdbcSourceConfig.getSql());
        } else {
            // 数值范围分片模式
            splits = JdbcSplitHelper.calculateNumericSplits(
                    jdbcSourceConfig.getUrl(),
                    jdbcSourceConfig.getUsername(),
                    jdbcSourceConfig.getPassword(),
                    jdbcSourceConfig.getTable(),
                    jdbcSourceConfig.getSql(),
                    jdbcSourceConfig.getSplitColumn(),
                    context.currentParallelism());
        }

        addPendingSplits(splits);
        log.info("JDBC SplitEnumerator 启动完成，分片数: {}", splits.size());
    }

    @Override
    public RangeEnumCheckpoint snapshotState(long checkpointId) {
        List<RangeSplit> pending = new ArrayList<>(pendingSplits);
        log.info("创建检查点 {}，待处理分片数: {}", checkpointId, pending.size());
        return new RangeEnumCheckpoint(pending);
    }

    @Override
    public void close() throws IOException {
        log.info("JDBC SplitEnumerator 关闭");
    }
}