package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitEnumerator;
import com.etl.source.jdbc.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
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

    /**
     * 分片配置
     */
    private final JdbcSourceConfig jdbcSourceConfig;

    /**
     * 构造函数（首次创建，无预计算分片）
     *
     * @param context          枚举器上下文
     * @param jdbcSourceConfig 分片配置
     */
    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context, JdbcSourceConfig jdbcSourceConfig) {
        super(context);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 初始化");
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context          枚举器上下文
     * @param checkpoint       检查点
     * @param jdbcSourceConfig 分片配置
     */
    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context,
                               RangeEnumCheckpoint checkpoint,
                               JdbcSourceConfig jdbcSourceConfig) {
        super(context, checkpoint);
        this.jdbcSourceConfig = jdbcSourceConfig;
        log.info("JDBC SplitEnumerator 从检查点恢复，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public void start() {
        log.info("JDBC SplitEnumerator 启动，开始计算分片");

        String url = jdbcSourceConfig.getUrl();
        String username = jdbcSourceConfig.getUsername();
        String password = jdbcSourceConfig.getPassword();
        String table = jdbcSourceConfig.getTable();
        String sql = jdbcSourceConfig.getSql();
        String splitColumn = jdbcSourceConfig.getSplitColumn();

        // 查询分片列范围
        Range<Long> range = JdbcSplitHelper.getSplitColumnRange(url, username, password, table, sql, splitColumn);
        log.info("分片列范围: [{}, {}]", range.getMinimum(), range.getMaximum());

        // 使用 JdbcSplitHelper 计算分片
        int parallelism = context.currentParallelism();
        List<RangeSplit> splits = JdbcSplitHelper.calculateSplits(
                splitColumn, range.getMinimum(), range.getMaximum(), parallelism);

        addPendingSplits(splits);
        log.info("JDBC SplitEnumerator 启动完成，分片数: {}", splits.size());
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