package com.etl.source.jdbc;

import com.etl.core.source.BaseSplitEnumerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.sql.*;
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

    /** 分片配置 */
    private final JdbcSplitConfig splitConfig;

    /**
     * 构造函数（首次创建，无预计算分片）
     *
     * @param context 枚举器上下文
     * @param splitConfig 分片配置
     */
    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context, JdbcSplitConfig splitConfig) {
        super(context);
        this.splitConfig = splitConfig;
        log.info("JDBC SplitEnumerator 初始化，延迟分片计算");
    }

    /**
     * 从检查点恢复的构造函数
     *
     * @param context 枚举器上下文
     * @param checkpoint 检查点
     * @param splitConfig 分片配置（用于恢复后可能的新分片计算）
     */
    public JdbcSplitEnumerator(SplitEnumeratorContext<RangeSplit> context,
                                RangeEnumCheckpoint checkpoint,
                                JdbcSplitConfig splitConfig) {
        super(context, checkpoint);
        this.splitConfig = splitConfig;
        log.info("JDBC SplitEnumerator 从检查点恢复，待处理分片数: {}", getPendingSplitCount());
    }

    @Override
    public void start() {
        log.info("JDBC SplitEnumerator 启动，开始计算分片");

        // 查询分片列范围
        Range<Long> range = getSplitColumnRange();
        log.info("分片列范围: [{}, {}]", range.getMinimum(), range.getMaximum());

        // 计算分片
        int parallelism = context.currentParallelism();
        List<RangeSplit> splits = calculateSplits(splitConfig.getSplitColumn(), range, parallelism);

        // 添加到待处理队列
        addPendingSplits(splits);

        log.info("JDBC SplitEnumerator 启动完成，分片数: {}", splits.size());
    }

    /**
     * 查询数据库获取分片列的范围
     *
     * @return 分片列的范围
     */
    private Range<Long> getSplitColumnRange() {
        String querySql = splitConfig.getDialect().buildRangeQuery(
                splitConfig.getTable(), splitConfig.getSql(), splitConfig.getSplitColumn());
        log.info("查询分片范围: {}", querySql);

        try (Connection conn = DriverManager.getConnection(
                splitConfig.getUrl(), splitConfig.getUsername(), splitConfig.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql)) {

            if (rs.next()) {
                long min = rs.getLong(1);
                long max = rs.getLong(2);
                return Range.between(min, max);
            }
            return Range.between(0L, 0L);
        } catch (SQLException e) {
            throw new RuntimeException("获取分片范围失败: " + e.getMessage(), e);
        }
    }

    /**
     * 根据范围和并行度计算所有分片
     *
     * @param splitColumn 分片键
     * @param range       数据范围
     * @param parallelism 并行度（分片数量）
     * @return 分片列表
     */
    private static List<RangeSplit> calculateSplits(String splitColumn, Range<Long> range, int parallelism) {
        List<RangeSplit> splits = new ArrayList<>();

        long start = range.getMinimum();
        long end = range.getMaximum();

        log.info("计算分片: range=[{}, {}], parallelism={}", start, end, parallelism);

        if (start > end) {
            log.warn("数据范围为空，不创建分片");
            return splits;
        }

        long totalRecords = end - start + 1;
        int actualSplitCount = (int) Math.min(parallelism, totalRecords);

        if (actualSplitCount < parallelism) {
            log.info("数据量({})小于并行度({})，实际分片数调整为 {}",
                    totalRecords, parallelism, actualSplitCount);
        }

        long splitSize = (totalRecords + actualSplitCount - 1) / actualSplitCount;

        long currentStart = start;
        for (int i = 0; i < actualSplitCount && currentStart <= end; i++) {
            long currentEnd = Math.min(currentStart + splitSize - 1, end);
            splits.add(new RangeSplit(splitColumn, currentStart, currentEnd));
            currentStart = currentEnd + 1;
        }

        log.info("共计算出 {} 个分片", splits.size());
        return splits;
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