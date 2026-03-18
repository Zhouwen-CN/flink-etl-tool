package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.source.AbstractSplitSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;

import java.util.ArrayList;
import java.util.List;

/**
 * 范围分片 Source 抽象基类
 * 适用于 MySQL、PostgreSQL 等关系型数据库
 *
 */
@Slf4j
public abstract class AbstractRangeSplitSource
        extends AbstractSplitSource<RangeSplit, RangeEnumCheckpoint> {

    public AbstractRangeSplitSource(SourceConfig config) {
        super(config);
    }

    /**
     * 子类实现：获取分片列的最小值和最大值
     * 例如 MySQL: SELECT MIN(id), MAX(id) FROM table
     *
     * @return 分片列的范围
     */
    protected abstract Range<Long> getSplitColumnRange();

    /**
     * 根据范围和并行度计算所有分片
     *
     * @param splitColumn 分片键
     * @param range       数据范围
     * @param parallelism 并行度（分片数量）
     * @return 分片列表
     */
    protected List<RangeSplit> calculateSplits(String splitColumn, Range<Long> range, int parallelism) {
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
}