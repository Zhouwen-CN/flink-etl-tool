package com.etl.core.source;

import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 范围分片 Source 抽象基类
 * 适用于 MySQL、PostgreSQL 等关系型数据库
 *
 * @param <T> 输出记录类型
 */
public abstract class AbstractRangeSplitSource<T> extends AbstractSplitSource<T, RangeSplit> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRangeSplitSource.class);

    protected final String splitColumn;
    protected final int splitSize;

    public AbstractRangeSplitSource(String splitColumn, int splitSize) {
        this.splitColumn = splitColumn;
        this.splitSize = splitSize;
    }

    /**
     * 子类实现：获取分片列的最小值和最大值
     * 例如 MySQL: SELECT MIN(id), MAX(id) FROM table
     *
     * @return 分片列的范围
     */
    protected abstract Range<Long> getSplitColumnRange();

    /**
     * 根据最小值、最大值和分片大小，计算所有分片
     * 框架自动实现，子类无需重写
     *
     * @return 分片列表
     */
    protected List<RangeSplit> calculateSplits() {
        Range<Long> range = getSplitColumnRange();
        List<RangeSplit> splits = new ArrayList<>();

        long start = range.getMinimum();
        long end = range.getMaximum();

        logger.info("计算分片: splitColumn={}, range=[{}, {}], splitSize={}",
                splitColumn, start, end, splitSize);

        while (start <= end) {
            long splitEnd = Math.min(start + splitSize - 1, end);
            splits.add(new RangeSplit(splitColumn, start, splitEnd));
            start = splitEnd + 1;
        }

        logger.info("共计算出 {} 个分片", splits.size());
        return splits;
    }
}