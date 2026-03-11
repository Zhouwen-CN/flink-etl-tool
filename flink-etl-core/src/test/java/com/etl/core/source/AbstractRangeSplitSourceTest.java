package com.etl.core.source;

import org.apache.commons.lang3.Range;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AbstractRangeSplitSource 分片计算测试
 */
class AbstractRangeSplitSourceTest {

    @Test
    void testCalculateSplitsWithParallelism2() {
        // parallelism = 2, range = [1, 10]
        // 期望: [1,5], [6,10]
        TestSource source = new TestSource("id", Range.between(1L, 10L));
        List<RangeSplit> splits = source.calculateSplitsPublic(Range.between(1L, 10L), 2);

        assertEquals(2, splits.size());
        assertEquals(1L, splits.get(0).getStart());
        assertEquals(5L, splits.get(0).getEnd());
        assertEquals(6L, splits.get(1).getStart());
        assertEquals(10L, splits.get(1).getEnd());
    }

    @Test
    void testCalculateSplitsWithParallelism4() {
        // parallelism = 4, range = [1, 100]
        // 期望: [1,25], [26,50], [51,75], [76,100]
        TestSource source = new TestSource("id", Range.between(1L, 100L));
        List<RangeSplit> splits = source.calculateSplitsPublic(Range.between(1L, 100L), 4);

        assertEquals(4, splits.size());
        assertEquals(1L, splits.get(0).getStart());
        assertEquals(25L, splits.get(0).getEnd());
        assertEquals(26L, splits.get(1).getStart());
        assertEquals(50L, splits.get(1).getEnd());
        assertEquals(51L, splits.get(2).getStart());
        assertEquals(75L, splits.get(2).getEnd());
        assertEquals(76L, splits.get(3).getStart());
        assertEquals(100L, splits.get(3).getEnd());
    }

    @Test
    void testCalculateSplitsWhenDataLessThanParallelism() {
        // parallelism = 8, range = [1, 5]
        // 数据量小于并行度，期望 5 个分片
        TestSource source = new TestSource("id", Range.between(1L, 5L));
        List<RangeSplit> splits = source.calculateSplitsPublic(Range.between(1L, 5L), 8);

        assertEquals(5, splits.size());
    }

    @Test
    void testCalculateSplitsWithSingleRecord() {
        // parallelism = 4, range = [1, 1] (单条记录)
        // 期望 1 个分片
        TestSource source = new TestSource("id", Range.between(1L, 1L));
        List<RangeSplit> splits = source.calculateSplitsPublic(Range.between(1L, 1L), 4);

        assertEquals(1, splits.size());
        assertEquals(1L, splits.get(0).getStart());
        assertEquals(1L, splits.get(0).getEnd());
    }

    /**
     * 测试用的 Source 实现
     * 提供公开方法调用受保护的 calculateSplits
     */
    private static class TestSource extends AbstractRangeSplitSource<String> {
        private final Range<Long> range;

        TestSource(String splitColumn, Range<Long> range) {
            super(splitColumn);
            this.range = range;
        }

        @Override
        protected Range<Long> getSplitColumnRange() {
            return range;
        }

        /**
         * 公开调用受保护的 calculateSplits 方法
         */
        public List<RangeSplit> calculateSplitsPublic(Range<Long> range, int parallelism) {
            return calculateSplits(range, parallelism);
        }

        @Override
        public SplitEnumerator<RangeSplit, RangeEnumCheckpoint> createEnumerator(
                SplitEnumeratorContext<RangeSplit> enumContext) {
            return null;
        }

        @Override
        public SplitEnumerator<RangeSplit, RangeEnumCheckpoint> restoreEnumerator(
                SplitEnumeratorContext<RangeSplit> enumContext,
                RangeEnumCheckpoint checkpoint) {
            return null;
        }

        @Override
        public SourceReader<String, RangeSplit> createReader(SourceReaderContext readerContext) {
            return null;
        }

        @Override
        public SimpleVersionedSerializer<RangeSplit> getSplitSerializer() {
            return null;
        }

        @Override
        public SimpleVersionedSerializer<RangeEnumCheckpoint> getEnumeratorCheckpointSerializer() {
            return null;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }
    }
}