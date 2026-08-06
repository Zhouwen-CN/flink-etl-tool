package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.core.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 数值分片器
 * 查询 MIN/MAX 数值范围，按步长分片（使用开区间边界）
 */
@Slf4j
public class NumericSplitter extends ChunkSplitter {

    public NumericSplitter(JdbcSourceConfig config, int parallelism) {
        super(config, parallelism);
    }

    @Override
    public List<JdbcSplit> generateSplits() {
        log.info("使用数值分片模式，并行度: {}", parallelism);

        // 1. 查询 MIN/MAX 数值范围
        Pair<Long, Long> range = this.queryNumericMinMax();

        // 空表检查
        if (range.getLeft() == null) {
            log.warn("表为空，不创建分片");
            return Collections.emptyList();
        }

        long min = range.getLeft();
        long max = range.getRight();

        log.info("数值范围: [{}, {}]", min, max);

        // 2. 计算分片数量和步长
        if (min > max) {
            log.warn("数据范围为空，不创建分片");
            return Collections.emptyList();
        }

        long totalRecords = max - min + 1;
        int actualSplitCount = (int) Math.min(parallelism, totalRecords);

        if (actualSplitCount < parallelism) {
            log.info("数据量({})小于并行度({})，实际分片数调整为 {}",
                totalRecords, parallelism, actualSplitCount);
        }

        long splitSize = (totalRecords + actualSplitCount - 1) / actualSplitCount;

        // 3. 生成分片（使用开区间）
        List<JdbcSplit> splits = new ArrayList<>();
        String column = dialect.quoteIdentifier(splitKey);
        String baseQuery = buildBaseQuery();

        long currentStart = min;
        for (int i = 0; i < actualSplitCount && currentStart <= max; i++) {
            long currentEnd = Math.min(currentStart + splitSize - 1, max);

            // 开区间 SQL：>= start AND < end+1
            String querySql = String.format("%s WHERE %s >= %d AND %s < %d",
                baseQuery, column, currentStart, column, currentEnd + 1);

            String splitId = splitKey + "_" + currentStart + "_" + currentEnd;
            splits.add(new JdbcSplit(
                    splitId,
                    querySql,
                    url,
                    username,
                    password,
                    batchSize,
                    queryTimeout
                    )
            );

            log.debug("分片 {}: {} 到 {}", i, currentStart, currentEnd);

            currentStart = currentEnd + 1;
        }

        log.info("生成 {} 个分片（数值开区间）", splits.size());
        return splits;
    }

    /**
     * 查询数值列的 MIN/MAX 范围
     *
     * @return Pair<min, max>，如果为空表则返回 Pair.of(null, null)
     */
    public Pair<Long, Long> queryNumericMinMax() {

        String column = dialect.quoteIdentifier(splitKey);
        String rangeQuery;

        if (table != null) {
            String quotedTable = dialect.quoteIdentifier(table);
            rangeQuery = String.format("SELECT MIN(%s), MAX(%s) FROM %s",
                    column, column, quotedTable);
        } else {
            rangeQuery = String.format("SELECT MIN(%s), MAX(%s) FROM (%s) AS t",
                    column, column, sql);
        }

        return JdbcUtil.query(url, username, password, null, rangeQuery, (conn, stmt, rs) -> {
            if (rs.next()) {
                // todo: 超过 Long.MAX_VALUE 的 BigDecimal 被截断，分片范围错误
                long min = rs.getLong(1);
                long max = rs.getLong(2);
                return Pair.of(min, max);
            }

            return Pair.of(null, null);

        });
    }
}