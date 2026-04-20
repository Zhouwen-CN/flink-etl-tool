package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.JdbcDialect;
import com.etl.connector.jdbc.source.RangeSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.utils.JdbcSplitHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 日期动态粒度分片器
 * 根据数据天数和并行度动态决定每个分片天数
 */
@Slf4j
public class DateSplitter extends ChunkSplitter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    public DateSplitter(JdbcSourceConfig config, int parallelism) {
        super(config, parallelism);
    }

    @Override
    public List<RangeSplit> generateSplits() {
        log.info("使用日期动态粒度分片模式，并行度: {}", parallelism);

        // 1. 查询 MIN/MAX 日期范围
        Pair<Date, Date> range = JdbcSplitHelper.queryDateMinMax(
            config.getDialect(),
            config.getUrl(),
            config.getUsername(),
            config.getPassword(),
            config.getTable(),
            config.getSql(),
            config.getSplitKey()
        );

        // 空表检查
        if (range.getLeft() == null) {
            log.warn("表为空，不创建分片");
            return Collections.emptyList();
        }

        LocalDate minDate = range.getLeft().toLocalDate();
        LocalDate maxDate = range.getRight().toLocalDate();

        log.info("日期范围: {} 到 {}", minDate, maxDate);

        // 2. 计算总天数
        long totalDays = ChronoUnit.DAYS.between(minDate, maxDate) + 1;
        log.info("总天数: {}", totalDays);

        // 3. 计算每个分片包含的天数（动态粒度）
        int splitCount = (int) Math.min(parallelism, totalDays);
        long daysPerSplit = (totalDays + splitCount - 1) / splitCount;

        if (totalDays < parallelism) {
            log.info("天数({})小于并行度({})，实际分片数调整为 {}",
                totalDays, parallelism, splitCount);
        }

        log.info("每个分片包含 {} 天", daysPerSplit);

        // 4. 生成分片（使用开区间）
        List<RangeSplit> splits = new ArrayList<>();
        LocalDate currentStart = minDate;
        JdbcDialect dialect = config.getDialect();
        String column = dialect.quoteIdentifier(config.getSplitKey());
        String baseQuery = buildBaseQuery();

        for (int i = 0; i < splitCount && !currentStart.isAfter(maxDate); i++) {
            LocalDate currentEnd = currentStart.plusDays(daysPerSplit - 1);
            if (currentEnd.isAfter(maxDate)) {
                currentEnd = maxDate;
            }

            // 开区间边界：>= startDate AND < endDate+1
            String startDateStr = formatDate(currentStart);
            String endDateStr = formatDate(currentEnd.plusDays(1)); // 开区间边界

            String querySql = dialect.buildDateRangeQuery(baseQuery, column, startDateStr, endDateStr);
            String splitId = config.getSplitKey() + "_date_" + startDateStr + "_" + endDateStr;

            splits.add(new RangeSplit(splitId, querySql));

            log.debug("分片 {}: {} 到 {}", i, startDateStr, endDateStr);

            currentStart = currentEnd.plusDays(1);
        }

        log.info("生成 {} 个分片（日期动态粒度）", splits.size());
        return splits;
    }

    /**
     * 格式化日期为 yyyy-MM-dd 格式
     */
    private String formatDate(LocalDate date) {
        return date.format(DATE_FORMATTER);
    }
}