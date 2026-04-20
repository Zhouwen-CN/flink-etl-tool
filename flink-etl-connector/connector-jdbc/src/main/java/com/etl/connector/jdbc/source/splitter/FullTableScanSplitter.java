package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.source.RangeSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * 全表扫描分片器
 * 无分片键或类型不支持时，生成单个全表扫描分片
 */
@Slf4j
public class FullTableScanSplitter extends ChunkSplitter {

    public FullTableScanSplitter(JdbcSourceConfig config, int parallelism) {
        super(config, parallelism);
    }

    @Override
    public List<RangeSplit> generateSplits() {
        log.info("使用全表扫描分片模式");

        String baseQuery = buildBaseQuery();
        RangeSplit split = new RangeSplit("full_table_scan", baseQuery);

        log.info("生成 1 个分片（全表扫描）");
        return Collections.singletonList(split);
    }
}