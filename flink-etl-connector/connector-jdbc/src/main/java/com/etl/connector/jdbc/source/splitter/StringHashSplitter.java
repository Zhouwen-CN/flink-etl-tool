package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.source.RangeSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * 字符串 Hash Mod 分片器
 * 使用 hash 函数对字符串列分片
 */
@Slf4j
public class StringHashSplitter extends ChunkSplitter {

    public StringHashSplitter(JdbcSourceConfig config, int parallelism) {
        super(config, parallelism);
    }

    @Override
    public List<RangeSplit> generateSplits() {
        log.info("使用字符串 Hash Mod 分片模式，并行度: {}", parallelism);

        String column = dialect.quoteIdentifier(splitKey);
        String baseQuery = buildBaseQuery();

        int splitCount = parallelism;
        List<RangeSplit> splits = new ArrayList<>();

        for (int i = 0; i < splitCount; i++) {
            // 获取方言的 hash mod 表达式
            String hashExpression = dialect.hashModExpression(column, splitCount);

            // 生成 SQL：WHERE hash_expression = i
            String querySql = String.format("%s WHERE %s = %d", baseQuery, hashExpression, i);
            String splitId = splitKey + "_hash_" + i;

            splits.add(new RangeSplit(splitId, querySql));
        }

        log.info("生成 {} 个分片（hash mod）", splits.size());
        return splits;
    }
}