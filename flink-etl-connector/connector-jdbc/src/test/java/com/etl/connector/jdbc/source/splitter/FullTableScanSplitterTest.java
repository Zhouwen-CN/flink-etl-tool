package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FullTableScanSplitterTest {

    @Test
    void testGenerateSplits() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("")
            .table("users")
            .splitStrategy(SplitStrategy.FULL_TABLE_SCAN)
            .dialect(new MySQLDialect())
            .build();

        FullTableScanSplitter splitter = new FullTableScanSplitter(config, 10);
        List<JdbcSplit> splits = splitter.generateSplits();

        // 验证只有 1 个分片
        assertEquals(1, splits.size());

        // 验证 SQL 格式
        JdbcSplit split = splits.get(0);
        assertEquals("full_table_scan", split.getSplitId());
        assertEquals("SELECT * FROM `users`", split.getQuerySql());
    }
}