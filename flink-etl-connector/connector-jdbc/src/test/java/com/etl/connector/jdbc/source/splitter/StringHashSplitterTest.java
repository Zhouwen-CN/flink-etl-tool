package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.source.JdbcSplit;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StringHashSplitterTest {

    @Test
    void testGenerateSplits() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("")
            .table("users")
            .splitKey("username")
            .splitStrategy(SplitStrategy.STRING_HASH)
            .dialect(new MySQLDialect())
            .build();

        StringHashSplitter splitter = new StringHashSplitter(config, 10);
        List<JdbcSplit> splits = splitter.generateSplits();

        // 验证分片数量 = 并行度
        assertEquals(10, splits.size());

        // 验证 SQL 格式
        for (int i = 0; i < splits.size(); i++) {
            JdbcSplit split = splits.get(i);
            assertTrue(split.getQuerySql().contains("CAST(MD5(`username`) AS UNSIGNED) % 10 = " + i));
            assertTrue(split.getQuerySql().startsWith("SELECT * FROM `users` WHERE"));
        }
    }
}