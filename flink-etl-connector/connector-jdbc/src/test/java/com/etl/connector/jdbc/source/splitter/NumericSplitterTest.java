package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class NumericSplitterTest {

    @Test
    void testCreateSplitter() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("")
            .table("test_table")
            .splitKey("id")
            .splitStrategy(SplitStrategy.NUMERIC)
            .dialect(new MySQLDialect())
            .build();

        NumericSplitter splitter = new NumericSplitter(config, 10);
        assertNotNull(splitter);
    }
}