package com.etl.connector.jdbc.source.splitter;

import com.etl.connector.jdbc.dialect.MySQLDialect;
import com.etl.connector.jdbc.source.config.JdbcSourceConfig;
import com.etl.connector.jdbc.source.enums.SplitStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DateSplitterTest {

    @Test
    void testCreateSplitter() {
        JdbcSourceConfig config = JdbcSourceConfig.builder()
            .url("jdbc:mysql://localhost:3306/test")
            .username("root")
            .password("")
            .table("orders")
            .splitKey("order_date")
            .splitStrategy(SplitStrategy.DATE_RANGE)
            .dialect(new MySQLDialect())
            .build();

        DateSplitter splitter = new DateSplitter(config, 10);
        assertNotNull(splitter);
    }
}