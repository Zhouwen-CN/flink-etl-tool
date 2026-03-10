package com.etl.source.mysql;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.SplitStrategy;
import com.etl.source.jdbc.JdbcSource;
import com.etl.source.jdbc.dialect.MySQLDialect;
import org.apache.flink.api.connector.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL Source 插件
 * 支持主键范围分片读取 MySQL 数据
 */
public class MySQLSourcePlugin implements SourcePlugin {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSourcePlugin.class);

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        logger.info("创建 MySQL Source");
        MySQLDialect dialect = new MySQLDialect();
        return new JdbcSource(config, dialect);
    }

    @Override
    public SplitStrategy getSplitStrategy() {
        return SplitStrategy.RANGE;
    }
}