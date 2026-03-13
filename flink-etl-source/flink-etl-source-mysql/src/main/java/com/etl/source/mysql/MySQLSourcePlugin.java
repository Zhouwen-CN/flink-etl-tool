package com.etl.source.mysql;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.SplitStrategy;
import com.etl.source.jdbc.JdbcSource;
import com.etl.source.jdbc.dialect.MySQLDialect;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;

/**
 * MySQL Source 插件
 * 支持主键范围分片读取 MySQL 数据
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class MySQLSourcePlugin implements SourcePlugin {

    @Override
    public String getType() {
        return "mysql";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        log.info("创建 MySQL Source");
        MySQLDialect dialect = new MySQLDialect();
        return new JdbcSource(config, dialect);
    }

    @Override
    public SplitStrategy getSplitStrategy() {
        return SplitStrategy.RANGE;
    }
}