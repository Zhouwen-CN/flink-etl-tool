package com.etl.source.jdbc;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;

/**
 * 通用 JDBC Source 插件
 * 支持所有 JDBC 数据库
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class JdbcSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "jdbc";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 JDBC Source");
        return new JdbcSource(config);
    }
}