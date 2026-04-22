package com.etl.connector.cdc.mysql;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.types.Row;

/**
 * MySQL CDC Source 插件
 * 使用 Ververica CDC Connector 实现增量数据捕获
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class MySqlCdcSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "mysql-cdc";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 MySQL CDC Source");

        // 解析配置
        MySqlCdcConfig cdcConfig = MySqlCdcConfig.fromSourceConfig(config);

        // 生成 serverId（如果用户未配置）
        int serverId = cdcConfig.getServerId() != null ?
            cdcConfig.getServerId() : generateAutoServerId();

        log.info("MySQL CDC 配置: hostname={}, port={}, database={}, table={}, startupMode={}, serverId={}",
            cdcConfig.getHostname(), cdcConfig.getPort(), cdcConfig.getDatabase(),
            cdcConfig.getTable(), cdcConfig.getStartupMode(), serverId);

        // 构建 MySqlSource
        MySqlSource<Row> source = MySqlSource.<Row>builder()
            .hostname(cdcConfig.getHostname())
            .port(cdcConfig.getPort())
            .databaseList(cdcConfig.getDatabase())
            .tableList(cdcConfig.getDatabase() + "." + cdcConfig.getTable())
            .username(cdcConfig.getUsername())
            .password(cdcConfig.getPassword())
            .deserializer(new MySqlCdcDeserializer(
                cdcConfig.getHostname(),
                cdcConfig.getPort(),
                cdcConfig.getDatabase(),
                cdcConfig.getUsername(),
                cdcConfig.getPassword(),
                cdcConfig.getTable()
            ))
            .startupOptions(cdcConfig.getStartupOptions())
            .serverId(String.valueOf(serverId))
            .build();

        return source;
    }

    /**
     * 自动生成 serverId（范围：5400-15400）
     * 用于避免多任务冲突
     */
    private int generateAutoServerId() {
        return (int) (System.currentTimeMillis() % 10000) + 5400;
    }
}