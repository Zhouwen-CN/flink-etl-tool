package com.etl.connector.cdc.mysql;

import com.etl.connector.cdc.mysql.config.MySqlCdcConfig;
import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
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
    public Source<Row, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 MySQL CDC Source");

        // 解析配置
        MySqlCdcConfig cdcConfig = MySqlCdcConfig.fromSourceConfig(config);

        String hostname = cdcConfig.getHostname();
        int port = cdcConfig.getPort();
        String database = cdcConfig.getDatabase();
        String table = cdcConfig.getTable();
        StartupMode startupMode = cdcConfig.getStartupMode();
        Integer serverId = cdcConfig.getServerId();
        String serverTimeZone = cdcConfig.getServerTimeZone();
        String splitKey = cdcConfig.getSplitKey();

        log.info("MySQL CDC 配置: hostname={}, port={}, database={}, table={}, startupMode={}, serverId={}, serverTimeZone={}, splitKey={}",
                hostname, port, database,
                table, startupMode, serverId,
                serverTimeZone, splitKey);

        // 构建 MySqlSource
        MySqlSourceBuilder<Row> builder = MySqlSource.<Row>builder()
                .hostname(hostname)
                .port(port)
                .databaseList(database)
                .tableList(database + "." + table)
                .username(cdcConfig.getUsername())
                .password(cdcConfig.getPassword())
                .deserializer(new MySqlCdcDeserializer(cdcConfig))
                .startupOptions(cdcConfig.getStartupOptions())
                .serverId(String.valueOf(serverId));

        // 设置 serverTimeZone，一般是 Asia/Shanghai、UTC 之类，具体可以查看报错信息
        if (serverTimeZone != null) {
            builder.serverTimeZone(serverTimeZone);
        }

        // 设置 splitKey（可选，用于快照并行读取）
        if (splitKey != null && cdcConfig.getStartupMode() == StartupMode.INITIAL) {
            builder.chunkKeyColumn(splitKey);
        }

        return builder.build();
    }
}