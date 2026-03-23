package com.etl.source.http;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;

/**
 * HTTP Source 插件
 * 支持从 REST API 获取 JSON 数据
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class HttpSourcePlugin implements SourcePlugin {

    @Override
    public String getType() {
        return "http";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        log.info("创建 HTTP Source");
        return new HttpSource(config);
    }
}