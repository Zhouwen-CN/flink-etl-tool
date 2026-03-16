package com.etl.source.localfile;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.etl.core.spi.SplitStrategy;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Source;

/**
 * 本地文件 Source 插件
 * 支持通配符匹配文件、按文件分片并行读取
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class LocalFileSourcePlugin implements SourcePlugin {

    @Override
    public String getType() {
        return "localfile";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config) {
        log.info("创建 LocalFile Source");
        return new LocalFileSource(config);
    }

    @Override
    public SplitStrategy getSplitStrategy() {
        return SplitStrategy.FILE_BASED;
    }
}