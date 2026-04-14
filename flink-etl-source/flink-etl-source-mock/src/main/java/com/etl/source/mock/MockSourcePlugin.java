package com.etl.source.mock;

import com.etl.core.config.SourceConfig;
import com.etl.core.spi.SourcePlugin;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;

/**
 * Mock Source 插件
 * 支持固定数据和随机生成两种模式
 */
@Slf4j
@AutoService(SourcePlugin.class)
public class MockSourcePlugin implements SourcePlugin {

    @Override
    public String identifier() {
        return "mock";
    }

    @Override
    public Source<?, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode) {
        log.info("创建 Mock Source");
        return new MockSource(config, runtimeMode);
    }
}