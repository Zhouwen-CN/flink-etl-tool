package com.etl.core.spi;

import com.etl.core.config.SourceConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Source;

/**
 * Source 插件接口
 * 所有数据源插件必须实现此接口
 */
public interface SourcePlugin extends Plugin {

    /**
     * 创建 Flink Source
     *
     * @param config      Source 配置
     * @param runtimeMode
     * @return Flink Source 实例
     */
    Source<?, ?, ?> createSource(SourceConfig config, RuntimeExecutionMode runtimeMode);
}