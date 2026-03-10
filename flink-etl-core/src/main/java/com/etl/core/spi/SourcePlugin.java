package com.etl.core.spi;

import com.etl.core.config.SourceConfig;
import org.apache.flink.api.connector.source.Source;

/**
 * Source 插件接口
 * 所有数据源插件必须实现此接口
 */
public interface SourcePlugin {

    /**
     * 获取插件类型标识
     * 例如：mysql、file、kafka
     *
     * @return 插件类型标识
     */
    String getType();

    /**
     * 创建 Flink Source
     *
     * @param config Source 配置
     * @return Flink Source 实例
     */
    Source<?, ?, ?> createSource(SourceConfig config);

    /**
     * 获取分片策略描述
     *
     * @return 该数据源支持的分片方式
     */
    SplitStrategy getSplitStrategy();
}