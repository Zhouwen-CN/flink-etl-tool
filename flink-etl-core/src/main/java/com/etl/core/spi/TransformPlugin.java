package com.etl.core.spi;

import com.etl.core.config.TransformConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Transform 插件接口
 * 所有数据转换插件必须实现此接口
 */
public interface TransformPlugin {

    /**
     * 获取插件类型标识
     *
     * @return 插件类型标识
     */
    String getType();

    /**
     * 执行转换
     *
     * @param config 转换配置
     * @param stEnv  Table 环境
     * @return 转换后的表
     */
    Table transform(TransformConfig config, StreamTableEnvironment stEnv);
}