package com.etl.core.spi;

import com.etl.core.config.TransformConfig;
import org.apache.flink.api.common.functions.MapFunction;

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
     * 创建转换函数
     *
     * @param config Transform 配置
     * @return Flink MapFunction
     */
    MapFunction<?, ?> createTransform(TransformConfig config);
}