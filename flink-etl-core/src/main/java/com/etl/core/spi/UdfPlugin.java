package com.etl.core.spi;

import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * UDF 插件接口
 * 所有自定义函数需要实现此接口，并使用 @AutoService 注解
 */
public interface UdfPlugin extends Plugin {

    /**
     * 创建 UDF 实例
     * 返回 Flink 的 UserDefinedFunction 实例（ScalarFunction、TableFunction 等）
     *
     * @return UDF 实例，不能为 null
     */
    UserDefinedFunction createFunction();
}