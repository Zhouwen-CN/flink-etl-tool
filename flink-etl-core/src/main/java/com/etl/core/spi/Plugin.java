package com.etl.core.spi;

public interface Plugin {

    /**
     * 获取插件类型标识
     *
     * @return 插件类型标识
     */
    String identifier();
}
