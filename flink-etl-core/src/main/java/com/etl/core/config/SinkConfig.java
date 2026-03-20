package com.etl.core.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Sink 配置类
 * 定义数据写入目标的基本配置
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class SinkConfig extends BaseConfig {

    private String type;
    private String inputTable;
}