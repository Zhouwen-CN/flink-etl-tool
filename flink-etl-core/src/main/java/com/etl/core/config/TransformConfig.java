package com.etl.core.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Transform 配置类
 * 定义数据转换的基本配置
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TransformConfig extends BaseConfig {

    private String type;
    private String inputTable;
    private String outputTable;
}