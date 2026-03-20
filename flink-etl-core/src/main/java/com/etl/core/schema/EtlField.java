package com.etl.core.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * ETL 字段定义
 * 包含字段名和类型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EtlField implements Serializable {

    /**
     * 字段名
     */
    private String name;

    /**
     * 字段类型
     */
    private EtlFieldType type;
}