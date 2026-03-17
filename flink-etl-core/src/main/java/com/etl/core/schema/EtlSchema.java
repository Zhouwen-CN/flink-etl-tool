package com.etl.core.schema;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ETL Schema 容器
 * 包含表名和字段列表定义
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EtlSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 表名，用于注册 Flink Table
     */
    private String tableName;

    /**
     * 字段列表
     */
    private List<EtlField> fields;

    /**
     * 按索引获取字段
     */
    public EtlField getField(int index) {
        return fields.get(index);
    }

    /**
     * 按名称获取字段
     */
    public EtlField getField(String name) {
        return fields.stream()
            .filter(f -> f.getName().equals(name))
            .findFirst()
            .orElse(null);
    }

    /**
     * 获取所有字段名
     */
    public List<String> getFieldNames() {
        return fields.stream()
            .map(EtlField::getName)
            .collect(Collectors.toList());
    }
}