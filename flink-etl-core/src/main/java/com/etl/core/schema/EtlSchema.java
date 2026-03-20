package com.etl.core.schema;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * ETL Schema 定义
 * 直接存储字段名和 Flink TypeInformation，消除中间转换层
 */
@Data
@NoArgsConstructor
public class EtlSchema implements Serializable {

    /**
     * 字段名数组
     */
    private String[] fieldNames;

    /**
     * 字段类型数组（Flink TypeInformation）
     */
    private TypeInformation<?>[] fieldTypes;

    /**
     * 构造函数，校验字段名和字段类型数组
     *
     * @param fieldNames  字段名数组
     * @param fieldTypes  字段类型数组
     */
    public EtlSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (fieldNames == null || fieldTypes == null) {
            throw new IllegalArgumentException("fieldNames 和 fieldTypes 不能为 null");
        }
        if (fieldNames.length != fieldTypes.length) {
            throw new IllegalArgumentException(
                "fieldNames 和 fieldTypes 长度不一致: " + fieldNames.length + " vs " + fieldTypes.length);
        }
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    /**
     * 获取字段数量
     */
    public int getFieldCount() {
        return fieldNames != null ? fieldNames.length : 0;
    }

    /**
     * 按索引获取字段名
     */
    public String getFieldName(int index) {
        if (fieldNames == null) {
            throw new IllegalStateException("Schema 未初始化");
        }
        if (index < 0 || index >= fieldNames.length) {
            throw new IndexOutOfBoundsException(
                "字段索引越界: " + index + ", 有效范围 [0, " + fieldNames.length + ")");
        }
        return fieldNames[index];
    }

    /**
     * 按索引获取字段类型
     */
    public TypeInformation<?> getFieldType(int index) {
        if (fieldTypes == null) {
            throw new IllegalStateException("Schema 未初始化");
        }
        if (index < 0 || index >= fieldTypes.length) {
            throw new IndexOutOfBoundsException(
                "字段索引越界: " + index + ", 有效范围 [0, " + fieldTypes.length + ")");
        }
        return fieldTypes[index];
    }

    /**
     * 获取所有字段名（List 形式，便于使用）
     */
    public List<String> getFieldNamesAsList() {
        return fieldNames != null ? Arrays.asList(fieldNames) : Collections.emptyList();
    }
}