package com.etl.core.config;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.SchemaParser;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Source 配置类
 * 定义数据源的基本配置
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class SourceConfig extends BaseConfig {

    private String type;
    private String outputTable;

    /**
     * 获取 Schema 配置
     *
     * @return EtlSchema 对象，如果未配置则返回 null
     */
    public EtlSchema getSchema() {
        Object schemaConfig = get("schema");
        if (schemaConfig == null) {
            return null;
        }
        return SchemaParser.parse(schemaConfig);
    }
}