package com.etl.core.config;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.SchemaParser;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

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

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient EtlSchema cachedSchema;

    /**
     * 获取 Schema 配置（缓存解析结果，config 构造后不可变）
     *
     * @return EtlSchema 对象，如果未配置则返回 null
     */
    public EtlSchema getSchema() {
        if (cachedSchema != null) {
            return cachedSchema;
        }
        Map<String,Object> schemaConfig = get("schema", Map.class);
        if (schemaConfig == null) {
            return null;
        }
        cachedSchema = SchemaParser.parse(schemaConfig);
        return cachedSchema;
    }
}