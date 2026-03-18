package com.etl.core.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Schema 解析器
 * 从配置对象解析 EtlSchema
 */
public class SchemaParser {

    @SuppressWarnings("unchecked")
    public static EtlSchema parse(Object schemaConfig, String tableName) {
        if (schemaConfig == null) {
            return null;
        }

        // 新格式：schema 直接是字段数组
        if (!(schemaConfig instanceof List)) {
            throw new SchemaConfigException("schema 必须是一个数组");
        }

        List<Map<String, Object>> fieldsConfig = (List<Map<String, Object>>) schemaConfig;
        List<EtlField> fields = new ArrayList<>();

        for (int i = 0; i < fieldsConfig.size(); i++) {
            Map<String, Object> fieldConfig = fieldsConfig.get(i);

            Object nameObj = fieldConfig.get("name");
            if (nameObj == null) {
                throw new SchemaConfigException("字段 [" + i + "] 缺少 'name'");
            }
            if (!(nameObj instanceof String)) {
                throw new SchemaConfigException("字段 [" + i + "] 的 'name' 必须是字符串");
            }

            Object typeObj = fieldConfig.get("type");
            if (typeObj == null) {
                throw new SchemaConfigException("字段 [" + i + "] 缺少 'type'");
            }
            if (!(typeObj instanceof String)) {
                throw new SchemaConfigException("字段 [" + i + "] 的 'type' 必须是字符串");
            }

            String name = (String) nameObj;
            String typeName = (String) typeObj;
            EtlFieldType type = EtlFieldType.fromString(typeName);
            if (type == null) {
                throw new SchemaConfigException(
                    "字段 [" + i + "] '" + name + "' 的类型 '" + typeName + "' 不支持");
            }

            fields.add(new EtlField(name, type));
        }

        return new EtlSchema(tableName, fields);
    }
}
