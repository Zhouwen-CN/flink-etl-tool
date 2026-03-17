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
    public static EtlSchema parse(Object schemaConfig) {
        if (schemaConfig == null) {
            return null;
        }

        // 类型校验
        if (!(schemaConfig instanceof Map)) {
            throw new SchemaConfigException("schema 必须是一个对象");
        }

        Map<String, Object> schemaMap = (Map<String, Object>) schemaConfig;
        Object fieldsObj = schemaMap.get("fields");

        if (fieldsObj == null) {
            throw new SchemaConfigException("schema 缺少 'fields' 字段");
        }

        if (!(fieldsObj instanceof List)) {
            throw new SchemaConfigException("'fields' 必须是数组");
        }

        List<Map<String, Object>> fieldsConfig = (List<Map<String, Object>>) fieldsObj;

        // 解析 tableName（可选）
        String tableName = (String) schemaMap.get("tableName");

        List<EtlField> fields = new ArrayList<>();
        for (int i = 0; i < fieldsConfig.size(); i++) {
            Map<String, Object> fieldConfig = fieldsConfig.get(i);

            Object nameObj = fieldConfig.get("name");
            if (nameObj == null) {
                throw new SchemaConfigException("字段[" + i + "] 缺少 'name'");
            }
            if (!(nameObj instanceof String)) {
                throw new SchemaConfigException("字段[" + i + "] 的 'name' 必须是字符串");
            }

            Object typeObj = fieldConfig.get("type");
            if (typeObj == null) {
                throw new SchemaConfigException("字段[" + i + "] 缺少 'type'");
            }
            if (!(typeObj instanceof String)) {
                throw new SchemaConfigException("字段[" + i + "] 的 'type' 必须是字符串");
            }

            String name = (String) nameObj;
            String typeName = (String) typeObj;
            EtlFieldType type = EtlFieldType.fromString(typeName);
            if (type == null) {
                throw new SchemaConfigException(
                    "字段[" + i + "] '" + name + "' 的类型 '" + typeName + "' 不支持");
            }

            fields.add(new EtlField(name, type));
        }

        return new EtlSchema(tableName, fields);
    }
}