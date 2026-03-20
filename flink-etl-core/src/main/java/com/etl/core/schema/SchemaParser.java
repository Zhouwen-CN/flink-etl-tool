package com.etl.core.schema;

import com.etl.core.exception.SchemaConfigException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema 解析器
 * 从配置对象解析 EtlSchema
 *
 * <p>配置格式：{ "fieldName": "TYPE", ... }
 * <p>支持类型：STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP
 */
public class SchemaParser {

    @SuppressWarnings("unchecked")
    public static EtlSchema parse(Object schemaConfig) {
        if (schemaConfig == null) {
            return null;
        }

        if (!(schemaConfig instanceof Map)) {
            throw new SchemaConfigException("schema 必须是对象格式 {fieldName: fieldType}");
        }

        return parseObjectFormat((Map<String, Object>) schemaConfig);
    }

    /**
     * 解析对象格式：{ "fieldName": "TYPE" }
     */
    private static EtlSchema parseObjectFormat(Map<String, Object> schemaConfig) {
        List<String> names = new ArrayList<>();
        List<TypeInformation<?>> types = new ArrayList<>();

        // 保持字段顺序
        Map<String, Object> orderedConfig = schemaConfig instanceof LinkedHashMap
            ? schemaConfig
            : new LinkedHashMap<>(schemaConfig);

        for (Map.Entry<String, Object> entry : orderedConfig.entrySet()) {
            String fieldName = entry.getKey();
            Object typeObj = entry.getValue();

            if (!(typeObj instanceof String)) {
                throw new SchemaConfigException(
                    "字段 '" + fieldName + "' 的类型必须是字符串");
            }

            TypeInformation<?> type = parseType((String) typeObj, fieldName);
            names.add(fieldName);
            types.add(type);
        }

        return new EtlSchema(
            names.toArray(new String[0]),
            types.toArray(new TypeInformation<?>[0])
        );
    }

    /**
     * 解析类型字符串为 Flink TypeInformation
     */
    private static TypeInformation<?> parseType(String typeName, String fieldName) {
        switch (typeName.toUpperCase()) {
            case "STRING":
                return Types.STRING;
            case "BOOLEAN":
                return Types.BOOLEAN;
            case "INT":
                return Types.INT;
            case "LONG":
                return Types.LONG;
            case "DOUBLE":
                return Types.DOUBLE;
            case "DECIMAL":
                return Types.BIG_DEC;
            case "TIMESTAMP":
                return Types.LOCAL_DATE_TIME;
            default:
                throw new SchemaConfigException(
                    "字段 '" + fieldName + "' 的类型 '" + typeName + "' 不支持，" +
                    "支持的类型: STRING, BOOLEAN, INT, LONG, DOUBLE, DECIMAL, TIMESTAMP");
        }
    }
}