package com.etl.core.schema;

import com.etl.core.exception.SchemaConfigException;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema 解析器
 * 从配置对象解析 EtlSchema
 *
 * <p>配置格式：
 * <ul>
 *   <li>简单类型：{ "fieldName": "TYPE" }</li>
 *   <li>基础类型数组：{ "tags": ["STRING"] }</li>
 *   <li>OBJECT 类型：{ "address": { "city": "STRING" } }</li>
 *   <li>对象数组：{ "friends": [{"name": "STRING"}] }</li>
 * </ul>
 *
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
     * 解析对象格式：{ "fieldName": "TYPE" | { ... } | [...] }
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

            TypeInformation<?> type = parseType(fieldName, typeObj);
            names.add(fieldName);
            types.add(type);
        }

        return new EtlSchema(
            names.toArray(new String[0]),
            types.toArray(new TypeInformation<?>[0])
        );
    }

    /**
     * 解析类型定义
     *
     * @param fieldName 字段名（用于错误提示）
     * @param typeObj 类型定义对象（字符串、Map 或 List）
     * @return Flink TypeInformation
     */
    @SuppressWarnings("unchecked")
    private static TypeInformation<?> parseType(String fieldName, Object typeObj) {
        if (typeObj instanceof String) {
            return parseSimpleType(fieldName, (String) typeObj);
        } else if (typeObj instanceof Map) {
            // OBJECT 类型
            return parseObjectType(fieldName, (Map<String, Object>) typeObj);
        } else if (typeObj instanceof List) {
            // 数组类型：["STRING"] 或 [{"name": "STRING"}]
            return parseArrayType(fieldName, (List<?>) typeObj);
        } else {
            throw new SchemaConfigException(
                "字段 '" + fieldName + "' 的类型必须是字符串、对象或数组");
        }
    }

    /**
     * 解析简单类型
     */
    private static TypeInformation<?> parseSimpleType(String fieldName, String typeName) {
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

    /**
     * 解析 OBJECT 类型
     * 返回 RowTypeInfo
     */
    private static TypeInformation<?> parseObjectType(String fieldName, Map<String, Object> objectDef) {
        List<String> fieldNames = new ArrayList<>();
        List<TypeInformation<?>> fieldTypes = new ArrayList<>();

        for (Map.Entry<String, Object> entry : objectDef.entrySet()) {
            String nestedFieldName = entry.getKey();
            Object nestedTypeObj = entry.getValue();

            fieldNames.add(nestedFieldName);
            fieldTypes.add(parseType(fieldName + "." + nestedFieldName, nestedTypeObj));
        }

        return Types.ROW_NAMED(
            fieldNames.toArray(new String[0]),
            fieldTypes.toArray(new TypeInformation<?>[0])
        );
    }

    /**
     * 解析数组类型
     * 支持两种格式：
     * - 基础类型数组：["STRING"], ["INT"]
     * - 对象数组：[{"name": "STRING", "age": "INT"}]
     */
    @SuppressWarnings("unchecked")
    private static TypeInformation<?> parseArrayType(String fieldName, List<?> arrayDef) {
        if (arrayDef.size() != 1) {
            throw new SchemaConfigException(
                "字段 '" + fieldName + "' 的数组类型定义长度必须为 1");
        }

        Object elementDef = arrayDef.get(0);

        if (elementDef instanceof String) {
            // 基础类型数组：["STRING"]
            return parseBasicArrayType(fieldName, (String) elementDef);
        } else if (elementDef instanceof Map) {
            // 对象数组：[{"name": "STRING"}]
            TypeInformation<?> elementType = parseObjectType(fieldName + "[]", (Map<String, Object>) elementDef);
            return Types.OBJECT_ARRAY(elementType);
        } else {
            throw new SchemaConfigException(
                "字段 '" + fieldName + "' 的数组元素类型必须是字符串（基础类型）或对象");
        }
    }

    /**
     * 解析基础类型数组：["STRING"], ["INT"]
     */
    private static TypeInformation<?> parseBasicArrayType(String fieldName, String elementTypeName) {
        TypeInformation<?> elementType = parseSimpleType(fieldName + "[]", elementTypeName);

        if (elementType instanceof BasicTypeInfo) {
            BasicTypeInfo<?> basicTypeInfo = (BasicTypeInfo<?>) elementType;
            return BasicArrayTypeInfo.getInfoFor(
                Array.newInstance(basicTypeInfo.getTypeClass(), 0).getClass()
            );
        }

        return Types.OBJECT_ARRAY(elementType);
    }
}