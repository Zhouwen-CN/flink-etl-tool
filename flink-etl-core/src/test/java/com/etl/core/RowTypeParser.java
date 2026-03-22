package com.etl.core;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * <p>
 * 根据字符串 schema 获取类型信息，根据类型信息解析 json
 * <pre>
 *     BasicTypeInfo：基本类型
 *     BasicArrayTypeInfo：包装类数组类型（还有一个基本数组类型，猜测 null 可能会有问题）
 *     ObjectArrayTypeInfo：对象数组类型
 *     RowTypeInfo：行类型
 * </pre>
 * </p>
 *
 * @author chen
 * @since 2025-07-09
 */
public class RowTypeParser implements Serializable {
    private static final String ROW_TYPE_PREFIX = "row<";
    private static final String ARRAY_TYPE_PREFIX = "array<";
    private static final String TYPE_SUFFIX = ">";
    private final List<Field> fields;
    private final ObjectMapper objectMapper;
    // 根节点不能是 array，如果是的话，可以用这个标记flatmap，然后当做行处理
    @Getter
    private Boolean isArrayRootNode;

    // 可能会需要用 topic 做些什么，可以自定义序列化器，把 topic 当做隐藏字段写入流中
    // private static final Field topic = new Field("_topic", Types.STRING);

    public RowTypeParser(String schema) {
        this.fields = this.parseRowType(schema);
        this.objectMapper = new ObjectMapper();
        this.isArrayRootNode = false;
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        streamEnvironment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamEnvironment);

        // 假设这是 kafka 中的数据
        String message = "{" +
                "  \"c_i\":1," +
                "  \"c_s\":\"s1\"," +
                "  \"c_r\":{" +
                "    \"c_r1\":1," +
                "    \"c_r2\":\"r2\"" +
                "  }," +
                "  \"c_ba\":[1,2,3]," +
                "  \"c_ra\":[" +
                "    {" +
                "      \"c_ra1\":2," +
                "      \"c_ra2\":\"ra2\"" +
                "    }," +
                "    {" +
                "      \"c_ra1\":3," +
                "      \"c_ra2\":\"ra2\"" +
                "    }" +
                "  ]" +
                "}";

        // 假设这是配置文件中的 schema 信息
        String schema = "row<c_i int,c_s string,c_r row<c_r1 int,c_r2 string>,c_ba array<int>,c_ra array<row<c_ra1 int,c_ra2 string>>>";

        RowTypeParser rowTypeParser = new RowTypeParser(schema);

        SingleOutputStreamOperator<Row> source = streamEnvironment
                .fromElements(message)
                .map(rowTypeParser::convert)
                .returns(rowTypeParser.getReturnType());

        Table table = tableEnvironment.fromDataStream(source);
        table.printSchema();
        tableEnvironment
                .sqlQuery("select c_ra[1].c_ra1 as col from " + table)
                .execute()
                .print();
    }

    //=========== 解析字符串 schema 获取类型信息
    private List<Field> parseRowType(String schema) {
        if (this.isRowType(schema)) {
            String inner = schema.substring(ROW_TYPE_PREFIX.length(), schema.length() - 1);
            return this.parseFieldList(inner);
        } else if (this.isArrayType(schema)) {
            isArrayRootNode = true;
            String inner = schema.substring(ARRAY_TYPE_PREFIX.length(), schema.length() - 1);
            return this.parseRowType(inner);
        } else {
            throw new IllegalArgumentException("Schema must start with ('row<' or 'array<') and end with '>'");
        }
    }

    private List<Field> parseFieldList(String listStr) {
        List<String> fieldStrList = this.splitIgnoringNested(listStr);
        List<Field> fields = new ArrayList<>();
        for (String fieldStr : fieldStrList) {
            int idx = fieldStr.indexOf(' ');
            if (idx == -1) throw new IllegalArgumentException("Invalid field: " + fieldStr);
            String name = fieldStr.substring(0, idx).trim();
            String typeStr = fieldStr.substring(idx + 1).trim();
            fields.add(new Field(name, this.parseType(typeStr)));
        }
        return fields;
    }

    private List<String> splitIgnoringNested(String str) {
        List<String> parts = new ArrayList<>();
        int start = 0;
        int depth = 0; // 括号嵌套深度
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '<') depth++;
            else if (c == '>') depth--;
            else if (c == ',' && depth == 0) {
                parts.add(str.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(str.substring(start));
        return parts;
    }

    private TypeInformation<?> parseType(String typeStr) {
        if (this.isRowType(typeStr)) {
            String inner = typeStr.substring(ROW_TYPE_PREFIX.length(), typeStr.length() - 1);
            List<Field> rowFields = this.parseFieldList(inner);
            String[] fieldNames = new String[rowFields.size()];
            TypeInformation<?>[] types = new TypeInformation<?>[rowFields.size()];
            for (int i = 0; i < rowFields.size(); i++) {
                Field field = rowFields.get(i);
                fieldNames[i] = field.name;
                types[i] = field.type;
            }
            return Types.ROW_NAMED(fieldNames, types);
        } else if (this.isArrayType(typeStr)) {
            String inner = typeStr.substring(ARRAY_TYPE_PREFIX.length(), typeStr.length() - 1);
            TypeInformation<?> typeInformation = this.parseType(inner);
            if (typeInformation instanceof BasicTypeInfo) {
                BasicTypeInfo<?> basicTypeInfo = (BasicTypeInfo<?>) typeInformation;
                return BasicArrayTypeInfo.getInfoFor(
                        Array.newInstance(basicTypeInfo.getTypeClass(), 0).getClass()
                );
            }
            return Types.OBJECT_ARRAY(typeInformation);
        } else {
            typeStr = typeStr.toLowerCase();
            switch (typeStr) {
                case "string":
                    return Types.STRING;
                case "int":
                    return Types.INT;
                case "bigint":
                    return Types.LONG;
                case "double":
                    return Types.DOUBLE;
                case "boolean":
                    return Types.BOOLEAN;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + typeStr);
            }
        }
    }

    private boolean isRowType(String typeStr) {
        return StringUtils.startsWithIgnoreCase(typeStr, ROW_TYPE_PREFIX) && typeStr.endsWith(TYPE_SUFFIX);
    }

    private boolean isArrayType(String typeStr) {
        return StringUtils.startsWithIgnoreCase(typeStr, ARRAY_TYPE_PREFIX) && typeStr.endsWith(TYPE_SUFFIX);
    }


    //===========根据类型信息将 json 转化成 row
    public Row convert(String message) {
        Map<String, JsonNode> readValue = this.readValue(message);
        return this.convert(this.fields, readValue);
    }

    private Row convert(List<Field> fields, Map<String, JsonNode> map) {
        int arity = fields.size();
        Row row = Row.withPositions(RowKind.INSERT, arity);
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            String name = field.name;
            TypeInformation<?> type = field.type;

            JsonNode jsonNode = map.get(name);
            Object value = this.getValue(type, jsonNode);

            row.setField(i, value);
        }

        return row;
    }

    private Object getValue(TypeInformation<?> type, JsonNode jsonNode) {
        if (jsonNode == null || jsonNode.isNull()) {
            return null;
        }

        if (type instanceof BasicTypeInfo) {
            BasicTypeInfo<?> basicTypeInfo = (BasicTypeInfo<?>) type;
            Class<?> typeClass = basicTypeInfo.getTypeClass();
            if (typeClass == String.class) {
                return jsonNode.asText();
            } else if (typeClass == Integer.class) {
                return jsonNode.asInt();
            } else if (typeClass == Long.class) {
                return jsonNode.asLong();
            } else if (typeClass == Double.class) {
                return jsonNode.asDouble();
            } else if (typeClass == Boolean.class) {
                return jsonNode.asBoolean();
            }
        } else if (type instanceof RowTypeInfo) {
            Map<String, JsonNode> map = this.convert(jsonNode);
            RowTypeInfo rowTypeInfo = (RowTypeInfo) type;
            String[] fieldNames = rowTypeInfo.getFieldNames();
            TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
            List<Field> fieldList = new ArrayList<>();
            for (int i = 0; i < fieldNames.length; i++) {
                Field field = new Field(fieldNames[i], fieldTypes[i]);
                fieldList.add(field);
            }
            return this.convert(fieldList, map);
        } else if (type instanceof BasicArrayTypeInfo || type instanceof ObjectArrayTypeInfo) {
            TypeInformation<?> componentInfo;
            if (type instanceof BasicArrayTypeInfo) {
                componentInfo = ((BasicArrayTypeInfo<?, ?>) type).getComponentInfo();
            } else {
                componentInfo = ((ObjectArrayTypeInfo<?, ?>) type).getComponentInfo();
            }
            ArrayList<Object> values = new ArrayList<>();
            for (JsonNode next : jsonNode) {
                Object value = this.getValue(componentInfo, next);
                values.add(value);
            }
            return values.toArray(new Object[0]);
        }
        throw new RuntimeException("Not support type: " + type);
    }

    // 获取 returns 类型
    public TypeInformation<Row> getReturnType() {
        val size = this.fields.size();
        String[] fieldNames = new String[size];
        TypeInformation<?>[] types = new TypeInformation<?>[size];
        for (int i = 0; i < size; i++) {
            Field field = this.fields.get(i);
            fieldNames[i] = field.name;
            types[i] = field.type;
        }
        return Types.ROW_NAMED(fieldNames, types);
    }

    // json 解析
    @SneakyThrows
    private Map<String, JsonNode> readValue(String message) {
        return objectMapper.readValue(message, new TypeReference<Map<String, JsonNode>>() {
        });
    }

    private Map<String, JsonNode> convert(JsonNode jsonNode) {
        return objectMapper.convertValue(jsonNode, new TypeReference<Map<String, JsonNode>>() {
        });
    }

    @AllArgsConstructor
    public static class Field implements Serializable {
        private String name;
        private TypeInformation<?> type;
    }
}