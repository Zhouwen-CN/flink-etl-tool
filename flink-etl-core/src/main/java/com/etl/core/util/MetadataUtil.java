package com.etl.core.util;

import com.etl.core.schema.EtlSchema;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 元数据工具类
 */
public final class MetadataUtil {
    public static final String SOURCE = "__SOURCE__";
    private static final Set<String> METADATA = new HashSet<>();

    static {
        METADATA.add(SOURCE);
    }

    public static Row removeAllMetadata(Row row) {
        return removeMetadata(row, METADATA).getKey();
    }

    /**
     * 删除元数据
     *
     * @param row  行
     * @param keys 需要删除元数据的key列表
     * @return key：删除元数据之后的行；value：元数据
     */
    private static Pair<Row, Map<String, String>> removeMetadata(Row row, Set<String> keys) {
        Set<String> fieldNames = row.getFieldNames(true);

        RowKind kind = row.getKind();
        List<Object> fieldByPosition = new ArrayList<>();
        LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>();
        Map<String, String> map = new HashMap<>();

        int i = 0;
        for (String fieldName : fieldNames) {
            Object value = row.getField(fieldName);
            if (keys.contains(fieldName)) {
                map.put(fieldName, value == null ? null : value.toString());
                continue;
            }

            fieldByPosition.add(value);
            positionByName.put(fieldName, i);
            i++;
        }

        return Pair.of(RowUtils.createRowWithNamedPositions(kind, fieldByPosition.toArray(), positionByName), map);
    }

    public static EtlSchema addSourceToSchema(EtlSchema schema) {
        return addMetadata(schema.getFieldNames(), schema.getFieldTypes(), Collections.singletonMap(SOURCE, Types.STRING));
    }

    public static EtlSchema addSourceToSchema(RowTypeInfo rowTypeInfo) {
        return addMetadata(rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes(), Collections.singletonMap(SOURCE, Types.STRING));
    }

    private static EtlSchema addMetadata(String[] names, TypeInformation<?>[] types, Map<String, TypeInformation<?>> extraMap) {
        int oldSize = names.length;
        int size = oldSize + extraMap.size();

        String[] newNames = Arrays.copyOf(names, size);
        TypeInformation<?>[] newTypes = Arrays.copyOf(types, size);

        int i = 0;
        for (Map.Entry<String, TypeInformation<?>> entry : extraMap.entrySet()) {
            String name = entry.getKey();
            TypeInformation<?> type = entry.getValue();
            newNames[oldSize + i] = name;
            newTypes[oldSize + i] = type;
            i++;
        }

        return new EtlSchema(newNames, newTypes);
    }
}
