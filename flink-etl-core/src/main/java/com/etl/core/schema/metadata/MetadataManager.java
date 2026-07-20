package com.etl.core.schema.metadata;

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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * metadata 字段有严格顺序，和 row 保持一致
 */
public class MetadataManager {

    private static final String TOPIC_FIELD = "topic";
    private static final String SOURCE_FIELD = "source";
    public static final String METADATA_FIELD = "_metadata";

    public static Row removeMetadata(Row row) {
        return extractMetadata(row).getKey();
    }

    public static Pair<Row, Metadata> extractMetadata(Row row) {
        Set<String> fieldNames = row.getFieldNames(true);
        RowKind kind = row.getKind();

        List<Object> fieldByPosition = new ArrayList<>();
        LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>();
        Map<String, String> metadataMap = new HashMap<>();

        int i = 0;
        for (String fieldName : fieldNames) {
            Object value = row.getField(fieldName);
            if (METADATA_FIELD.equals(fieldName) && value instanceof Row) {
                Row r = (Row) value;
                Set<String> fn = r.getFieldNames(true);
                for (String key : fn) {
                    metadataMap.put(key, r.getFieldAs(key));
                }
                continue;
            }

            fieldByPosition.add(value);
            positionByName.put(fieldName, i);
            i++;
        }

        Metadata metadata = Metadata.builder()
                .topic(metadataMap.get(TOPIC_FIELD))
                .source(metadataMap.get(SOURCE_FIELD))
                .build();

        return Pair.of(RowUtils.createRowWithNamedPositions(kind, fieldByPosition.toArray(), positionByName), metadata);
    }

    public static EtlSchema addMetadata(EtlSchema etlSchema) {
        return addMetadata(etlSchema.getFieldNames(), etlSchema.getFieldTypes());
    }

    public static EtlSchema addMetadata(RowTypeInfo rowTypeInfo) {
        return addMetadata(rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
    }


    private static EtlSchema addMetadata(String[] names, TypeInformation<?>[] types) {
        int length = names.length;
        int size = length + 1;

        String[] newNames = Arrays.copyOf(names, size);
        TypeInformation<?>[] newTypes = Arrays.copyOf(types, size);

        newNames[length] = METADATA_FIELD;
        newTypes[length] = Types.ROW_NAMED(new String[]{TOPIC_FIELD, SOURCE_FIELD}, Types.STRING, Types.STRING);

        return new EtlSchema(newNames, newTypes);
    }
}
