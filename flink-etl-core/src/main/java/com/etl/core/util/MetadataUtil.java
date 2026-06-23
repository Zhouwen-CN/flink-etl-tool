package com.etl.core.util;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 元数据工具类
 */
public final class MetadataUtil {
    private static final String CDC_SOURCE = "__SOURCE__";
    private static final Set<String> METADATA = new HashSet<>();

    static {
        METADATA.add(CDC_SOURCE);
    }


    public static EtlSchema addSourceToSchema(EtlSchema schema) {
        return addMetadataToSchema(schema, CDC_SOURCE, Types.STRING);
    }

    private static EtlSchema addMetadataToSchema(EtlSchema schema, String fieldName, TypeInformation<?> fieldType) {
        int fieldCount = schema.getFieldCount();

        String[] f1 = schema.getFieldNames();
        TypeInformation<?>[] t1 = schema.getFieldTypes();

        String[] f2 = Arrays.copyOf(f1, fieldCount + 1);
        TypeInformation<?>[] t2 = Arrays.copyOf(t1, fieldCount + 1);

        f2[fieldCount] = fieldName;
        t2[fieldCount] = fieldType;

        return new EtlSchema(f2, t2);
    }
}
