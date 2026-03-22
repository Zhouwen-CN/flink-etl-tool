package com.etl.core.schema;

import com.etl.core.exception.SchemaConfigException;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaParserTest {

    @Test
    void testParseObjectFormat() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("id", "LONG");
        schemaConfig.put("name", "STRING");
        schemaConfig.put("age", "INT");

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertNotNull(schema);
        assertEquals(3, schema.getFieldCount());
        assertEquals("id", schema.getFieldName(0));
        assertEquals(Types.LONG, schema.getFieldType(0));
        assertEquals("name", schema.getFieldName(1));
        assertEquals(Types.STRING, schema.getFieldType(1));
        assertEquals("age", schema.getFieldName(2));
        assertEquals(Types.INT, schema.getFieldType(2));
    }

    @Test
    void testParseNullReturnsNull() {
        assertNull(SchemaParser.parse(null));
    }

    @Test
    void testParseInvalidFormatThrowsException() {
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse("invalid"));
    }

    @Test
    void testParseInvalidTypeThrowsException() {
        Map<String, Object> schemaConfig = Map.of("id", "INVALID_TYPE");
        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
    }

    @Test
    void testAllSupportedTypes() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("f1", "STRING");
        schemaConfig.put("f2", "BOOLEAN");
        schemaConfig.put("f3", "INT");
        schemaConfig.put("f4", "LONG");
        schemaConfig.put("f5", "DOUBLE");
        schemaConfig.put("f6", "DECIMAL");
        schemaConfig.put("f7", "TIMESTAMP");

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertEquals(Types.STRING, schema.getFieldType(0));
        assertEquals(Types.BOOLEAN, schema.getFieldType(1));
        assertEquals(Types.INT, schema.getFieldType(2));
        assertEquals(Types.LONG, schema.getFieldType(3));
        assertEquals(Types.DOUBLE, schema.getFieldType(4));
        assertEquals(Types.BIG_DEC, schema.getFieldType(5));
        assertEquals(Types.LOCAL_DATE_TIME, schema.getFieldType(6));
    }

    // ========== 复杂类型测试 ==========

    @Test
    void testParseArraySimpleType() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("tags", "ARRAY<STRING>");
        schemaConfig.put("scores", "ARRAY<INT>");

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertEquals(2, schema.getFieldCount());

        // tags: ARRAY<STRING> → BasicArrayTypeInfo
        TypeInformation<?> tagsType = schema.getFieldType(0);
        assertTrue(tagsType instanceof BasicArrayTypeInfo);
        BasicArrayTypeInfo<?, ?> arrayInfo = (BasicArrayTypeInfo<?, ?>) tagsType;
        assertEquals(Types.STRING, arrayInfo.getComponentInfo());

        // scores: ARRAY<INT> → BasicArrayTypeInfo
        TypeInformation<?> scoresType = schema.getFieldType(1);
        assertTrue(scoresType instanceof BasicArrayTypeInfo);
        BasicArrayTypeInfo<?, ?> scoresArrayInfo = (BasicArrayTypeInfo<?, ?>) scoresType;
        assertEquals(Types.INT, scoresArrayInfo.getComponentInfo());
    }

    @Test
    void testParseObjectType() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        Map<String, Object> addressDef = new LinkedHashMap<>();
        addressDef.put("city", "STRING");
        addressDef.put("street", "STRING");
        schemaConfig.put("address", addressDef);

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        // address: OBJECT → RowTypeInfo
        TypeInformation<?> addressType = schema.getFieldType(0);
        assertTrue(addressType instanceof RowTypeInfo);

        RowTypeInfo rowTypeInfo = (RowTypeInfo) addressType;
        assertArrayEquals(new String[]{"city", "street"}, rowTypeInfo.getFieldNames());
        assertEquals(Types.STRING, rowTypeInfo.getFieldTypes()[0]);
        assertEquals(Types.STRING, rowTypeInfo.getFieldTypes()[1]);
    }

    @Test
    void testParseObjectWithNestedArray() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        Map<String, Object> addressDef = new LinkedHashMap<>();
        addressDef.put("city", "STRING");
        addressDef.put("zipcodes", "ARRAY<INT>");
        schemaConfig.put("address", addressDef);

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        RowTypeInfo addressType = (RowTypeInfo) schema.getFieldType(0);
        TypeInformation<?> zipcodesType = addressType.getFieldTypes()[1];
        assertTrue(zipcodesType instanceof BasicArrayTypeInfo);
        BasicArrayTypeInfo<?, ?> zipcodesArrayInfo = (BasicArrayTypeInfo<?, ?>) zipcodesType;
        assertEquals(Types.INT, zipcodesArrayInfo.getComponentInfo());
    }

    @Test
    void testParseArrayObjectType() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();

        Map<String, Object> friendDef = new LinkedHashMap<>();
        friendDef.put("name", "STRING");
        friendDef.put("age", "INT");
        schemaConfig.put("friends", List.of(friendDef));

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        // friends: ARRAY<OBJECT> → ObjectArrayTypeInfo<RowTypeInfo>
        TypeInformation<?> friendsType = schema.getFieldType(0);
        assertTrue(friendsType instanceof ObjectArrayTypeInfo);

        ObjectArrayTypeInfo<?, ?> arrayInfo = (ObjectArrayTypeInfo<?, ?>) friendsType;
        RowTypeInfo elementTypeInfo = (RowTypeInfo) arrayInfo.getComponentInfo();

        assertArrayEquals(new String[]{"name", "age"}, elementTypeInfo.getFieldNames());
        assertEquals(Types.STRING, elementTypeInfo.getFieldTypes()[0]);
        assertEquals(Types.INT, elementTypeInfo.getFieldTypes()[1]);
    }

    @Test
    void testParseNestedArrayInObjectArray() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();

        Map<String, Object> friendDef = new LinkedHashMap<>();
        friendDef.put("name", "STRING");
        friendDef.put("tags", "ARRAY<STRING>");
        schemaConfig.put("friends", List.of(friendDef));

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        ObjectArrayTypeInfo<?, ?> friendsType = (ObjectArrayTypeInfo<?, ?>) schema.getFieldType(0);
        RowTypeInfo friendType = (RowTypeInfo) friendsType.getComponentInfo();

        TypeInformation<?> tagsType = friendType.getFieldTypes()[1];
        assertTrue(tagsType instanceof BasicArrayTypeInfo);
        BasicArrayTypeInfo<?, ?> tagsArrayInfo = (BasicArrayTypeInfo<?, ?>) tagsType;
        assertEquals(Types.STRING, tagsArrayInfo.getComponentInfo());
    }

    @Test
    void testParseCompleteNestedStructure() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("id", "LONG");
        schemaConfig.put("hobby", "ARRAY<STRING>");

        Map<String, Object> addressDef = new LinkedHashMap<>();
        addressDef.put("city", "STRING");
        addressDef.put("zipcodes", "ARRAY<INT>");
        schemaConfig.put("address", addressDef);

        Map<String, Object> friendDef = new LinkedHashMap<>();
        friendDef.put("name", "STRING");
        friendDef.put("tags", "ARRAY<STRING>");
        schemaConfig.put("friends", List.of(friendDef));

        EtlSchema schema = SchemaParser.parse(schemaConfig);

        assertEquals(4, schema.getFieldCount());
        assertEquals(Types.LONG, schema.getFieldType(0));
        assertTrue(schema.getFieldType(1) instanceof BasicArrayTypeInfo);
        assertTrue(schema.getFieldType(2) instanceof RowTypeInfo);
        assertTrue(schema.getFieldType(3) instanceof ObjectArrayTypeInfo);
    }

    @Test
    void testParseInvalidArrayFormat() {
        Map<String, Object> schemaConfig = new LinkedHashMap<>();
        schemaConfig.put("tags", "ARRAY<INVALID>");

        assertThrows(SchemaConfigException.class, () -> SchemaParser.parse(schemaConfig));
    }
}