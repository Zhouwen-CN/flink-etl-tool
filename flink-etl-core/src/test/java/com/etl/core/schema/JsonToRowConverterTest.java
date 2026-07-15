package com.etl.core.schema;

import com.etl.core.schema.convert.JsonToRowConverter;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JsonToRowConverter 测试
 * 重点测试数组类型的转换（验证反射优化的正确性）
 */
class JsonToRowConverterTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    // region 基本类型数组测试

    @Test
    void testConvertStringArray() throws Exception {
        // 测试 String[] 数组转换
        String json = "{\"id\":1,\"tags\":[\"tag1\",\"tag2\",\"tag3\"]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "tags"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        String[] tags = (String[]) row.getField(1);
        assertArrayEquals(new String[]{"tag1", "tag2", "tag3"}, tags);
    }

    @Test
    void testConvertIntegerArray() throws Exception {
        // 测试 Integer[] 数组转换
        String json = "{\"id\":1,\"scores\":[100,90,85]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "scores"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            ObjectArrayTypeInfo.getInfoFor(Types.INT)
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        Integer[] scores = (Integer[]) row.getField(1);
        assertArrayEquals(new Integer[]{100, 90, 85}, scores);
    }

    @Test
    void testConvertLongArray() throws Exception {
        // 测试 Long[] 数组转换
        String json = "{\"id\":1,\"timestamps\":[" + Long.MAX_VALUE + "," + (Long.MAX_VALUE - 1) + "," + (Long.MAX_VALUE - 2) + "]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "timestamps"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            ObjectArrayTypeInfo.getInfoFor(Types.LONG)
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        Long[] timestamps = (Long[]) row.getField(1);
        assertEquals(Long.MAX_VALUE, timestamps[0]);
        assertEquals(Long.MAX_VALUE - 1, timestamps[1]);
        assertEquals(Long.MAX_VALUE - 2, timestamps[2]);
    }

    @Test
    void testConvertDoubleArray() throws Exception {
        // 测试 Double[] 数组转换
        String json = "{\"id\":1,\"prices\":[10.5,20.3,30.8]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "prices"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            ObjectArrayTypeInfo.getInfoFor(Types.DOUBLE)
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        Double[] prices = (Double[]) row.getField(1);
        assertArrayEquals(new Double[]{10.5, 20.3, 30.8}, prices);
    }

    @Test
    void testConvertBooleanArray() throws Exception {
        // 测试 Boolean[] 数组转换
        String json = "{\"id\":1,\"flags\":[true,false,true]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "flags"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            ObjectArrayTypeInfo.getInfoFor(Types.BOOLEAN)
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        Boolean[] flags = (Boolean[]) row.getField(1);
        assertArrayEquals(new Boolean[]{true, false, true}, flags);
    }

    @Test
    void testConvertBigDecimalArray() throws Exception {
        // 测试 BigDecimal[] 数组转换（BigDecimal 在 JSON 中作为数字）
        String json = "{\"id\":1,\"amounts\":[100.50,200.75,300.00]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "amounts"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            ObjectArrayTypeInfo.getInfoFor(Types.BIG_DEC)
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        BigDecimal[] amounts = (BigDecimal[]) row.getField(1);
        // 使用 compareTo 比较 BigDecimal 值（忽略 scale）
        assertTrue(new BigDecimal("100.50").compareTo(amounts[0]) == 0);
        assertTrue(new BigDecimal("200.75").compareTo(amounts[1]) == 0);
        assertTrue(new BigDecimal("300.00").compareTo(amounts[2]) == 0);
    }

    @Test
    void testConvertLocalDateTimeArray() throws Exception {
        // 测试 LocalDateTime[] 数组转换
        String json = "{\"id\":1,\"times\":[\"2024-01-01 10:00:00\",\"2024-01-02 11:30:00\"]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "times"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            ObjectArrayTypeInfo.getInfoFor(Types.LOCAL_DATE_TIME)
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        LocalDateTime[] times = (LocalDateTime[]) row.getField(1);
        assertEquals(LocalDateTime.of(2024, 1, 1, 10, 0, 0), times[0]);
        assertEquals(LocalDateTime.of(2024, 1, 2, 11, 30, 0), times[1]);
    }

    // endregion

    // region 包含 null 值的数组测试

    @Test
    void testConvertStringArrayWithNull() throws Exception {
        // 测试包含 null 的 String[] 数组
        String json = "{\"id\":1,\"tags\":[\"tag1\",null,\"tag3\"]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "tags"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        String[] tags = (String[]) row.getField(1);
        assertEquals("tag1", tags[0]);
        assertNull(tags[1]);
        assertEquals("tag3", tags[2]);
    }

    @Test
    void testConvertIntegerArrayWithNull() throws Exception {
        // 测试包含 null 的 Integer[] 数组
        String json = "{\"id\":1,\"scores\":[100,null,85]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "scores"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            ObjectArrayTypeInfo.getInfoFor(Types.INT)
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        Integer[] scores = (Integer[]) row.getField(1);
        assertEquals(100, scores[0]);
        assertNull(scores[1]);
        assertEquals(85, scores[2]);
    }

    // endregion

    // region Row[] 复杂类型数组测试

    @Test
    void testConvertRowArray() throws Exception {
        // 测试 Row[] 数组（复杂类型数组）
        String json = "{\"id\":1,\"addresses\":[{\"city\":\"北京\",\"zip\":\"100001\"},{\"city\":\"上海\",\"zip\":\"200001\"}]}";
        JsonNode node = objectMapper.readTree(json);

        // 定义 address Row 类型
        RowTypeInfo addressRowType = new RowTypeInfo(
            new TypeInformation<?>[]{Types.STRING, Types.STRING},
            new String[]{"city", "zip"}
        );

        String[] fieldNames = {"id", "addresses"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            ObjectArrayTypeInfo.getInfoFor(addressRowType)
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));

        Row[] addresses = (Row[]) row.getField(1);
        assertEquals(2, addresses.length);

        // 验证第一个地址
        Row address1 = addresses[0];
        assertEquals("北京", address1.getField(0));
        assertEquals("100001", address1.getField(1));

        // 验证第二个地址
        Row address2 = addresses[1];
        assertEquals("上海", address2.getField(0));
        assertEquals("200001", address2.getField(1));
    }

    // endregion

    // region JSONArray 多条记录测试

    @Test
    void testConvertJSONArray() throws Exception {
        // 测试 JSONArray（多条记录）
        String json = "[{\"id\":1,\"name\":\"张三\"},{\"id\":2,\"name\":\"李四\"}]";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(2, rows.size());
        assertEquals(1L, rows.get(0).getField(0));
        assertEquals("张三", rows.get(0).getField(1));
        assertEquals(2L, rows.get(1).getField(0));
        assertEquals("李四", rows.get(1).getField(1));
    }

    // endregion

    // region 边界情况测试

    @Test
    void testConvertEmptyArray() throws Exception {
        // 测试空数组
        String json = "{\"id\":1,\"tags\":[]}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "tags"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        String[] tags = (String[]) row.getField(1);
        assertEquals(0, tags.length);
    }

    @Test
    void testConvertNullField() throws Exception {
        // 测试 null 字段
        String json = "{\"id\":1,\"name\":null}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "name"};
        TypeInformation<?>[] fieldTypes = {Types.LONG, Types.STRING};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        assertNull(row.getField(1));
    }

    @Test
    void testConvertNullArrayField() throws Exception {
        // 测试 null 数组字段
        String json = "{\"id\":1,\"tags\":null}";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id", "tags"};
        TypeInformation<?>[] fieldTypes = {
            Types.LONG,
            BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO
        };
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        List<Row> rows = JsonToRowConverter.convertJsonToRows(node, schema);

        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(1L, row.getField(0));
        assertNull(row.getField(1));
    }

    // endregion

    // region 异常情况测试

    @Test
    void testInvalidJsonType() throws Exception {
        // 测试无效 JSON 类型（既不是对象也不是数组）
        String json = "\"invalid\"";
        JsonNode node = objectMapper.readTree(json);

        String[] fieldNames = {"id"};
        TypeInformation<?>[] fieldTypes = {Types.LONG};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        assertThrows(IllegalArgumentException.class, () ->
            JsonToRowConverter.convertJsonToRows(node, schema));
    }

    @Test
    void testNullJsonNode() {
        // 测试 null JsonNode
        String[] fieldNames = {"id"};
        TypeInformation<?>[] fieldTypes = {Types.LONG};
        EtlSchema schema = new EtlSchema(fieldNames, fieldTypes);

        assertThrows(IllegalArgumentException.class, () ->
            JsonToRowConverter.convertJsonToRows(null, schema));
    }

    // endregion
}