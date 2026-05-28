package com.etl.connector.http.format;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.connector.http.source.format.XmlFormat;
import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * XmlFormat 单元测试
 */
class XmlFormatTest {

    private final XmlFormat format = new XmlFormat();

    private static EtlSchema schema() {
        return new EtlSchema(
                new String[]{"id", "name"},
                new TypeInformation<?>[]{Types.INT, Types.STRING});
    }

    @Test
    void testIdentifier() {
        assertEquals("xml", format.identifier());
    }

    @Test
    void testParseMultipleElements() {
        String xml = "<response>"
                + "<items>"
                + "<item><id>1</id><name>a</name></item>"
                + "<item><id>2</id><name>b</name></item>"
                + "</items>"
                + "</response>";
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("xml")
                .xmlPath("/response/items/item")
                .schema(schema())
                .build();

        List<Row> rows = format.parse(xml, config);

        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getField(0));
        assertEquals("a", rows.get(0).getField(1));
        assertEquals(2, rows.get(1).getField(0));
        assertEquals("b", rows.get(1).getField(1));
    }

    @Test
    void testParseSingleElementWithoutXmlPath() {
        String xml = "<root><id>10</id><name>x</name></root>";
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("xml")
                .schema(schema())
                .build();

        List<Row> rows = format.parse(xml, config);

        assertEquals(1, rows.size());
        assertEquals(10, rows.get(0).getField(0));
        assertEquals("x", rows.get(0).getField(1));
    }

    @Test
    void testParseAttributeFallback() {
        String xml = "<response>"
                + "<item id=\"5\" name=\"hello\"/>"
                + "</response>";
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("xml")
                .xmlPath("/response/item")
                .schema(schema())
                .build();

        List<Row> rows = format.parse(xml, config);

        assertEquals(1, rows.size());
        assertEquals(5, rows.get(0).getField(0));
        assertEquals("hello", rows.get(0).getField(1));
    }

    @Test
    void testParseMissingFieldYieldsNull() {
        String xml = "<response>"
                + "<item><id>7</id></item>"
                + "</response>";
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("xml")
                .xmlPath("/response/item")
                .schema(schema())
                .build();

        List<Row> rows = format.parse(xml, config);

        assertEquals(1, rows.size());
        assertEquals(7, rows.get(0).getField(0));
        assertNull(rows.get(0).getField(1));
    }

    @Test
    void testParseInvalidXmlThrows() {
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("xml")
                .xmlPath("/r")
                .schema(schema())
                .build();

        assertThrows(IllegalArgumentException.class, () -> format.parse("<not-xml", config));
    }

    // region 复杂类型

    @Test
    void testParseNestedObject() {
        String xml = "<record>"
                + "<id>1</id>"
                + "<address><city>北京</city><zip>100001</zip></address>"
                + "</record>";
        RowTypeInfo addressType = (RowTypeInfo) Types.ROW_NAMED(
                new String[]{"city", "zip"}, Types.STRING, Types.STRING);
        EtlSchema s = new EtlSchema(
                new String[]{"id", "address"},
                new TypeInformation<?>[]{Types.INT, addressType});
        HttpSourceConfig config = HttpSourceConfig.builder().format("xml").schema(s).build();

        List<Row> rows = format.parse(xml, config);

        assertEquals(1, rows.size());
        Row address = (Row) rows.get(0).getField(1);
        assertEquals("北京", address.getField(0));
        assertEquals("100001", address.getField(1));
    }

    @Test
    void testParseRowArray() {
        // 模拟 SOAP 数据集：多个同名子元素
        String xml = "<NewDataSet>"
                + "<ds><ID>1</ID><name>张三</name></ds>"
                + "<ds><ID>2</ID><name>李四</name></ds>"
                + "</NewDataSet>";
        RowTypeInfo dsType = (RowTypeInfo) Types.ROW_NAMED(
                new String[]{"ID", "name"}, Types.STRING, Types.STRING);
        TypeInformation<?> arrayType = ObjectArrayTypeInfo.getInfoFor(dsType);
        EtlSchema s = new EtlSchema(
                new String[]{"ds"},
                new TypeInformation<?>[]{arrayType});
        HttpSourceConfig config = HttpSourceConfig.builder()
                .format("xml")
                .xmlPath("/NewDataSet")
                .schema(s)
                .build();

        List<Row> rows = format.parse(xml, config);

        assertEquals(1, rows.size());
        Row[] items = (Row[]) rows.get(0).getField(0);
        assertEquals(2, items.length);
        assertEquals("1", items[0].getField(0));
        assertEquals("张三", items[0].getField(1));
        assertEquals("2", items[1].getField(0));
        assertEquals("李四", items[1].getField(1));
    }

    @Test
    void testParseStringArray() {
        String xml = "<record>"
                + "<tag>x</tag>"
                + "<tag>y</tag>"
                + "<tag>z</tag>"
                + "</record>";
        EtlSchema s = new EtlSchema(
                new String[]{"tag"},
                new TypeInformation<?>[]{BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO});
        HttpSourceConfig config = HttpSourceConfig.builder().format("xml").schema(s).build();

        List<Row> rows = format.parse(xml, config);

        assertEquals(1, rows.size());
        String[] tags = (String[]) rows.get(0).getField(0);
        assertArrayEquals(new String[]{"x", "y", "z"}, tags);
    }

    // endregion
}