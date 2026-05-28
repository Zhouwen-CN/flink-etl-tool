package com.etl.core.schema;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * XmlToRowConverter 单元测试
 */
class XmlToRowConverterTest {

    private Element parse(String xml) throws DocumentException {
        return new SAXReader().read(new StringReader(xml)).getRootElement();
    }

    // region 基本类型

    @Test
    void testConvertSimpleTypes() throws Exception {
        String xml = "<record><id>42</id><name>test</name></record>";
        Element element = parse(xml);

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "name"},
                new TypeInformation<?>[]{Types.INT, Types.STRING});

        Row row = XmlToRowConverter.convertXmlToRow(element, schema);

        assertEquals(42, row.getField(0));
        assertEquals("test", row.getField(1));
    }

    @Test
    void testConvertAttributeFallback() throws Exception {
        String xml = "<record id=\"5\" name=\"hello\"/>";
        Element element = parse(xml);

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "name"},
                new TypeInformation<?>[]{Types.INT, Types.STRING});

        Row row = XmlToRowConverter.convertXmlToRow(element, schema);

        assertEquals(5, row.getField(0));
        assertEquals("hello", row.getField(1));
    }

    @Test
    void testConvertMissingFieldYieldsNull() throws Exception {
        String xml = "<record><id>1</id></record>";
        Element element = parse(xml);

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "name"},
                new TypeInformation<?>[]{Types.INT, Types.STRING});

        Row row = XmlToRowConverter.convertXmlToRow(element, schema);

        assertEquals(1, row.getField(0));
        assertNull(row.getField(1));
    }

    // endregion

    // region 嵌套对象

    @Test
    void testConvertNestedObject() throws Exception {
        String xml = "<record><id>1</id><address><city>北京</city><zip>100001</zip></address></record>";
        Element element = parse(xml);

        RowTypeInfo addressType = (RowTypeInfo) Types.ROW_NAMED(
                new String[]{"city", "zip"},
                Types.STRING, Types.STRING);

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "address"},
                new TypeInformation<?>[]{Types.INT, addressType});

        Row row = XmlToRowConverter.convertXmlToRow(element, schema);

        assertEquals(1, row.getField(0));
        Row address = (Row) row.getField(1);
        assertEquals("北京", address.getField(0));
        assertEquals("100001", address.getField(1));
    }

    @Test
    void testConvertMissingNestedObjectYieldsNull() throws Exception {
        String xml = "<record><id>1</id></record>";
        Element element = parse(xml);

        RowTypeInfo addressType = (RowTypeInfo) Types.ROW_NAMED(
                new String[]{"city", "zip"},
                Types.STRING, Types.STRING);

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "address"},
                new TypeInformation<?>[]{Types.INT, addressType});

        Row row = XmlToRowConverter.convertXmlToRow(element, schema);

        assertEquals(1, row.getField(0));
        assertNull(row.getField(1));
    }

    // endregion

    // region 数组类型

    @Test
    void testConvertRowArray() throws Exception {
        String xml = "<record><id>1</id>"
                + "<tag><name>a</name></tag>"
                + "<tag><name>b</name></tag>"
                + "<tag><name>c</name></tag>"
                + "</record>";
        Element element = parse(xml);

        RowTypeInfo tagType = (RowTypeInfo) Types.ROW_NAMED(new String[]{"name"}, Types.STRING);
        TypeInformation<?> arrayType = ObjectArrayTypeInfo.getInfoFor(tagType);

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "tag"},
                new TypeInformation<?>[]{Types.INT, arrayType});

        Row row = XmlToRowConverter.convertXmlToRow(element, schema);

        assertEquals(1, row.getField(0));
        Row[] tags = (Row[]) row.getField(1);
        assertEquals(3, tags.length);
        assertEquals("a", tags[0].getField(0));
        assertEquals("b", tags[1].getField(0));
        assertEquals("c", tags[2].getField(0));
    }

    @Test
    void testConvertStringArray() throws Exception {
        String xml = "<record><id>1</id>"
                + "<tag>x</tag>"
                + "<tag>y</tag>"
                + "</record>";
        Element element = parse(xml);

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "tag"},
                new TypeInformation<?>[]{Types.INT, BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO});

        Row row = XmlToRowConverter.convertXmlToRow(element, schema);

        assertEquals(1, row.getField(0));
        String[] tags = (String[]) row.getField(1);
        assertArrayEquals(new String[]{"x", "y"}, tags);
    }

    @Test
    void testConvertEmptyArrayYieldsNull() throws Exception {
        String xml = "<record><id>1</id></record>";
        Element element = parse(xml);

        RowTypeInfo tagType = (RowTypeInfo) Types.ROW_NAMED(new String[]{"name"}, Types.STRING);
        TypeInformation<?> arrayType = ObjectArrayTypeInfo.getInfoFor(tagType);

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "tag"},
                new TypeInformation<?>[]{Types.INT, arrayType});

        Row row = XmlToRowConverter.convertXmlToRow(element, schema);

        assertEquals(1, row.getField(0));
        assertNull(row.getField(1));
    }

    // endregion

    // region 多条记录

    @Test
    void testConvertXmlToRows() throws Exception {
        String xml = "<records>"
                + "<item><id>1</id><name>a</name></item>"
                + "<item><id>2</id><name>b</name></item>"
                + "</records>";
        Document doc = new SAXReader().read(new StringReader(xml));
        @SuppressWarnings("unchecked")
        List<Element> elements = doc.getRootElement().elements("item");

        EtlSchema schema = new EtlSchema(
                new String[]{"id", "name"},
                new TypeInformation<?>[]{Types.INT, Types.STRING});

        List<Row> rows = XmlToRowConverter.convertXmlToRows(elements, schema);

        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getField(0));
        assertEquals("a", rows.get(0).getField(1));
        assertEquals(2, rows.get(1).getField(0));
        assertEquals("b", rows.get(1).getField(1));
    }

    // endregion
}