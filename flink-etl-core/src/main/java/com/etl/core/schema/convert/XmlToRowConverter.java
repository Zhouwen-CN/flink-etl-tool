package com.etl.core.schema.convert;

import com.etl.core.schema.EtlSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.dom4j.Element;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * XML Element 转 Row 转换器
 * 处理 dom4j Element 到 Flink Row 的转换，支持嵌套对象和数组
 * 与 JsonToRowConverter 对称设计
 */
public class XmlToRowConverter {

    private XmlToRowConverter() {
        // 私有构造函数，防止实例化
    }

    /**
     * 将 Element 列表转换为 Row 列表
     *
     * @param elements Element 列表（来自 XPath selectNodes）
     * @param schema   Schema 定义
     * @return Row 列表
     */
    public static List<Row> convertXmlToRows(List<Element> elements, EtlSchema schema) {
        List<Row> rows = new ArrayList<>(elements.size());
        for (Element element : elements) {
            rows.add(convertXmlToRow(element, schema));
        }
        return rows;
    }

    /**
     * 将 Element 转换为 Row（基于 EtlSchema）
     *
     * @param element Element 节点
     * @param schema  Schema 定义
     * @return Row 对象
     */
    public static Row convertXmlToRow(Element element, EtlSchema schema) {
        int fieldCount = schema.getFieldCount();
        Row row = Row.withPositions(fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = schema.getFieldName(i);
            TypeInformation<?> fieldType = schema.getFieldType(i);
            Object value = convertFromElement(element, fieldName, fieldType);
            row.setField(i, value);
        }

        return row;
    }

    /**
     * 将 Element 转换为 Row（基于 RowTypeInfo，用于嵌套对象）
     *
     * @param element    Element 节点
     * @param rowTypeInfo Row 类型信息
     * @return Row 对象
     */
    public static Row convertXmlToRow(Element element, TypeInformation<?> rowTypeInfo) {
        RowTypeInfo rowInfo = (RowTypeInfo) rowTypeInfo;
        String[] fieldNames = rowInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = rowInfo.getFieldTypes();

        Row row = Row.withPositions(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Object value = convertFromElement(element, fieldNames[i], fieldTypes[i]);
            row.setField(i, value);
        }

        return row;
    }

    /**
     * 从 Element 中提取并转换字段值
     *
     * @param element   父 Element
     * @param fieldName 字段名（子元素名或属性名）
     * @param fieldType 目标类型
     * @return 转换后的值
     */
    private static Object convertFromElement(Element element, String fieldName, TypeInformation<?> fieldType) {
        // 复杂类型：数组
        if (fieldType instanceof ObjectArrayTypeInfo || fieldType instanceof BasicArrayTypeInfo) {
            return convertXmlArray(element, fieldName, fieldType);
        }

        // 复杂类型：嵌套对象
        if (fieldType instanceof RowTypeInfo) {
            Element child = element.element(fieldName);
            if (child == null) {
                return null;
            }
            return convertXmlToRow(child, fieldType);
        }

        // 基本类型
        String text = extractFieldText(element, fieldName);
        return TypeConverter.convertFromValue(text, fieldName, fieldType);
    }

    /**
     * 将同名子元素列表转换为数组
     *
     * @param element   父 Element
     * @param fieldName 子元素名
     * @param arrayType 数组类型信息
     * @return 包装类型数组（Integer[], Long[], String[], Row[] 等）
     */
    private static Object convertXmlArray(Element element, String fieldName, TypeInformation<?> arrayType) {
        List<? extends Element> children = element.elements(fieldName);
        if (children.isEmpty()) {
            return null;
        }

        TypeInformation<?> componentType = getComponentType(arrayType);
        int size = children.size();

        // Row[] 类型数组
        if (componentType instanceof RowTypeInfo) {
            Row[] array = new Row[size];
            for (int i = 0; i < size; i++) {
                array[i] = convertXmlToRow(children.get(i), componentType);
            }
            return array;
        }

        // 基本类型数组
        Object array = Array.newInstance(componentType.getTypeClass(), size);
        for (int i = 0; i < size; i++) {
            String text = children.get(i).getText();
            Array.set(array, i, TypeConverter.convertFromValue(text, fieldName, componentType));
        }
        return array;
    }

    /**
     * 提取字段文本值
     * 优先取子元素文本，回退到同名属性值
     *
     * @param element   父 Element
     * @param fieldName 字段名
     * @return 文本值，未找到时返回 null
     */
    private static String extractFieldText(Element element, String fieldName) {
        Element child = element.element(fieldName);
        if (child != null) {
            return child.getText();
        }
        return element.attributeValue(fieldName);
    }

    /**
     * 获取数组元素类型
     */
    private static TypeInformation<?> getComponentType(TypeInformation<?> arrayType) {
        TypeInformation<?> componentInfo = null;
        if (arrayType instanceof BasicArrayTypeInfo) {
            componentInfo = ((BasicArrayTypeInfo<?, ?>) arrayType).getComponentInfo();
        } else if (arrayType instanceof ObjectArrayTypeInfo) {
            componentInfo = ((ObjectArrayTypeInfo<?, ?>) arrayType).getComponentInfo();
        }
        return componentInfo;
    }
}