package com.etl.connector.http.source.format;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.core.schema.XmlToRowConverter;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * XML 格式解析器
 * 通过 dom4j 解析 XML，xmlPath（XPath 语法）定位记录元素集合
 * 每个元素生成一行 Row，支持简单类型、嵌套对象和数组
 */
@Slf4j
@AutoService(HttpFormat.class)
public class XmlFormat implements HttpFormat {

    @Override
    public String identifier() {
        return "xml";
    }

    @Override
    public List<Row> parse(String rawResponse, HttpSourceConfig config) {
        Document document = readDocument(rawResponse);

        List<Node> nodes = selectNodes(document, config.getXmlPath());
        if (nodes.isEmpty()) {
            log.warn("XmlFormat 未匹配到任何节点，xmlPath: {}", config.getXmlPath());
            return Collections.emptyList();
        }

        List<Element> elements = toElements(nodes);
        List<Row> rows = XmlToRowConverter.convertXmlToRows(elements, config.getSchema());
        log.info("XmlFormat 解析完成，记录数: {}", rows.size());
        return rows;
    }

    private Document readDocument(String rawResponse) {
        try {
            return new SAXReader().read(new StringReader(rawResponse));
        } catch (DocumentException e) {
            throw new IllegalArgumentException("XML 解析失败: " + e.getMessage(), e);
        }
    }

    private List<Node> selectNodes(Document document, String xmlPath) {
        if (xmlPath == null || xmlPath.isEmpty()) {
            // 未指定路径时取根元素本身（作为单条记录）
            return Collections.singletonList(document.getRootElement());
        }
        return document.selectNodes(xmlPath);
    }

    private List<Element> toElements(List<Node> nodes) {
        List<Element> elements = new ArrayList<>(nodes.size());
        for (Node node : nodes) {
            if (!(node instanceof Element)) {
                throw new IllegalArgumentException(
                        "xmlPath 匹配到非元素节点（仅支持 Element），节点类型: " + node.getNodeTypeName());
            }
            elements.add((Element) node);
        }
        return elements;
    }
}