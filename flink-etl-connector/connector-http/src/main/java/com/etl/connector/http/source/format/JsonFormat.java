package com.etl.connector.http.source.format;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.utils.JsonUtils;
import com.google.auto.service.AutoService;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * JSON 格式解析器
 * 通过 JsonPath 提取数据节点，再转换为 Row
 */
@Slf4j
@AutoService(HttpFormat.class)
public class JsonFormat implements HttpFormat {

    @Override
    public String identifier() {
        return "json";
    }

    @Override
    public List<Row> parse(String rawResponse, HttpSourceConfig config) {
        JsonNode rootNode;
        try {
            rootNode = JsonUtils.getByJsonPath(rawResponse, config.getJsonPath());
        } catch (PathNotFoundException e) {
            throw new IllegalArgumentException("JsonPath 提取失败: " + config.getJsonPath(), e);
        }

        List<Row> rows = JsonToRowConverter.convertJsonToRows(rootNode, config.getSchema());
        log.info("JsonFormat 解析完成，记录数: {}", rows.size());
        return rows;
    }
}