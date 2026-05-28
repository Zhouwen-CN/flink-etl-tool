package com.etl.connector.http.source.format;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

/**
 * Raw 格式解析器
 * 整个响应体作为单字段 STRING Row 返回
 */
@Slf4j
@AutoService(HttpFormat.class)
public class RawFormat implements HttpFormat {

    @Override
    public String identifier() {
        return "raw";
    }

    @Override
    public List<Row> parse(String rawResponse, HttpSourceConfig config) {
        Row row = Row.withPositions(1);
        row.setField(0, rawResponse);
        log.info("RawFormat 解析完成，记录数: 1");
        return Collections.singletonList(row);
    }
}