package com.etl.connector.http.source.format;

import com.etl.connector.http.source.config.HttpSourceConfig;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * HTTP 响应格式解析接口
 * 将 HTTP 响应字符串解析为 Row 列表
 */
public interface HttpFormat {

    /**
     * 格式标识
     *
     * @return 格式标识（json / xml / raw）
     */
    String identifier();

    /**
     * 将 HTTP 响应字符串解析为 Row 列表
     *
     * @param rawResponse HTTP 响应原文
     * @param config      HTTP Source 配置
     * @return Row 列表
     */
    List<Row> parse(String rawResponse, HttpSourceConfig config);
}