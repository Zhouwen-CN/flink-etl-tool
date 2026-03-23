package com.etl.source.http;

import com.etl.core.schema.EtlSchema;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

/**
 * HTTP Source 配置
 * 用于传递所有参数到 Enumerator 和 SplitReader
 */
@Getter
@Builder
public class HttpSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 请求 URL */
    private final String url;
    /** HTTP 方法，GET 或 POST */
    private final String method;
    /** 请求头 */
    private final Map<String, Object> headers;
    /** 查询参数 */
    private final Map<String, Object> params;
    /** 请求体（JSON 对象序列化后的字符串） */
    private final String body;
    /** JSONPath 表达式，提取数据 */
    private final String dataPath;
    /** Schema 定义 */
    private final EtlSchema schema;
}