package com.etl.connector.http.source.config;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import com.etl.core.utils.JsonUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * HTTP Source 配置
 * 用于传递所有参数到 Enumerator 和 SplitReader
 */
@Getter
@Builder
@Slf4j
public class HttpSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 支持的 format 类型 */
    private static final Set<String> SUPPORTED_FORMATS = new HashSet<>(Arrays.asList("json", "xml", "raw"));

    /**
     * 请求 URL
     */
    private final String url;
    /**
     * HTTP 方法，GET 或 POST
     */
    private final String method;
    /**
     * 请求头
     */
    private final Map<String, Object> headers;
    /**
     * 查询参数
     */
    private final Map<String, Object> params;
    /**
     * 请求体（JSON 对象序列化后的字符串）
     */
    private final String body;
    /**
     * 响应格式：json / xml / raw（默认 json）
     */
    private final String format;
    /**
     * JSONPath 表达式，提取 JSON 数据节点
     */
    private final String jsonPath;
    /**
     * XPath 表达式，提取 XML 数据节点集合
     */
    private final String xmlPath;
    /**
     * Schema 定义
     */
    private final EtlSchema schema;

    public static HttpSourceConfig fromSourceConfig(SourceConfig config) {
        // URL（必填）
        String url = config.getString("url");
        Preconditions.checkArgument(url != null && !url.isEmpty(), "url is null or empty");

        // HTTP 方法（可选，默认 GET）
        String method = config.getString("method", "GET");
        Preconditions.checkArgument("GET".equalsIgnoreCase(method) || "POST".equalsIgnoreCase(method),
                "method must be GET or POST");

        // 请求头（可选）
        Map<String, Object> headers = config.getMap("headers");

        // 查询参数（可选）
        Map<String, Object> params = config.getMap("params");

        // 请求体（可选）
        String body = null;
        Object object = config.getObject("body");
        if (object != null) {
            if (object instanceof String) {
                body = (String) object;
            } else if (object instanceof Map) {
                body = JsonUtils.writeValueAsString(object);
            } else {
                throw new IllegalArgumentException("Body of type " + object.getClass().getName() + " is not supported");
            }
        }

        // 格式（可选，默认 json）
        String format = config.getString("format", "json");
        Preconditions.checkArgument(SUPPORTED_FORMATS.contains(format),
                "format must be one of " + SUPPORTED_FORMATS + ", but got: " + format);

        // 路径配置（按 format 取对应字段）
        String jsonPath = config.getString("jsonPath");
        String xmlPath = config.getString("xmlPath");

        // Schema（必填）
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");

        // raw 格式：schema 必须只有 1 个 STRING 字段
        if ("raw".equals(format)) {
            Preconditions.checkArgument(schema.getFieldCount() == 1,
                    "raw format requires schema with exactly 1 field, but got " + schema.getFieldCount());
            Preconditions.checkArgument(Types.STRING.equals(schema.getFieldType(0)),
                    "raw format requires schema field type to be STRING, but got " + schema.getFieldType(0));
        }

        log.info("创建 HttpSource: url={}, method={}, format={}, jsonPath={}, xmlPath={}",
                url, method, format, jsonPath, xmlPath);

        // 封装配置
        return HttpSourceConfig.builder()
                .url(url)
                .method(method.toUpperCase())
                .headers(headers)
                .params(params)
                .body(body)
                .format(format)
                .jsonPath(jsonPath)
                .xmlPath(xmlPath)
                .schema(schema)
                .build();
    }
}