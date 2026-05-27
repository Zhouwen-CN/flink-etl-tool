package com.etl.connector.http.source.config;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import com.etl.core.utils.JsonUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;

/**
 * HTTP Source 配置
 * 用于传递所有参数到 Enumerator 和 SplitReader
 */
@Getter
@Builder
@Slf4j
public class HttpSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

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
     * JSONPath 表达式，提取数据
     */
    private final String dataPath;
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

        // JSONPath（可选）
        String dataPath = config.getString("dataPath");

        // Schema（必填）
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");

        log.info("创建 HttpSource: url={}, method={}, dataPath={}", url, method, dataPath);

        // 封装配置
        return HttpSourceConfig.builder()
                .url(url)
                .method(method.toUpperCase())
                .headers(headers)
                .params(params)
                .body(body)
                .dataPath(dataPath)
                .schema(schema)
                .build();
    }
}