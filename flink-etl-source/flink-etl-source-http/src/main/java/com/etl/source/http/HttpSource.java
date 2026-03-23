package com.etl.source.http;

import com.etl.core.config.SourceConfig;
import com.etl.core.schema.EtlSchema;
import com.etl.core.source.AbstractSplitSource;
import com.etl.core.source.BaseSplitReader;
import com.etl.core.source.serde.DefaultCheckpointSerializer;
import com.etl.core.source.serde.DefaultSplitSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.function.Supplier;

/**
 * HTTP Source 实现
 * 支持 GET/POST 请求获取 JSON 数据
 */
@Slf4j
public class HttpSource extends AbstractSplitSource<HttpSplit, HttpEnumCheckpoint> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final HttpSourceConfig httpSourceConfig;

    @SuppressWarnings("unchecked")
    public HttpSource(SourceConfig config) {
        super(config);

        // URL（必填）
        String url = config.getString("url");
        Preconditions.checkArgument(url != null && !url.isEmpty(), "url is null or empty");

        // HTTP 方法（可选，默认 GET）
        String method = config.getString("method", "GET");
        Preconditions.checkArgument("GET".equalsIgnoreCase(method) || "POST".equalsIgnoreCase(method),
                "method must be GET or POST");

        // 请求头（可选）
        Map<String, Object> headers = null;
        Object headersObj = config.get("headers");
        if (headersObj != null) {
            if (!(headersObj instanceof Map)) {
                throw new IllegalArgumentException("headers 必须是对象格式 {key: value}");
            }
            headers = (Map<String, Object>) headersObj;
        }

        // 查询参数（可选）
        Map<String, Object> params = null;
        Object paramsObj = config.get("params");
        if (paramsObj != null) {
            if (!(paramsObj instanceof Map)) {
                throw new IllegalArgumentException("params 必须是对象格式 {key: value}");
            }
            params = (Map<String, Object>) paramsObj;
        }

        // 请求体（可选）
        String body = null;
        Object bodyObj = config.get("body");
        if (bodyObj != null) {
            if (!(bodyObj instanceof Map)) {
                throw new IllegalArgumentException("body 必须是对象格式 {key: value}");
            }
            try {
                body = OBJECT_MAPPER.writeValueAsString(bodyObj);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("body 序列化失败: " + e.getMessage(), e);
            }
        }

        // JSONPath（可选）
        String dataPath = config.getString("dataPath");

        // Schema（必填）
        EtlSchema schema = config.getSchema();
        Preconditions.checkNotNull(schema, "schema is null");

        // 封装配置
        this.httpSourceConfig = HttpSourceConfig.builder()
                .url(url)
                .method(method.toUpperCase())
                .headers(headers)
                .params(params)
                .body(body)
                .dataPath(dataPath)
                .schema(schema)
                .build();

        log.info("创建 HttpSource: url={}, method={}, dataPath={}", url, method, dataPath);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<HttpSplit, HttpEnumCheckpoint> createEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext) {
        log.info("创建 SplitEnumerator");
        return new HttpSplitEnumerator(enumContext, httpSourceConfig);
    }

    @Override
    public SplitEnumerator<HttpSplit, HttpEnumCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext,
            HttpEnumCheckpoint checkpoint) {
        log.info("从检查点恢复 SplitEnumerator");
        return new HttpSplitEnumerator(enumContext, checkpoint, httpSourceConfig);
    }

    @Override
    public SourceReader<Row, HttpSplit> createReader(SourceReaderContext readerContext) {
        log.info("创建 SourceReader");
        var splitReaderSupplier = (Supplier<BaseSplitReader<Row, HttpSplit>>) HttpSplitReader::new;
        return new HttpSourceReader(splitReaderSupplier, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<HttpSplit> getSplitSerializer() {
        return new DefaultSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<HttpEnumCheckpoint> getEnumeratorCheckpointSerializer() {
        return new DefaultCheckpointSerializer<>();
    }
}