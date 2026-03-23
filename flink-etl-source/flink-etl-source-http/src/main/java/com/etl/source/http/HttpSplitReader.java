package com.etl.source.http;

import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * HTTP 分片读取器
 * 执行 HTTP 请求并将响应转换为 Row
 */
@Slf4j
public class HttpSplitReader implements BaseSplitReader<Row, HttpSplit> {
    private static final int CONNECT_TIMEOUT = 30000;
    private static final int READ_TIMEOUT = 60000;

    private final Queue<HttpSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        HttpSplit split = pendingSplits.poll();
        if (split == null) {
            // 没有待处理的分片，返回空结果
            builder.addFinishedSplits(finishedSplits);
            return builder.build();
        }

        try {
            // 执行 HTTP 请求
            String jsonResponse = executeRequest(split.getConfig());

            // 转换为 Row
            List<Row> rows = JsonToRowConverter.convert(
                    jsonResponse,
                    split.getConfig().getDataPath(),
                    split.getConfig().getSchema()
            );

            log.info("HTTP 请求完成，获取 {} 条记录", rows.size());

            // 添加记录
            for (Row row : rows) {
                builder.add(split.splitId(), row);
            }

            // 标记分片完成
            finishedSplits.add(split.splitId());

        } catch (Exception e) {
            log.error("HTTP 请求失败: {}", e.getMessage(), e);
            throw new RuntimeException("HTTP 请求失败: " + e.getMessage(), e);
        }

        builder.addFinishedSplits(finishedSplits);
        return builder.build();
    }

    /**
     * 执行 HTTP 请求
     */
    private String executeRequest(HttpSourceConfig config) throws Exception {
        String urlString = config.getUrl();

        // 添加查询参数
        if (config.getParams() != null && !config.getParams().isEmpty()) {
            StringBuilder urlBuilder = new StringBuilder(urlString);
            urlBuilder.append("?");
            for (Map.Entry<String, Object> entry : config.getParams().entrySet()) {
                urlBuilder.append(entry.getKey())
                        .append("=")
                        .append(java.net.URLEncoder.encode(String.valueOf(entry.getValue()), StandardCharsets.UTF_8))
                        .append("&");
            }
            urlString = urlBuilder.substring(0, urlBuilder.length() - 1);
        }

        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        try {
            connection.setRequestMethod(config.getMethod());
            connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);

            // 设置请求头
            connection.setRequestProperty("Accept", "application/json");
            if (config.getHeaders() != null) {
                for (Map.Entry<String, Object> entry : config.getHeaders().entrySet()) {
                    connection.setRequestProperty(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }

            // POST 请求体
            if ("POST".equalsIgnoreCase(config.getMethod()) && config.getBody() != null) {
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                try (OutputStream os = connection.getOutputStream()) {
                    byte[] input = config.getBody().getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }
            }

            // 检查响应码
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("HTTP 请求失败，响应码: " + responseCode);
            }

            // 读取响应
            StringBuilder response = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
            }

            return response.toString();

        } finally {
            connection.disconnect();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<HttpSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个 HTTP 分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        log.info("HttpSplitReader 关闭");
    }
}