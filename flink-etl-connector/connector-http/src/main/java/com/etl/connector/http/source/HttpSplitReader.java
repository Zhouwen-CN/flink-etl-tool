package com.etl.connector.http.source;

import com.etl.connector.http.source.config.HttpSourceConfig;
import com.etl.connector.http.source.format.HttpFormat;
import com.etl.connector.http.source.format.HttpFormatLoader;
import com.etl.core.source.AbstractSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HTTP 分片读取器
 * 执行 HTTP 请求并通过 Format SPI 将响应解析为 Row
 */
@Slf4j
public class HttpSplitReader extends AbstractSplitReader<Row, HttpSplit> {
    private static final int CONNECT_TIMEOUT = 30000;
    private static final int READ_TIMEOUT = 60000;

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

        HttpSourceConfig config = split.getConfig();
        try {
            // 执行 HTTP 请求
            String rawResponse = executeRequest(config);

            // 通过 Format SPI 解析响应
            HttpFormat format = HttpFormatLoader.load(config.getFormat());
            List<Row> rows = format.parse(rawResponse, config);

            log.info("HTTP 请求完成，format={}，获取 {} 条记录", config.getFormat(), rows.size());

            // 添加记录
            for (Row row : rows) {
                builder.add(split.splitId(), row);
            }

            // 标记分片完成
            finishedSplits.add(split.splitId());
        } catch (IOException | IllegalArgumentException e) {
            log.error("HTTP 请求失败: {}", e.getMessage(), e);
            throw new RuntimeException("HTTP 请求失败: " + e.getMessage(), e);
        } catch (Exception e) {
            // 捕获其他未预期的异常（如 Format 解析中的异常）
            log.error("数据转换失败: {}", e.getMessage(), e);
            throw new RuntimeException("数据转换失败: " + e.getMessage(), e);
        }

        return builder.build();
    }

    /**
     * 执行 HTTP 请求
     */
    private String executeRequest(HttpSourceConfig config) throws IOException {
        String urlString = config.getUrl();

        // 添加查询参数
        if (config.getParams() != null && !config.getParams().isEmpty()) {
            StringBuilder urlBuilder = new StringBuilder(urlString);
            urlBuilder.append("?");
            for (Map.Entry<String, Object> entry : config.getParams().entrySet()) {
                urlBuilder.append(entry.getKey())
                        .append("=")
                        .append(URLEncoder.encode(String.valueOf(entry.getValue()), StandardCharsets.UTF_8.name()))
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

            // 设置默认请求头，自定义设置会覆盖默认设置
            if ("xml".equalsIgnoreCase(config.getFormat())) {
                connection.setRequestProperty("Accept", "text/xml");
                connection.setRequestProperty("Content-Type", "text/xml");
            }else{
                connection.setRequestProperty("Accept", "application/json");
                connection.setRequestProperty("Content-Type", "application/json");
            }
            if (config.getHeaders() != null) {
                for (Map.Entry<String, Object> entry : config.getHeaders().entrySet()) {
                    connection.setRequestProperty(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }

            // POST 请求体
            if ("POST".equalsIgnoreCase(config.getMethod()) && config.getBody() != null) {
                connection.setDoOutput(true);
                try (OutputStream os = connection.getOutputStream()) {
                    byte[] input = config.getBody().getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }
            }

            // 检查响应码
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("HTTP 请求失败，响应码: " + responseCode);
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
    public void close() throws Exception {
        log.info("HttpSplitReader 关闭");
    }
}