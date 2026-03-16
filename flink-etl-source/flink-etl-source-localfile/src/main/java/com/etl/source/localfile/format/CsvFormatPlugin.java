package com.etl.source.localfile.format;

import com.etl.core.config.SourceConfig;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * CSV 格式解析插件
 * 支持有头和无头两种模式
 */
@Slf4j
@AutoService(FileFormatPlugin.class)
public class CsvFormatPlugin implements FileFormatPlugin {

    @Override
    public String getType() {
        return "csv";
    }

    @Override
    public List<String> resolveFields(SourceConfig config, InputStream firstFile) {
        boolean hasHeader = config.getBoolean("header", true);

        if (hasHeader) {
            // 从文件头解析字段名
            String encoding = config.getString("encoding");
            Charset charset = encoding != null ? Charset.forName(encoding) : StandardCharsets.UTF_8;

            String delimiter = config.getString("delimiter");
            char delim = delimiter != null ? delimiter.charAt(0) : ',';

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(firstFile, charset))) {
                // 使用 Apache Commons CSV 解析首行获取字段名
                CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                        .setDelimiter(delim)
                        .build();

                CSVParser parser = csvFormat.parse(reader);
                Iterator<CSVRecord> iterator = parser.iterator();

                if (!iterator.hasNext()) {
                    throw new RuntimeException("CSV 文件为空，无法解析字段名");
                }

                // 从第一条记录解析字段名
                CSVRecord headerRecord = iterator.next();
                List<String> fields = new ArrayList<>(headerRecord.size());
                for (int i = 0; i < headerRecord.size(); i++) {
                    fields.add(headerRecord.get(i).trim());
                }

                log.info("从 CSV 文件头解析到 {} 个字段: {}", fields.size(), fields);
                return fields;

            } catch (IOException e) {
                throw new RuntimeException("解析 CSV 文件头失败: " + e.getMessage(), e);
            }
        } else {
            // 从配置获取字段名
            List<String> columns = config.getList("columns");
            if (columns == null || columns.isEmpty()) {
                throw new RuntimeException("header=false 时必须指定 columns 配置");
            }
            log.info("从配置获取到 {} 个字段: {}", columns.size(), columns);
            return columns;
        }
    }

    @Override
    public Iterable<Row> parse(SourceConfig config, InputStream inputStream, List<String> fields) {
        String encoding = config.getString("encoding");
        Charset charset = encoding != null ? Charset.forName(encoding) : StandardCharsets.UTF_8;

        String delimiter = config.getString("delimiter");
        char delim = delimiter != null ? delimiter.charAt(0) : ',';

        boolean hasHeader = config.getBoolean("header", true);

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setDelimiter(delim)
                .build();

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charset));
            CSVParser parser = csvFormat.parse(reader);

            return new CsvRowIterable(parser, fields, reader, inputStream, hasHeader);

        } catch (IOException e) {
            throw new RuntimeException("解析 CSV 文件失败: " + e.getMessage(), e);
        }
    }

    /**
     * CSV Row 迭代器封装
     * 确保在迭代完成后关闭输入流
     */
    private static class CsvRowIterable implements Iterable<Row> {

        private final CSVParser parser;
        private final List<String> fields;
        private final BufferedReader reader;
        private final InputStream inputStream;
        private final boolean skipHeader;
        private volatile boolean closed = false;

        CsvRowIterable(CSVParser parser, List<String> fields, BufferedReader reader, InputStream inputStream, boolean skipHeader) {
            this.parser = parser;
            this.fields = fields;
            this.reader = reader;
            this.inputStream = inputStream;
            this.skipHeader = skipHeader;
        }

        @Override
        public Iterator<Row> iterator() {
            return new Iterator<Row>() {
                private final Iterator<CSVRecord> csvIterator = parser.iterator();
                private boolean headerSkipped = false;

                @Override
                public boolean hasNext() {
                    if (closed) {
                        return false;
                    }
                    // 跳过头部行
                    if (skipHeader && !headerSkipped && csvIterator.hasNext()) {
                        csvIterator.next(); // 跳过头部
                        headerSkipped = true;
                    }
                    boolean hasNext = csvIterator.hasNext();
                    if (!hasNext) {
                        // 迭代完成，关闭资源
                        closeQuietly();
                    }
                    return hasNext;
                }

                @Override
                public Row next() {
                    CSVRecord record = csvIterator.next();
                    Row row = new Row(fields.size());

                    for (int i = 0; i < fields.size(); i++) {
                        String value = i < record.size() ? record.get(i) : null;
                        row.setField(i, value);
                    }

                    return row;
                }

                private void closeQuietly() {
                    if (closed) {
                        return;
                    }
                    closed = true;
                    try {
                        parser.close();
                    } catch (Exception e) {
                        log.warn("关闭 CSV 解析器失败", e);
                    }
                    try {
                        reader.close();
                    } catch (Exception e) {
                        log.warn("关闭 BufferedReader 失败", e);
                    }
                    try {
                        inputStream.close();
                    } catch (Exception e) {
                        log.warn("关闭输入流失败", e);
                    }
                }
            };
        }
    }
}