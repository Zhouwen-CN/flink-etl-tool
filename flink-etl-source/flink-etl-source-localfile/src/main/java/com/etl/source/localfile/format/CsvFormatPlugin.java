package com.etl.source.localfile.format;

import com.etl.core.schema.EtlField;
import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.TypeConverter;
import com.etl.source.localfile.config.LocalFileSourceConfig;
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
import java.util.Iterator;

/**
 * CSV 格式解析插件
 * 字段名和类型从 source.schema 配置中获取
 */
@Slf4j
@AutoService(FileFormatPlugin.class)
public class CsvFormatPlugin implements FileFormatPlugin {

    @Override
    public String getType() {
        return "csv";
    }

    @Override
    public Iterable<Row> parse(LocalFileSourceConfig localFileSourceConfig, InputStream inputStream) {
        EtlSchema schema = localFileSourceConfig.getSchema();
        Charset charset = Charset.forName(localFileSourceConfig.getEncoding());
        String delimiter = localFileSourceConfig.getDelimiter();
        boolean skipHeader = localFileSourceConfig.isSkipHeader();

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .build();

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charset));
            CSVParser parser = csvFormat.parse(reader);

            return new CsvRowIterable(parser, schema, reader, inputStream, skipHeader);

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
        private final EtlSchema schema;
        private final BufferedReader reader;
        private final InputStream inputStream;
        private final boolean skipHeader;
        private volatile boolean closed = false;

        CsvRowIterable(CSVParser parser, EtlSchema schema, BufferedReader reader,
                       InputStream inputStream, boolean skipHeader) {
            this.parser = parser;
            this.schema = schema;
            this.reader = reader;
            this.inputStream = inputStream;
            this.skipHeader = skipHeader;
        }

        @Override
        public Iterator<Row> iterator() {
            return new Iterator<>() {
                private final Iterator<CSVRecord> csvIterator = parser.iterator();
                private boolean headerSkipped = false;

                @Override
                public boolean hasNext() {
                    if (closed) {
                        return false;
                    }
                    // 跳过头部行（如果配置了 skipHeader=true）
                    if (skipHeader && !headerSkipped && csvIterator.hasNext()) {
                        csvIterator.next(); // 跳过头部
                        headerSkipped = true;
                    }
                    boolean hasNext = csvIterator.hasNext();
                    if (!hasNext) {
                        closeQuietly();
                    }
                    return hasNext;
                }

                @Override
                public Row next() {
                    CSVRecord record = csvIterator.next();
                    int schemaSize = schema.getFields().size();
                    int recordSize = record.size();
                    Row row = new Row(schemaSize);

                    for (int i = 0; i < schemaSize; i++) {
                        Object value;
                        if (i < recordSize) {
                            value = record.get(i);
                        } else {
                            log.warn("CSV 行缺少字段 '{}', 已设为 null", schema.getField(i).getName());
                            value = null;
                        }

                        EtlField field = schema.getField(i);
                        Object converted;
                        try {
                            converted = TypeConverter.convert(value, field.getName(), field.getType());
                        } catch (Exception e) {
                            throw new RuntimeException("CSV 字段类型转换失败: 字段名=" + field.getName()
                                    + ", 字段类型=" + field.getType() + ", 原始值=" + value, e);
                        }
                        row.setField(i, converted);
                    }

                    // 检查是否有多余列
                    if (recordSize > schemaSize) {
                        log.warn("CSV 行有 {} 个多余列被忽略", recordSize - schemaSize);
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
