package com.etl.connector.localfile.source;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import com.etl.connector.localfile.source.format.FileFormatPlugin;
import com.etl.core.source.AbstractSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * 本地文件分片读取器
 * 实现阻塞式数据读取，配合 BaseSourceReader 使用
 *
 * <p>设计说明：
 * <ul>
 *   <li>每个文件分片创建独立的输入流</li>
 *   <li>通过 FileFormatPlugin 解析文件内容</li>
 *   <li>直接返回 Flink Row 类型</li>
 *   <li>字段名和类型从 source.schema 配置中获取</li>
 *   <li>配置信息从 Split 中获取，不通过构造函数传递</li>
 * </ul>
 */
@Slf4j
public class LocalFileSplitReader extends AbstractSplitReader<Row, LocalFileSplit> {

    private final Set<String> finishedSplits = new HashSet<>();

    // 当前分片读取状态
    private LocalFileSplit currentSplit;
    private Iterator<Row> rowIterator;
    private FileFormatPlugin formatPlugin;

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        // 如果没有当前分片，尝试开始新分片
        if (currentSplit == null) {
            LocalFileSplit split = pendingSplits.poll();
            if (split == null) {
                // 没有待处理的分片，返回空结果
                RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }

            // 开始新分片
            startNewSplit(split);
        }

        // 读取数据
        return fetchRows();
    }

    /**
     * 开始读取新分片
     */
    private void startNewSplit(LocalFileSplit split) throws IOException {
        log.info("开始读取文件: {}", split.getFilePath());

        // 从 Split 获取配置
        currentSplit = split;
        LocalFileSourceConfig config = currentSplit.getConfig();
        formatPlugin = config.getFormatPlugin();

        // 使用配置中的格式插件
        InputStream inputStream = Files.newInputStream(Paths.get(split.getFilePath()));
        Iterable<Row> rows = formatPlugin.parse(config, inputStream);
        rowIterator = rows.iterator();
    }

    /**
     * 读取数据
     */
    private RecordsWithSplitIds<Row> fetchRows() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        int recordsInBatch = 0;
        Integer batchSize = currentSplit.getConfig().getBatchSize();
        String splitId = currentSplit.splitId();

        // 读取一批记录
        while (rowIterator.hasNext() && recordsInBatch < batchSize) {
            Row row = rowIterator.next();
            builder.add(splitId, row);
            recordsInBatch++;
        }

        // 如果没有更多记录，标记分片完成
        if (!rowIterator.hasNext()) {
            finishedSplits.add(splitId);
            log.info("文件 {} 读取完成", currentSplit.getFileName());
            this.close();
        }

        return builder.build();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(formatPlugin);
        rowIterator = null;
        currentSplit = null;
        formatPlugin = null;
        log.info("LocalFileSplitReader 关闭");
    }
}