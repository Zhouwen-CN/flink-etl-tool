package com.etl.connector.localfile.source;

import com.etl.connector.localfile.source.config.LocalFileSourceConfig;
import com.etl.connector.localfile.source.format.FileFormatPlugin;
import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;

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
public class LocalFileSplitReader implements BaseSplitReader<Row, LocalFileSplit> {

    private final Queue<LocalFileSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    // 当前分片读取状态
    private LocalFileSplit currentSplit;
    private InputStream currentInputStream;
    private Iterator<Row> currentRowIterator;

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
        FileFormatPlugin formatPlugin = config.getFormatPlugin();

        try {
            currentInputStream = Files.newInputStream(Paths.get(split.getFilePath()));

            // 使用配置中的格式插件
            Iterable<Row> rows = formatPlugin.parse(config, currentInputStream);
            currentRowIterator = rows.iterator();

        } catch (IOException e) {
            closeCurrentSplit();
            throw new IOException("打开文件失败: " + split.getFilePath(), e);
        }
    }

    /**
     * 读取数据
     */
    private RecordsWithSplitIds<Row> fetchRows() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        try {
            int recordsInBatch = 0;

            // 读取一批记录
            while (currentRowIterator.hasNext() && recordsInBatch < currentSplit.getConfig().getBatchSize()) {
                Row row = currentRowIterator.next();
                builder.add(currentSplit.splitId(), row);
                recordsInBatch++;
            }

            // 如果没有更多记录，标记分片完成
            if (!currentRowIterator.hasNext()) {
                finishedSplits.add(currentSplit.splitId());
                log.info("文件 {} 读取完成", currentSplit.getFileName());

                // 关闭资源
                closeCurrentSplit();
            }

        } catch (Exception e) {
            closeCurrentSplit();
            throw e;
        }

        return builder.build();
    }

    /**
     * 关闭当前分片的资源
     */
    private void closeCurrentSplit() {
        if (currentInputStream != null) {
            try {
                currentInputStream.close();
            } catch (Exception e) {
                log.warn("关闭输入流失败", e);
            }
        }

        currentInputStream = null;
        currentRowIterator = null;
        currentSplit = null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<LocalFileSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个新文件分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        closeCurrentSplit();
        log.info("LocalFileSplitReader 关闭");
    }
}