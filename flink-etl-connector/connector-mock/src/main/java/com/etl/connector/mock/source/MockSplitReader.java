package com.etl.connector.mock.source;

import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.source.BaseSplitReader;
import com.etl.connector.mock.source.config.MockSourceConfig;
import com.etl.connector.mock.source.generator.RandomRowGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * Mock Split 读取器
 * <p>
 * 行为由配置决定：
 * <ul>
 *   <li>配置了 rows 或 numRows：bounded 模式，数据读取完毕后程序自然停止</li>
 *   <li>未配置 rows 和 numRows：unbounded 模式，按 intervalMs 持续生成数据</li>
 * </ul>
 */
@Slf4j
public class MockSplitReader implements BaseSplitReader<Row, MockSplit> {

    private final boolean bounded;

    private final MockSourceConfig mockConfig;
    private final EtlSchema schema;

    private final Queue<MockSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    // 有界模式状态
    private MockSplit currentSplit;
    private int currentRowIndex = 0;

    // 无界模式状态
    private long rowCounter = 0;

    public MockSplitReader(MockSourceConfig mockConfig) {
        this.mockConfig = mockConfig;
        this.schema = mockConfig.getSchema();
        this.bounded = mockConfig.isBounded();
    }

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        if (bounded) {
            return fetchBoundedData();
        } else {
            return fetchUnboundedData();
        }
    }

    /**
     * 有界模式：从 iterator 读取预生成的数据，数据读取完毕后标记分片结束
     */
    private RecordsWithSplitIds<Row> fetchBoundedData() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        // 如果没有当前分片，尝试开始新分片
        if (currentSplit == null) {
            MockSplit split = pendingSplits.poll();
            if (split == null) {
                // 没有待处理的分片，返回空结果
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }

            currentSplit = split;
            currentRowIndex = 0;
            log.info("开始读取分片: {}", split.splitId());
        }

        // 有界模式 - 生成所有数据
        Iterator<Row> batchDataIterator;
        if (mockConfig.getData() != null) {
            List<Row> rows = JsonToRowConverter.convertJsonToRows(mockConfig.getData(), schema);
            batchDataIterator = rows.iterator();
            log.info("有界模式：从 data 配置生成 {} 行数据", rows.size());
        } else {
            List<Row> rows = RandomRowGenerator.generateRows(schema, mockConfig.getNumRows());
            batchDataIterator = rows.iterator();
            log.info("有界模式：随机生成 {} 行数据", mockConfig.getNumRows());
        }

        // 读取所有数据
        while (batchDataIterator.hasNext()) {
            Row row = batchDataIterator.next();
            currentRowIndex++;
            builder.add(currentSplit.splitId(), row);
            log.debug("读取第 {} 行: {}", currentRowIndex, row);
        }

        // 数据读取完毕，标记分片结束
        finishedSplits.add(currentSplit.splitId());
        log.info("有界模式数据读取完毕，共 {} 行", currentRowIndex);

        currentSplit = null;
        return builder.build();
    }

    /**
     * 无界模式：每次 fetch 生成一行数据后 sleep
     */
    private RecordsWithSplitIds<Row> fetchUnboundedData() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        // 如果没有当前分片，尝试开始新分片
        if (currentSplit == null) {
            MockSplit split = pendingSplits.poll();
            if (split == null) {
                return builder.build();
            }

            currentSplit = split;
            log.info("开始无界模式分片: {}", split.splitId());
        }

        // 生成一行数据
        try {
            Row row = RandomRowGenerator.generateRow(schema);
            rowCounter++;
            builder.add(currentSplit.splitId(), row);
            log.debug("生成第 {} 行: {}", rowCounter, row);
        } catch (Exception e) {
            throw new IOException("生成数据失败", e);
        }

        // sleep 间隔
        try {
            Thread.sleep(mockConfig.getIntervalMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MockSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个新分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        if (!bounded) {
            log.info("MockSplitReader 关闭，无界模式共生成 {} 行数据", rowCounter);
        } else {
            log.info("MockSplitReader 关闭，有界模式共读取 {} 行数据", currentRowIndex);
        }
    }
}
