package com.etl.source.mock;

import com.etl.core.schema.EtlSchema;
import com.etl.core.source.BaseSplitReader;
import com.etl.source.mock.config.MockSourceConfig;
import com.etl.source.mock.generator.DataRowGenerator;
import com.etl.source.mock.generator.RandomRowGenerator;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock Split 读取器
 * 核心逻辑：batch 模式读取固定数据，streaming 模式定时生成数据
 */
@Slf4j
public class MockSplitReader implements BaseSplitReader<Row, MockSplit> {

    private final MockSourceConfig mockConfig;
    private final EtlSchema schema;

    private final Queue<MockSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    // batch 模式状态
    private MockSplit currentSplit;
    private Iterator<Row> batchDataIterator;
    private int currentRowIndex = 0;

    // streaming 模式状态
    private ScheduledExecutorService scheduler;
    private volatile boolean running = true;
    private final AtomicLong rowCounter = new AtomicLong(0);
    private Queue<Row> streamingDataQueue;

    public MockSplitReader(MockSourceConfig mockConfig) {
        this.mockConfig = mockConfig;
        this.schema = mockConfig.getSchema();

        // 初始化数据生成器
        if (mockConfig.getRows() != null) {
            // batch 模式 - 固定数据
            List<Row> rows = DataRowGenerator.generateRows(mockConfig.getRows(), schema);
            batchDataIterator = rows.iterator();
            log.info("Batch 模式：从 rows 配置生成 {} 行数据", rows.size());
        } else if (mockConfig.getRunMode() == MockSourceConfig.RunMode.BATCH) {
            // batch 模式 - 随机生成
            List<Row> rows = RandomRowGenerator.generateRows(schema, mockConfig.getNumRows());
            batchDataIterator = rows.iterator();
            log.info("Batch 模式：随机生成 {} 行数据", rows.size());
        } else {
            // streaming 模式 - 准备定时生成
            streamingDataQueue = new ArrayDeque<>();
            scheduler = Executors.newSingleThreadScheduledExecutor();
            log.info("Streaming 模式：准备启动定时生成器，间隔 {} ms", mockConfig.getIntervalMs());
        }
    }

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        if (mockConfig.getRunMode() == MockSourceConfig.RunMode.BATCH) {
            return fetchBatchData();
        } else {
            return fetchStreamingData();
        }
    }

    /**
     * batch 模式：从 iterator 读取数据
     */
    private RecordsWithSplitIds<Row> fetchBatchData() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        // 如果没有当前分片，尝试开始新分片
        if (currentSplit == null) {
            MockSplit split = pendingSplits.poll();
            if (split == null) {
                // 没有待处理的分片，返回空结果
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }

            // 开始新分片
            currentSplit = split;
            currentRowIndex = 0;
            log.info("开始读取分片: {}", split.splitId());
        }

        // 读取数据（一次返回所有数据）
        while (batchDataIterator.hasNext()) {
            Row row = batchDataIterator.next();
            currentRowIndex++;
            builder.add(currentSplit.splitId(), row);
            log.debug("读取第 {} 行数据: {}", currentRowIndex, row);
        }

        // 数据读取完毕，标记分片结束
        finishedSplits.add(currentSplit.splitId());
        log.info("Batch 模式数据读取完毕，共 {} 行", currentRowIndex);

        // 清空当前分片状态
        currentSplit = null;

        return builder.build();
    }

    /**
     * streaming 模式：定时生成数据
     */
    private RecordsWithSplitIds<Row> fetchStreamingData() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        // 如果没有当前分片，尝试开始新分片
        if (currentSplit == null) {
            MockSplit split = pendingSplits.poll();
            if (split == null) {
                // 没有待处理的分片，返回空结果
                return builder.build();
            }

            // 开始新分片
            currentSplit = split;
            log.info("开始 streaming 分片: {}", split.splitId());

            // 启动定时任务
            startStreamingGenerator();
        }

        // 从队列中取出已生成的数据
        Row row;
        int count = 0;
        while ((row = streamingDataQueue.poll()) != null) {
            builder.add(currentSplit.splitId(), row);
            count++;
        }

        if (count > 0) {
            log.debug("Streaming 模式取出 {} 行数据", count);
        }

        return builder.build();
    }

    /**
     * 启动 streaming 模式的定时生成器
     */
    private void startStreamingGenerator() {
        scheduler.scheduleAtFixedRate(() -> {
            if (!running) {
                return;
            }

            try {
                // 生成随机数据
                Row row = RandomRowGenerator.generateRow(schema);
                rowCounter.incrementAndGet();

                // 添加到队列
                streamingDataQueue.offer(row);

                log.debug("生成第 {} 行数据: {}", rowCounter.get(), row);
            } catch (Exception e) {
                log.error("生成数据失败", e);
            }
        }, 0, mockConfig.getIntervalMs(), TimeUnit.MILLISECONDS);

        log.info("Streaming 模式：scheduler 已启动");
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MockSplit> splitsChanges) {
        pendingSplits.addAll(splitsChanges.splits());
        log.debug("接收到 {} 个新分片", splitsChanges.splits().size());
    }

    @Override
    public void close() throws Exception {
        running = false;

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("MockSplitReader 关闭，streaming 模式共生成 {} 行数据", rowCounter.get());
        } else {
            log.info("MockSplitReader 关闭，batch 模式共读取 {} 行数据", currentRowIndex);
        }
    }
}