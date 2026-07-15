package com.etl.connector.mock.source;

import com.etl.connector.mock.source.config.MockSourceConfig;
import com.etl.connector.mock.source.generator.RandomRowGenerator;
import com.etl.core.schema.convert.JsonToRowConverter;
import com.etl.core.source.AbstractSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Mock Split 读取器
 * <p>
 * 行为由配置决定：
 * <ul>
 *   <li>配置了 rows 或 numRows：bounded 模式，数据读取完毕后程序自然停止</li>
 *   <li>未配置 rows 和 numRows：unbounded 模式，按 intervalMs 持续生成数据</li>
 * </ul>
 * <p>
 * 配置信息从 Split 中获取，不通过构造函数传递
 */
@Slf4j
public class MockSplitReader extends AbstractSplitReader<Row, MockSplit> {

    private final Set<String> finishedSplits = new HashSet<>();

    // 有界模式状态
    private MockSplit currentSplit;
    private MockSourceConfig currentConfig;
    private long rowCounter = 0;

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        // 如果没有当前分片，尝试开始新分片
        if (currentSplit == null) {
            MockSplit split = pendingSplits.poll();
            if (split == null) {
                // 没有待处理的分片，返回空结果
                RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();
                builder.addFinishedSplits(finishedSplits);
                return builder.build();
            }

            // 从 Split 获取配置
            currentSplit = split;
            currentConfig = split.getMockConfig();

            log.info("开始读取分片: {}", split.splitId());
        }

        if (currentConfig.isBounded()) {
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

        // 有界模式 - 生成所有数据
        Iterator<Row> batchDataIterator;
        if (currentConfig.getData() != null) {
            List<Row> rows = JsonToRowConverter.convertJsonToRows(currentConfig.getData(), currentConfig.getSchema());
            batchDataIterator = rows.iterator();
            log.info("有界模式：从 data 配置生成 {} 行数据", rows.size());
        } else {
            List<Row> rows = RandomRowGenerator.generateRows(currentConfig.getSchema(), currentConfig.getNumRows());
            batchDataIterator = rows.iterator();
            log.info("有界模式：随机生成 {} 行数据", currentConfig.getNumRows());
        }

        // 读取所有数据
        while (batchDataIterator.hasNext()) {
            Row row = batchDataIterator.next();
            rowCounter++;
            builder.add(currentSplit.splitId(), row);
            log.debug("读取第 {} 行: {}", rowCounter, row);
        }

        // 数据读取完毕，标记分片结束
        finishedSplits.add(currentSplit.splitId());
        log.info("有界模式数据读取完毕，共 {} 行", rowCounter);

        // 置空会尝试重新获取分片，获取不到会 addFinishedSplits，不然会无限读取
        currentSplit = null;
        currentConfig = null;
        return builder.build();
    }

    /**
     * 无界模式：每次 fetch 生成一行数据后 sleep
     */
    private RecordsWithSplitIds<Row> fetchUnboundedData() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        // 生成一行数据
        try {
            Row row = RandomRowGenerator.generateRow(currentConfig.getSchema());
            rowCounter++;
            builder.add(currentSplit.splitId(), row);
            log.debug("生成第 {} 行: {}", rowCounter, row);
        } catch (Exception e) {
            throw new IOException("生成数据失败", e);
        }

        // sleep 间隔
        try {
            Thread.sleep(currentConfig.getIntervalMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return builder.build();
    }

    @Override
    public void close() throws Exception {
        log.info("MockSplitReader 关闭，共生成 {} 行数据", rowCounter);
    }
}
