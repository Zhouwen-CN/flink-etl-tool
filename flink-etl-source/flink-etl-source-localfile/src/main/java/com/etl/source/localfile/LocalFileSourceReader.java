package com.etl.source.localfile;

import com.etl.core.source.BaseSourceReader;
import com.etl.core.source.BaseSplitReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

/**
 * 本地文件 Source Reader
 * 继承 BaseSourceReader，自动处理线程模型和状态管理
 */
@Slf4j
public class LocalFileSourceReader extends BaseSourceReader<Row, Row, LocalFileSplit, LocalFileSplitState> {

    public LocalFileSourceReader(
            Supplier<BaseSplitReader<Row, LocalFileSplit>> splitReaderSupplier,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, new LocalFileRecordEmitter(), new Configuration(), context);
    }

    @Override
    public LocalFileSplitState initializedState(LocalFileSplit split) {
        log.debug("初始化分片状态: {}", split.splitId());
        return new LocalFileSplitState(split);
    }

    @Override
    protected LocalFileSplit toSplitType(String splitId, LocalFileSplitState splitState) {
        return splitState.getSplit();
    }
}